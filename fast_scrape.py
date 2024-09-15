import json
import os
import pickle
import requests
from urllib.parse import urljoin, urlparse
from redis import Redis
from rq import Queue
import logging
import re
import csv
import xml.etree.ElementTree as ET
import feedparser
import urllib.robotparser
from queue import PriorityQueue
import networkx as nx
import nltk
from sklearn.feature_extraction.text import TfidfVectorizer
from nltk.corpus import stopwords
from nltk import pos_tag, ne_chunk, word_tokenize
from nltk.tree import Tree
from collections import Counter
from bs4 import BeautifulSoup, Comment
import spacy
from nltk.stem import WordNetLemmatizer

# Download required NLTK resources
nltk.download("punkt")
nltk.download("averaged_perceptron_tagger")
nltk.download("maxent_ne_chunker")
nltk.download("words")
nltk.download("stopwords")
from ovarit import ovarit_domain_scrape
from reddit import reddit_domain_scrape

# Initialize Redis connection for persistence and RQ for job queuing
redis_conn = Redis()

# Setup logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)
CHECKPOINT_FILE = "crawler_checkpoint.pkl"

class URLItem:
    def __init__(self, priority, url, url_type):
        self.priority = priority
        self.url = url
        self.url_type = url_type

    # Define comparison methods for the priority queue
    def __lt__(self, other):
        return self.priority < other.priority


class KeyPhraseFocusCrawler:
    def __init__(
        self,
        start_urls,
        keywords,
        anti_keywords,
        allowed_subdirs,
        output_csv="crawler_results.csv",
        feeds=[],
    ):
        self.start_urls = start_urls
        self.keywords = keywords
        self.anti_keywords = anti_keywords
        self.allowed_subdirs = allowed_subdirs
        self.output_csv = output_csv
        self.feeds = feeds
        self.urls_to_visit = PriorityQueue()
        self.session = requests.Session()
        self.seen_urls = set()
        self.graph = nx.DiGraph()
        self.host = {}
        self._initialize_output_csv()
        self._initialize_urls()
        self.stop_words = set(stopwords.words("english"))
        self.nlp = spacy.load("en_core_web_sm")
        self.keyphrase_correlates = {}
        self.keyphrase_non_correlates = {}
        self.good = 0
        self.bad = 0

    def save_checkpoint(self):
        """Save the current state of the crawler to a file."""
        state = {
            "urls_to_visit": list(self.urls_to_visit.queue),
            "seen_urls": self.seen_urls,
            "graph": self.graph,
            "keyphrase_correlates": self.keyphrase_correlates,
            "keyphrase_non_correlates": self.keyphrase_non_correlates,
            "good": self.good,
            "bad": self.bad,
        }
        with open(CHECKPOINT_FILE, "wb") as f:
            pickle.dump(state, f)
        logger.info("Checkpoint saved.")

    def load_checkpoint(self):
        """Load the saved state of the crawler from a file."""
        if os.path.exists(CHECKPOINT_FILE):
            with open(CHECKPOINT_FILE, "rb") as f:
                state = pickle.load(f)
                self.urls_to_visit = PriorityQueue()
                for item in state["urls_to_visit"]:
                    self.urls_to_visit.put(item)
                self.seen_urls = state["seen_urls"]
                self.graph = state["graph"]
                self.keyphrase_correlates = state["keyphrase_correlates"]
                self.keyphrase_non_correlates = state["keyphrase_non_correlates"]
                self.good = state["good"]
                self.bad = state["bad"]
            logger.info("Checkpoint loaded.")
    
    def workout_new_keyphrases(self):
        output = {}

        # Ensure self.keywords and self.anti_keywords are initialized
        keywords = getattr(self, "keywords", [])
        anti_keywords = getattr(self, "anti_keywords", [])

        try:
            # Open CSV in append mode ('a') to avoid overwriting
            with open("new_keyphrases.csv", mode="a", newline="") as f:
                csv_w = csv.writer(f)
                # Check if the file is empty and write headers if so
                if f.tell() == 0:
                    csv_w.writerow(
                        [
                            "Keyphrase",
                            "Score",
                            "Keyphrase Correlates",
                            "Found Keywords",
                            "Found Keywords Anti",
                        ]
                    )

                for keyphrase in self.keyphrase_correlates:
                    found_keywords = [kw for kw in keywords if kw in keyphrase]
                    found_keywords_anti = [akw for akw in anti_keywords if akw in keyphrase]

                    # Calculate score with proper checks to avoid KeyError
                    if keyphrase in self.keyphrase_non_correlates:
                        score = (self.keyphrase_correlates[keyphrase] / self.good) - (
                            self.keyphrase_non_correlates[keyphrase] / self.bad
                        )
                    else:
                        score = self.keyphrase_correlates[keyphrase] / self.good

                    # Store output for returning later
                    output[keyphrase] = score

                    # Write to CSV
                    csv_w.writerow(
                        [
                            keyphrase,
                            score,
                            True,  # Assuming this column is just a static True value as in your original code
                            json.dumps(found_keywords),  # Convert list to JSON string
                            json.dumps(found_keywords_anti),  # Convert list to JSON string
                        ]
                    )

        except Exception as e:
            logger.error(f"An error occurred while processing keyphrases: {e}")

        return output

    def _extract_named_entities(self, text):
        """
        Extract Named Entities using SpaCy's Named Entity Recognition (NER).

        Args:
            text (str): The input text.

        Returns:
            list: A list of named entities.
        """
        # Use SpaCy's NER model
        doc = self.nlp(text)
        # Extract entities of interest
        entities = [ent.text.lower() for ent in doc.ents]

        # Filter entities: remove stop words, overly short entities, and non-informative words
        filtered_entities = [
            entity
            for entity in entities
            if entity not in self.stop_words
            and len(entity.split()) > 1
            and len(entity) > 3
        ]

        return list(set(filtered_entities))  # Remove duplicates

    def _extract_noun_phrases(self, text):
        """
        Extract Noun Phrases using SpaCy's dependency parsing.

        Args:
            text (str): The input text.

        Returns:
            list: A list of noun phrases.
        """
        doc = self.nlp(text)
        noun_phrases = []

        # Extract noun chunks based on SpaCy's dependency parsing
        for chunk in doc.noun_chunks:
            # Convert noun chunk to lowercase and remove stop words
            chunk_text = chunk.text.lower()

            # Filter out chunks containing only stop words or that are too short
            if (
                any(word not in self.stop_words for word in chunk_text.split())
                and len(chunk_text.split()) > 1
            ):
                noun_phrases.append(chunk_text)

        # Filter out redundant or overly generic phrases
        filtered_noun_phrases = [
            phrase
            for phrase in noun_phrases
            if len(phrase.split()) > 1 and len(phrase) > 3
        ]

        return list(set(filtered_noun_phrases))  # Remove duplicates

    def _extract_tfidf_phrases(self, text, min_phrase_len, max_phrase_len):
        """
        Extract key phrases using TF-IDF to find phrases unique to this document.

        Args:
            text (str): The input text.
            min_phrase_len (int): Minimum length of the key phrases.
            max_phrase_len (int): Maximum length of the key phrases.

        Returns:
            list: A list of TF-IDF based key phrases.
        """
        vectorizer = TfidfVectorizer(
            ngram_range=(min_phrase_len, max_phrase_len), stop_words="english"
        )
        tfidf_matrix = vectorizer.fit_transform([text])
        tfidf_scores = zip(
            vectorizer.get_feature_names_out(), tfidf_matrix.toarray()[0]
        )
        tfidf_phrases = [
            phrase
            for phrase, score in sorted(tfidf_scores, key=lambda x: x[1], reverse=True)
            if score > 0
        ]

        return tfidf_phrases

    def combine_with(self, keyphrases, is_good):
        if is_good:
            self.good += 1
            for keyphrase in keyphrases:
                if keyphrase in self.keyphrase_correlates:
                    self.keyphrase_correlates[keyphrase] += 1
                else:
                    self.keyphrase_correlates[keyphrase] = 1
        else:
            self.bad += 1
            for keyphrase in keyphrases:
                if keyphrase in self.keyphrase_non_correlates:
                    self.keyphrase_non_correlates[keyphrase] += 1
                else:
                    self.keyphrase_non_correlates[keyphrase] = 1

    def extract_new_key_phrases(self, text: str, min_phrase_len=2, max_phrase_len=4):
        """
        Extracts new potential key phrases from the text using a combination of NER, noun phrases, and TF-IDF,
        with enhanced handling of duplicates and phrase normalization.

        Args:
            text (str): The input text to extract key phrases from.
            min_phrase_len (int): Minimum length of the key phrases.
            max_phrase_len (int): Maximum length of the key phrases.

        Returns:
            dict: A dictionary of the most common extracted key phrases and their counts.
        """
        # Initialize stop words and lemmatizer
        stop_words = set(stopwords.words("english"))
        lemmatizer = WordNetLemmatizer()

        # Optimized function to normalize phrases
        def normalize_phrase(phrase):
            # Tokenize, filter stop words, and lemmatize in one step
            tokens = nltk.word_tokenize(phrase.lower())
            normalized_tokens = [
                lemmatizer.lemmatize(word)
                for word in tokens
                if word.isalpha() and word not in stop_words
            ]
            return " ".join(normalized_tokens)

        # Extract and normalize Named Entities
        named_entities = [
            normalize_phrase(entity) for entity in self._extract_named_entities(text)
        ]

        # Extract and normalize Noun Phrases
        noun_phrases = [
            normalize_phrase(phrase) for phrase in self._extract_noun_phrases(text)
        ]

        # Extract and normalize TF-IDF Phrases
        tfidf_phrases = [
            normalize_phrase(phrase)
            for phrase in self._extract_tfidf_phrases(
                text, min_phrase_len, max_phrase_len
            )
        ]

        # Combine all key phrases into a single list
        combined_phrases = named_entities + noun_phrases + tfidf_phrases

        # Dictionary to store phrases and their occurrence counts
        phrase_counts = {}

        # Iterate over each phrase to check if it should be included
        for phrase in combined_phrases:
            # Skip phrases containing any keywords or anti-keywords
            if any(keyword in phrase for keyword in self.keywords + self.anti_keywords):
                continue

            # Count the occurrence of the phrase in the text
            phrase_occurrence_count = text.lower().count(phrase)
            if phrase_occurrence_count > 1:
                phrase_counts[phrase] = phrase_occurrence_count

        return phrase_counts

    def _initialize_output_csv(self):
        """Initialize the output CSV file."""
        self.csv_file = open(self.output_csv, mode="a", newline="", buffering=1)
        self.csv_writer = csv.writer(self.csv_file)
        self.csv_writer.writerow(["URL", "Score", "Keywords Found","type_page","description","headline","datePublished","dateModified","author","new_keyphrases"])

    def _initialize_urls(self):
        """Initialize start URLs, scraping from specific domains if needed."""
        for url in self.start_urls:
            self._add_to_queue(url, "start", priority=0)
            domain = urlparse(url).netloc
            self.host[domain] = self.pass_robot(url)
            logger.info(f"Added {domain} to the host list.")
            for url_, text in ovarit_domain_scrape(domain):
                score, found_keywords = self.relative_score(text)
                self._add_to_queue(url_, "start", priority=score)
            for url_, text in reddit_domain_scrape(domain):
                score, found_keywords = self.relative_score(text)
                self._add_to_queue(url_, "start", priority=score)
        for feed in self.feeds:
            self._add_to_queue(feed, "feed", priority=0)
        logger.info("Crawler initialized with start URLs, keywords, and feeds.")

    def _normalize_url(self, url):
        """Normalize the URL by removing fragments and trailing slashes."""
        return url.split("#")[0].rstrip("/")

    def _is_allowed_domain(self, url):
        """Check if the URL belongs to an allowed domain."""
        return any(domain in url for domain in self.allowed_subdirs)

    def _add_to_queue(self, url, url_type, from_page=None, priority=0):
        """Add a URL to the queue if it hasn't been visited, with a given priority."""
        normalized_url = self._normalize_url(url)
        if normalized_url in self.seen_urls:
            logger.debug(f"URL already seen: {normalized_url}")
            return
        self.urls_to_visit.put(URLItem(priority, normalized_url, url_type))
        self.graph.add_node(url, score=priority)
        if from_page:
            self.graph.add_edge(
                from_page, normalized_url, score=priority, url_type=url_type
            )
        if priority > 0:
            logger.info(
                f"Added URL to queue: {url} with priority {priority} {url_type}"
            )

    def pass_robot(self, url):
        """Check if the URL passes the robots.txt."""
        parsed_url = urlparse(url)
        robots_url = f"{parsed_url.scheme}://{parsed_url.netloc}/robots.txt"
        try:
            response = self.session.get(robots_url)
            if response.status_code == 200:
                rob = urllib.robotparser.RobotFileParser()
                rob.parse(response.text.splitlines())
                for sitemap in rob.site_maps() or []:
                    self._add_to_queue(sitemap, "sitemap", priority=0, from_page=url)
                logger.info(f"Robots.txt fetched and parsed for {url}")
                return rob
        except Exception as e:
            logger.error(f"Error fetching robots.txt for {url}: {e}")
        return None

    def relative_score(self, text):
        """Calculate the relevance score of a page or anchor text based on keywords and key phrases."""
        score = sum(
            len(re.findall(re.escape(keyword), text, re.IGNORECASE))
            for keyword in self.keywords
        )
        anti_score = sum(
            len(re.findall(re.escape(anti_keyword), text, re.IGNORECASE))
            for anti_keyword in self.anti_keywords
        )
        found_keywords = [
            keyword
            for keyword in self.keywords
            if re.search(re.escape(keyword), text, re.IGNORECASE)
        ]
        logger.debug(
            f"Calculated score: {score}, anti-score: {anti_score}, found keywords: {found_keywords}"
        )
        return score - anti_score, found_keywords

    def _process_url(self, url):
        """Process the URL by fetching and analyzing its content."""
        try:
            response = self.session.get(url)
            if response.status_code == 200:
                bs = BeautifulSoup(response.text, "html.parser")
                for link in bs.find_all("a", href=True):
                    href = link.get("href")
                    score, found_keywords = self.relative_score(link.text)
                    full_url = urljoin(url, href)
                    if self._is_allowed_domain(self._normalize_url(full_url)):
                        self._add_to_queue(
                            full_url, "webpage", priority=score, from_page=url
                        )
                score, found_keywords = self.relative_score(bs.text)
                self.update_graph_node(url, score, found_keywords)
                logger.info(f"Processed URL: {url} with score: {score}")
                return url, score, found_keywords
        except Exception as e:
            logger.error(f"Error processing URL {url}: {e}")
        return None, 0, []

    def _process_feed(self, url):
        """Process RSS feed URLs."""
        try:
            response = self.session.get(url)
            if (
                response.status_code == 200
                and "application/rss+xml" in response.headers["Content-Type"]
            ):
                feed = feedparser.parse(response.text)
                for entry in feed.entries:
                    priority, _ = self.relative_score(entry.title)
                    self._add_to_queue(
                        entry.link, "webpage", priority=priority, from_page=url
                    )
                logger.info(f"Processed feed: {url}")
        except Exception as e:
            logger.error(f"Error processing feed {url}: {e}")

    def _process_sitemap(self, url):
        """Process sitemap XML URLs."""
        try:
            response = self.session.get(url)
            if response.status_code == 200:
                bs = BeautifulSoup(response.text, "xml")
                for loc in bs.find_all("loc"):
                    self._add_to_queue(loc.text, "webpage", priority=0, from_page=url)
                for sitemap in bs.find_all("sitemap"):
                    self._add_to_queue(
                        sitemap.text, "sitemap", priority=0, from_page=url
                    )
                logger.info(f"Processed sitemap: {url}")
        except Exception as e:
            logger.error(f"Error processing sitemap {url}: {e}")

    def extract_metadata(self, bs):
        """
        Extract metadata from a BeautifulSoup object using various standards such as OpenGraph,
        standard meta tags, Dublin Core, Microformats, and Twitter Cards.

        Args:
            bs (BeautifulSoup): A BeautifulSoup object representing the HTML content.

        Returns:
            dict: A dictionary containing extracted metadata fields.
        """
        # Initialize variables for extracted metadata
        metadata = {
            "type_page": None,
            "description": None,
            "headline": None,
            "datePublished": None,
            "dateModified": None,
            "author": [],
        }
        try:

            # Extract JSON-LD, RDFa, Microdata
            for script in bs.find_all("script"):
                if script.get("type") == "application/ld+json":
                    json_data = json.loads(script.string)
                    # Extract type, description, headline, etc. from JSON-LD
                    metadata["type_page"] = json_data.get("@type", metadata["type_page"])
                    metadata["description"] = json_data.get(
                        "description", metadata["description"]
                    )
                    metadata["headline"] = json_data.get("headline", metadata["headline"])
                    metadata["datePublished"] = json_data.get(
                        "datePublished", metadata["datePublished"]
                    )
                    metadata["dateModified"] = json_data.get(
                        "dateModified", metadata["dateModified"]
                    )
                    if "author" in json_data:
                        author_data = json_data["author"]
                        if isinstance(author_data, list):
                             # Loop through list of authors
                             for author in author_data:
                                 if isinstance(author, dict):
                                     metadata["author"].append({
                                         "name": author.get("name"),
                                         "sameAs": author.get("sameAs")
                                     })
                                 elif isinstance(author, str):
                                     # Handle case where author is a simple string
                                     metadata["author"].append({"name": author})
                        
                        elif isinstance(author_data, dict):
                             # Single author case, and it's a dictionary
                             metadata["author"].append({
                                 "name": author_data.get("name"),
                                 "sameAs": author_data.get("sameAs")
                             })
                        
                        elif isinstance(author_data, str):
                             # Single author case, and it's a string
                             metadata["author"].append({"name": author_data})
                            
            if metadata["type_page"] is not None:
                return metadata

            # Extract OpenGraph metadata
            og_type = bs.find("meta", property="og:type")
            if og_type:
                metadata["type_page"] = og_type["content"]
            og_description = bs.find("meta", property="og:description")
            if og_description:
                metadata["description"] = og_description["content"]
            og_title = bs.find("meta", property="og:title")
            if og_title:
                metadata["headline"] = og_title["content"]
            og_date = bs.find("meta", property="article:published_time")
            if og_date:
                metadata["datePublished"] = og_date["content"]
            if metadata["type_page"] is not None:
                return metadata

            # Extract Twitter Card metadata
            twitter_card = bs.find("meta", attrs={"name": "twitter:card"})
            if twitter_card:
                metadata["type_page"] = twitter_card["content"]
            twitter_title = bs.find("meta", attrs={"name": "twitter:title"})
            if twitter_title:
                metadata["headline"] = twitter_title["content"]
            twitter_description = bs.find("meta", attrs={"name": "twitter:description"})
            if twitter_description:
                metadata["description"] = twitter_description["content"]
            twitter_creator = bs.find("meta", attrs={"name": "twitter:creator"})
            if twitter_creator:
                metadata["author"].append({"name": twitter_creator["content"]})
            if metadata["type_page"] is not None:
                return metadata

            # Extract standard meta tags
            meta_description = bs.find("meta", attrs={"name": "description"})
            if meta_description:
                metadata["description"] = meta_description["content"]
            meta_author = bs.find("meta", attrs={"name": "author"})
            if meta_author:
                metadata["author"].append({"name": meta_author["content"]})
            meta_date = bs.find("meta", attrs={"name": "date"})
            if meta_date:
                metadata["datePublished"] = meta_date["content"]
            if metadata["type_page"] is not None:
                return metadata

            # Extract Dublin Core metadata
            dc_title = bs.find("meta", attrs={"name": "DC.title"})
            if dc_title:
                metadata["headline"] = dc_title["content"]
            dc_creator = bs.find("meta", attrs={"name": "DC.creator"})
            if dc_creator:
                metadata["author"].append({"name": dc_creator["content"]})
            dc_date = bs.find("meta", attrs={"name": "DC.date"})
            if dc_date:
                metadata["datePublished"] = dc_date["content"]
            if metadata["type_page"] is not None:
                return metadata

            # Extract Microformats (hentry, hcard)
            hentry = bs.find(class_="hentry")
            if hentry:
                if not metadata["headline"]:
                    entry_title = hentry.find(class_="entry-title")
                    if entry_title:
                        metadata["headline"] = entry_title.get_text()
                if not metadata["datePublished"]:
                    entry_published = hentry.find(class_="published")
                    if entry_published:
                        metadata["datePublished"] = entry_published["title"]
                if not metadata["author"]:
                    entry_author = hentry.find(class_="author")
                    if entry_author:
                        metadata["author"].append({"name": entry_author.get_text()})
            if metadata["type_page"] is not None:
                return metadata
        except Exception as e:
            logger.error(f"Error extracting metadata: {e}")
        return metadata
    
    def _process_webpage(self, url):
        """Process standard webpage URLs."""
        score = 0
        new_keyphrases = {}
        try:
            response = self.session.get(url)
            if response.status_code == 200:
                bs = BeautifulSoup(response.text, "html.parser")
                metadata = self.extract_metadata(bs)

                # Remove unwanted elements: scripts, styles, and comments
                for script_or_style in bs(["script", "style"]):
                    script_or_style.decompose()

                # Extract the cleaned text
                text = bs.get_text(separator=" ")

                # Normalize and clean up the extracted text
                lines = (line.strip() for line in text.splitlines())
                chunks = (
                    phrase.strip() for line in lines for phrase in line.split("  ")
                )
                text_content = "\n".join(chunk for chunk in chunks if chunk)

                # Extract links and add them to the queue
                for anchor in bs.find_all("a", href=True):
                    full_url = urljoin(url, anchor.get("href"))
                    if self._is_allowed_domain(self._normalize_url(full_url)):
                        priority, _ = self.relative_score(anchor.text)
                        self._add_to_queue(
                            full_url, "webpage", priority=priority, from_page=url
                        )

                # Calculate the relevance score for the page text
                score, found_keywords = self.relative_score(text_content)

                # Extract new key phrases from the page text
                if metadata["type_page"] is not None:
                    if "article" in metadata["type_page"].lower():
                        new_keyphrases = self.extract_new_key_phrases(text_content)
                        if score > 0:
                            self.combine_with(new_keyphrases, True)
                        else:
                            self.combine_with(new_keyphrases, False)
    
                # Log and update graph node with the result
                if score > 0:
                    self.csv_writer.writerow(
                        [
                            url,
                            score,
                            found_keywords,
                            metadata["type_page"],
                            metadata["description"],
                            metadata["headline"],
                            metadata["datePublished"],
                            metadata["dateModified"],
                            metadata["author"],
                            json.dumps(new_keyphrases),
                        ]
                    )
                    self.update_graph_node(url, score, found_keywords)
                    logger.info(f"Processed webpage: {url} with score: {score}")

                return (
                    url,
                    score,
                    found_keywords,
                    metadata["type_page"],
                    metadata["description"],
                    metadata["headline"],
                    metadata["datePublished"],
                    metadata["dateModified"],
                    metadata["author"],
                    new_keyphrases,  # Add new_keyphrases to the return statement
                )
        except Exception as e:
            logger.error(f"Error processing webpage {url}: {e}")
        return url, score, [], None, None, None, None, None, None, new_keyphrases  # Add new_keyphrases to the return statement


    
    
    
    def crawl(self):
        """Start the crawling process."""
        logger.info("Starting the crawling process.")
        self.load_checkpoint()
        count = 0
        while not self.urls_to_visit.empty():
            count = count + 1
            if count % 100 == 0:
                self.workout_new_keyphrases()
                self.save_checkpoint()
            url_item = self.urls_to_visit.get()
            if url_item.url in self.seen_urls:
                logger.debug(f"Skipping already seen URL: {url_item.url}")
                continue
            self.seen_urls.add(url_item.url)
            rob = self.host.get(urlparse(url_item.url).netloc)
            if rob and not rob.can_fetch("*", url_item.url):
                logger.info(f"URL blocked by robots.txt: {url_item.url}")
                continue

            if url_item.url_type == "feed":
                self._process_feed(url_item.url)
            elif url_item.url_type == "sitemap":
                self._process_sitemap(url_item.url)
            else:
                (
                    url,
                    score,
                    found_keywords,
                    type_page,
                    description,
                    headline,
                    datePublished,
                    dateModified,
                    author,
                    new_keyphrases
                ) = self._process_webpage(url_item.url)
                if score > 0:
                    yield url, score, found_keywords, type_page, description, headline, datePublished, dateModified, author, new_keyphrases

        logger.info("Crawling process completed.")

    def update_graph_node(self, url, score, found_keywords):
        """Update the graph node with new score and keywords found."""
        if url in self.graph:
            self.graph.nodes[url]["score"] = score
            self.graph.nodes[url]["keywords"] = found_keywords
            logger.debug(
                f"Updated graph node: {url} with score: {score} and keywords: {found_keywords}"
            )
        else:
            logger.warning(f"Attempted to update non-existent graph node: {url}")

    def crawl_graft_recraw(self, max_depth=3):
        """Re-crawl the graph using a priority queue based on depth and score."""
        visited = set()
        priority_queue = PriorityQueue()

        for start_page in self.start_urls:
            priority_queue.put((0, start_page))
        with open("ouput_recraw_graft.csv", mode="r") as f:
            with csv.writer(f) as csv_w:
                csv_w.writerow(["URL", "Score", "Keywords Found"])
                while not priority_queue.empty():
                    depth, current_page = priority_queue.get()
                    if current_page in visited or depth > max_depth:
                        continue

                    visited.add(current_page)
                    logger.info(f"Crawling: {current_page} at depth {depth}")

                    (
                        url,
                        score,
                        found_keywords,
                        type_page,
                        description,
                        headline,
                        datePublished,
                        dateModified,
                        author,
                        new_keyphrases
                    ) = self._process_webpage(current_page)
                    if score > 0:
                        yield url, score, found_keywords, type_page, description, headline, datePublished, dateModified, author, new_keyphrases

                    for neighbor in self.graph.neighbors(current_page):
                        if neighbor not in visited:
                            edge_data = self.graph.get_edge_data(current_page, neighbor)
                            combined_score = self.graph.nodes[neighbor][
                                "score"
                            ] + edge_data.get("weight", 1)
                            priority_queue.put((depth + 1, neighbor))
                            logger.info(
                                f"Queued neighbor: {neighbor} with combined score: {combined_score}"
                            )

                            if self.graph.nodes[neighbor]["score"] > 0:
                                yield neighbor, self.graph.nodes[neighbor][
                                    "score"
                                ], self.graph.nodes[neighbor].get("keywords", [])
                                csv_w.writerow(
                                    [
                                        neighbor,
                                        self.graph.nodes[neighbor]["score"],
                                        self.graph.nodes[neighbor].get("keywords", []),
                                        url,
                                        score,
                                        found_keywords,
                                        type_page,
                                        description,
                                        headline,
                                        datePublished,
                                        dateModified,
                                        author,
                                        json.dumps(new_keyphrases)
                                    ]
                                )


import utils.keywords as kw


def main():
    # Define the input configuration
    main_url = [
        "https://www.bbc.co.uk",
        "https://feeds.bbci.co.uk/",
        "https://www.bbc.co.uk/newsround/",
    ]

    allowed_subdirs = [
        "https://www.bbc.co.uk/news/",
        "https://www.bbc.co.uk/sport/",
        "https://www.bbc.co.uk/newsround/",
    ]

    # Define keywords and anti-keywords
    keywords = kw.Little_List  # Replace with your actual keyword list
    anti_keywords = []  # Define any anti-keywords if needed

    feeds = [
        "http://feeds.bbci.co.uk/news/rss.xml",
        "http://feeds.bbci.co.uk/news/world/rss.xml",
        "http://feeds.bbci.co.uk/news/business/rss.xml",
        "http://feeds.bbci.co.uk/news/politics/rss.xml",
        "http://feeds.bbci.co.uk/news/education/rss.xml",
        "http://feeds.bbci.co.uk/news/science_and_environment/rss.xml",
        "http://feeds.bbci.co.uk/news/technology/rss.xml",
        "http://feeds.bbci.co.uk/news/entertainment_and_arts/rss.xml",
    ]

    # Initialize the crawler with the corrected parameters
    crawler = KeyPhraseFocusCrawler(
        start_urls=main_url,
        keywords=keywords,
        anti_keywords=anti_keywords,
        allowed_subdirs=allowed_subdirs,
        feeds=feeds,
    )
    for result in crawler.crawl():
        pass
    for result in crawler.crawl_graft_recraw():
        pass


# Run the main function
if __name__ == "__main__":
    main()
