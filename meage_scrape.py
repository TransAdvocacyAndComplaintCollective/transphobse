import asyncio
from heapq import heappop, heappush
import math
import statistics
from threading import Lock
import aiohttp
import json
import os
import pickle
import logging
import re
import csv
import urllib.robotparser
from queue import PriorityQueue
import networkx as nx
import nltk
from nltk.corpus import stopwords
from nltk.stem import WordNetLemmatizer
from bs4 import BeautifulSoup
from readabilipy import simple_json_from_html_string, simple_tree_from_html_string
import requests
import spacy
from sklearn.feature_extraction.text import TfidfVectorizer
from urllib.parse import urljoin, urlparse
import feedparser
import numpy as np
import networkx as nx
from sklearn.metrics.pairwise import cosine_similarity
from tqdm import tqdm
import yake
from keybert import KeyBERT
import concurrent.futures
from reddit import reddit_domain_scrape

# Download required NLTK resources
nltk.download("punkt")
nltk.download("averaged_perceptron_tagger")
nltk.download("maxent_ne_chunker")
nltk.download("words")
nltk.download("stopwords")

# Initialize logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)
CHECKPOINT_FILE = "crawler_checkpoint.pkl"

class UpdatablePriorityQueue:
    """A custom priority queue that supports updating the priority of existing items, peeking, and more."""

    def __init__(self, min_heap=True):
        self.heap = []  # List of (priority, count, item) tuples
        self.entry_finder = {}  # Mapping of items to their heap entry
        self.REMOVED = '<removed-item>'  # Placeholder for a removed item
        self.counter = 0  # Unique sequence count to handle same-priority items
        self.min_heap = min_heap  # If False, use max-heap

    def add_or_update(self, item, priority):
        """Add a new item or update the priority of an existing item."""
        if item in self.entry_finder:
            self.remove(item)
        # For max-heap, we store negative priorities
        actual_priority = priority if self.min_heap else -priority
        entry = [actual_priority, self.counter, item]
        self.counter += 1
        self.entry_finder[item] = entry
        heappush(self.heap, entry)

    def remove(self, item):
        """Remove an existing item from the queue."""
        entry = self.entry_finder.pop(item)
        entry[-1] = self.REMOVED

    def pop(self):
        """Remove and return the lowest-priority item. Raise KeyError if empty."""
        while self.heap:
            priority, count, item = heappop(self.heap)
            if item is not self.REMOVED:
                del self.entry_finder[item]
                return item  # Return only the item, which is a URLItem object
        raise KeyError('pop from an empty priority queue')

    def peek(self):
        """Return the lowest-priority item without removing it. Raise KeyError if empty."""
        while self.heap:
            priority, count, item = self.heap[0]
            if item is not self.REMOVED:
                return item, (priority if self.min_heap else -priority)
            else:
                heappop(self.heap)  # Remove the removed item
        raise KeyError('peek from an empty priority queue')

    def change_priority(self, item, new_priority):
        """Change the priority of an existing item. Raises KeyError if not found."""
        if item not in self.entry_finder:
            raise KeyError(f'Item {item} not found in priority queue')
        self.add_or_update(item, new_priority)

    def get_priority(self, item):
        """Return the current priority of the given item."""
        if item in self.entry_finder:
            entry = self.entry_finder[item]
            return entry[0] if self.min_heap else -entry[0]
        raise KeyError(f'Item {item} not found in priority queue')

    def clear(self):
        """Remove all items from the priority queue."""
        self.heap.clear()
        self.entry_finder.clear()

    def all_items(self):
        """Return a list of all items in the queue along with their priorities."""
        return [
            (item, (priority if self.min_heap else -priority))
            for priority, count, item in self.heap
            if item is not self.REMOVED
        ]

    def empty(self):
        """Return True if the queue is empty."""
        return not self.entry_finder

    def __contains__(self, item):
        """Check if an item is in the queue."""
        return item in self.entry_finder

    def __len__(self):
        """Return the number of items in the queue."""
        return len(self.entry_finder)


class URLItem:
    def __init__(self, priority, url, url_type):
        self.priority = priority
        self.url = url
        self.url_type = url_type

    def __eq__(self, other):
        return self.url == other.url

    def __hash__(self):
        return hash(self.url)

    def __repr__(self):
        return f'URLItem(priority={self.priority}, url="{self.url}", url_type="{self.url_type}")'

class KeyPhraseFocusCrawler:
    def __init__(
        self,
        start_urls,
        keywords,
        anti_keywords,
        allowed_subdirs,
        output_csv="meage_crawler_results.csv",
        feeds=[],
    ):
        self.start_urls = start_urls
        self.keywords = keywords
        self.anti_keywords = anti_keywords
        self.allowed_subdirs = allowed_subdirs
        self.output_csv = output_csv
        self.feeds = feeds
        self.urls_to_visit = UpdatablePriorityQueue()
        self.seen_urls = set()
        self.graph = nx.DiGraph()
        self.host = {}
        self.stop_words = set(stopwords.words("english"))
        self.nlp = spacy.load("en_core_web_sm")
        self.pythonJsLock = Lock()
        self.keyphrase_correlates = {}
        self.keyphrase_non_correlates = {}
        self.good = 0
        self.bad = 0
        self.rob = {}
        self._initialize_output_csv()
        self._initialize_urls()
        # Initialize NLP models and other tools
        self.nlp = spacy.load("en_core_web_sm")
        self.kw_model = KeyBERT()

    def _initialize_output_csv(self):
        """Initialize the output CSV file."""
        self.csv_file = open(self.output_csv, mode="a", newline="", buffering=1)
        self.csv_writer = csv.writer(self.csv_file)
        if self.csv_file.tell() == 0:
            self.csv_writer.writerow(
                [
                    "URL",
                    "Score",
                    "Keywords Found",
                    "type_page",
                    "description",
                    "headline",
                    "datePublished",
                    "dateModified",
                    "author",
                    "new_keyphrases",
                ]
            )

    def _initialize_urls(self):
        """Initialize start URLs and add them to the queue."""

        def process_url(url):
            """Process a single URL and add it to the queue."""
            self._add_to_queue(url, "start", priority=0)
            domain = urlparse(url).netloc
            self.host[domain] = self.pass_robot(url)

            # Scrape Ovarit domain URLs
            site_urls = ovarit_domain_scrape(domain)
            for site_url in site_urls:
                score, found_keywords = self.relative_score(site_url[1])
                if url in site_url[0]:
                    self._add_to_queue(site_url[0], "start", priority=score)

            # Scrape Reddit domain URLs
            site_urls = reddit_domain_scrape(domain)
            for site_url in site_urls:
                score, found_keywords = self.relative_score(site_url[1])
                if url in site_url[0]:
                    print(site_url)
                    self._add_to_queue(site_url[0], "start", priority=score)

        # Start processing URLs concurrently using ThreadPoolExecutor
        with concurrent.futures.ThreadPoolExecutor() as executor:
            # Submit tasks for start_urls processing
            futures = [executor.submit(process_url, url) for url in self.start_urls]

            # Wait for all futures to complete
            concurrent.futures.wait(futures)

        # Process feeds separately in a single-threaded manner
        for feed in self.feeds:
            self._add_to_queue(feed, "feed", priority=0)

    def pass_robot(self, url):
        """Check if the URL passes the robots.txt."""
        parsed_url = urlparse(url)
        robots_url = f"{parsed_url.scheme}://{parsed_url.netloc}/robots.txt"
        try:
            response = requests.get(robots_url)
            if response.status_code == 200:
                rob = urllib.robotparser.RobotFileParser()
                rob.parse(response.text.splitlines())
                self.rob[parsed_url.hostname] = rob
                return rob
        except Exception as e:
            logger.error(f"Error fetching robots.txt for {url}: {e}")
        return None

    def _add_to_queue(self, url, url_type, from_page=None, priority=0):
        """Add a URL to the queue if it hasn't been visited, with a given priority."""
        normalized_url = self._normalize_url(url)
        if normalized_url in self.seen_urls:
            return
        self.urls_to_visit.add_or_update(URLItem(priority, normalized_url, url_type),priority)
        self.graph.add_node(url, score=priority)
        if from_page:
            self.graph.add_edge(
                from_page, normalized_url, score=priority, url_type=url_type
            )
        if priority > 0:
            logger.info(
                f"Added URL to queue: {url} with priority {priority} {url_type}"
            )

    def workout_new_keyphrases(self):
        """
        Discover and write new key phrases to a CSV file by comparing correlated and non-correlated contexts.
        This method uses TF-IDF and cosine similarity to filter out redundant or less relevant phrases.
        """
        keywords_set = set(self.keywords)
        anti_keywords_set = set(self.anti_keywords)

        # Prepare data for TF-IDF
        correlates_texts = [" ".join([kp for kp in self.keyphrase_correlates.keys()])]
        non_correlates_texts = [" ".join([kp for kp in self.keyphrase_non_correlates.keys()])]
        
        # Combine texts for TF-IDF vectorization
        combined_texts = correlates_texts + non_correlates_texts
        
        # Calculate TF-IDF vectors for keyphrases
        vectorizer = TfidfVectorizer().fit(combined_texts)
        tfidf_matrix = vectorizer.transform(combined_texts)
        
        # Compute cosine similarity between correlated and non-correlated keyphrases
        similarity_matrix = cosine_similarity(tfidf_matrix[0], tfidf_matrix[1])
        
        # Open the CSV file for appending
        try:
            with open("new_keyphrases.csv", mode="w", newline="") as f:
                csv_writer = csv.writer(f)
                
                # Write header if file is empty
                if f.tell() == 0:
                    csv_writer.writerow([
                        "Keyphrase", "Good Count", "Bad Count", "Score Difference",
                        "Tag", "Type", "Keywords Matched", "Anti-Keywords Matched", "Count Score"
                    ])
                
                # Process each correlated keyphrase
                for keyphrase, data in self.keyphrase_correlates.items():
                    good_count = data["count_trems"] / data["count"]
                    score_difference = good_count
                    
                    # Initialize bad_count
                    bad_count = 0
                    
                    # Iterate through non-correlated keyphrases and compute weighted `bad_count`
                    for non_keyphrase, non_data in self.keyphrase_non_correlates.items():
                        # Calculate cosine similarity between the correlated and non-correlated keyphrase
                        similarity = cosine_similarity(vectorizer.transform([keyphrase]), vectorizer.transform([non_keyphrase]))[0][0]
                        
                        if similarity > 0.8:  # Example threshold for similarity
                            # Calculate weighted bad_count contribution
                            bad_count += (non_data["count_trems"] / non_data["count"]) * similarity * score_difference

                    # Calculate final score difference
                    score_difference = good_count - bad_count
                    count_score = score_difference

                    # Filter out low score differences or highly similar phrases
                    if score_difference > 0.2:  # Example threshold, can be adjusted
                        # Determine keyword and anti-keyword matches
                        keyword_match = [key for key in keywords_set if key in keyphrase]
                        anti_keyword_match = [key for key in anti_keywords_set if key in keyphrase]
                        
                        # Write new keyphrase data to the CSV
                        csv_writer.writerow([
                            keyphrase,
                            good_count,
                            bad_count,
                            score_difference,
                            data["tag"],
                            data["type"],
                            ', '.join(keyword_match),
                            ', '.join(anti_keyword_match),
                            count_score  # Added count_score to CSV
                        ])
        
        except Exception as e:
            logger.error(f"An error occurred while processing keyphrases: {e}")


    def combine_keywords(self, items, correlates):
        # Select the dictionary to update
        keywords = (
            self.keyphrase_correlates if correlates else self.keyphrase_non_correlates
        )

        # Update or add keywords with max score and accumulated counts
        for ke in items:
            text = items[ke]["text"]
            score = items[ke]["score"]
            if text in keywords:
                keywords[text]["score"].append(items[ke]["score"])
                keywords[text]["tag"] = items[ke]["tag"]
                keywords[text]["type"] = items[ke]["type"]
                keywords[text]["count"] = 1 + keywords[text]["count"]
                keywords[text]["count_trems"] = items[ke]["count"] + keywords[text]["count_trems"] 
            else:
                keywords[text] = {
                    "score": [score],
                    "tag": items[ke]["tag"],
                    "type": items[ke]["type"],
                    "count": 1,
                    "count_trems": items[ke]["count"] ,
                }
        # Update the appropriate dictionary in the class
        if correlates:
            self.keyphrase_correlates = keywords
        else:
            self.keyphrase_non_correlates = keywords

    def extract_entities_with_types(self, text):
        """Extract named entities and their types using spaCy."""
        # Process the input text with spaCy's NLP model
        doc = self.nlp(text)

        # Extract entities and their corresponding types
        entities_with_types = [(ent.text, ent.label_) for ent in doc.ents]

        return entities_with_types

    def _normalize_url(self, url):
        """Normalize the URL by removing fragments and trailing slashes."""
        return url.split("#")[0].rstrip("/")

    def _is_allowed_domain(self, url):
        """Check if the URL belongs to an allowed domain."""
        return any(domain in url for domain in self.allowed_subdirs)

    async def _fetch(self, session, url):
        """Fetch a URL using aiohttp session."""
        try:
            async with session.get(url) as response:
                if response.status == 200:
                    return await response.text()
        except Exception as e:
            logger.error(f"Error fetching URL {url}: {e}")
        return None

    async def _process_url(self, session, url):
        """Process the URL by fetching and analyzing its content in a separate thread."""
        text = await self._fetch(session, url)

        # Define the blocking work to be done in a thread
        def process_url_content(text, url):
            if text:
                bs = BeautifulSoup(text, "html.parser")
                metadata = self.extract_metadata(bs)

                # Remove unwanted elements: scripts, styles, and comments
                for script_or_style in bs(["script", "style"]):
                    script_or_style.decompose()

                # Extract the cleaned text
                with self.pythonJsLock:
                    article = simple_json_from_html_string(str(text), use_readability=True)
                if article["content"] is not None:
                    bs = BeautifulSoup(article["content"], "html.parser")
                    text_content = bs.get_text(separator=" ")
                    score, found_keywords = self.relative_score(text_content)
                else:
                    text_content = bs.get_text(separator=" ")
                    score, found_keywords = self.relative_score(text_content)

                # Extract extract_key_phrases from the page text
                new_keyphrases = self.extract_key_phrases(text_content)
                # Update graph and log information
                self.update_graph_node(url, score, found_keywords)

                # Extract and process links
                for link in bs.find_all("a", href=True):
                    href = link.get("href")
                    full_url = urljoin(url, href)
                    if self._is_allowed_domain(self._normalize_url(full_url)):
                        link_score, _ = self.relative_score(link.text)
                        self._add_to_queue(
                            full_url, "webpage", priority=link_score, from_page=url
                        )

                # Write results to CSV
                if score > 0:
                    self.combine_keywords(new_keyphrases, True)
                    self.good += 1
                    if isinstance(new_keyphrases, np.ndarray):  # Check if it's a NumPy array
                        new_keyphrases = new_keyphrases.tolist()  # Convert to list
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
                    elif isinstance(new_keyphrases, list) or isinstance(new_keyphrases, dict):
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
                else:
                    self.combine_keywords(new_keyphrases, True)
                    self.bad += 1

                return url, score, found_keywords, metadata, new_keyphrases
            return None, 0, [], {}, {}

        # If text is available, process it in a thread
        if text:
            await asyncio.to_thread(process_url_content, text, url)

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
        return score - anti_score, found_keywords

    def extract_metadata(self, bs):
        """Extract metadata from a BeautifulSoup object using OpenGraph, Twitter Cards, and other tags."""
        metadata = {
            "type_page": None,
            "description": None,
            "headline": None,
            "datePublished": None,
            "dateModified": None,
            "author": [],
        }
        try:
            # OpenGraph metadata
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

            # Twitter Card metadata
            twitter_title = bs.find("meta", attrs={"name": "twitter:title"})
            if twitter_title:
                metadata["headline"] = twitter_title["content"]
            twitter_description = bs.find("meta", attrs={"name": "twitter:description"})
            if twitter_description:
                metadata["description"] = twitter_description["content"]
            twitter_creator = bs.find("meta", attrs={"name": "twitter:creator"})
            if twitter_creator:
                metadata["author"].append({"name": twitter_creator["content"]})

        except Exception as e:
            logger.error(f"Error extracting metadata: {e}")
        return metadata

    def clean_text(self, text):
        """Clean input text by removing extra newlines, spaces, URLs, and unwanted characters."""
        # Remove URLs
        # Remove non-alphanumeric characters except spaces
        text = re.sub(r"[^a-zA-Z0-9\s]", "", text)
        # Remove multiple newlines and replace them with a single newline
        text = re.sub(r"\n+", "\n", text)
        # Replace multiple spaces with a single space
        text = re.sub(r"\s+", " ", text)
        # Trim leading and trailing spaces
        text = text.strip()
        return text

    def is_valid_phrase(self, phrase):
        """
        Check if a given phrase is valid for keyword extraction.
        This method filters out phrases that are purely numeric, too short,
        or consist of only stopwords or symbols.

        Args:
            phrase (str): The phrase to be validated.

        Returns:
            bool: True if the phrase is valid, False otherwise.
        """
        # Check if the phrase is empty or None
        if not phrase or not phrase.strip():
            return False

        # Check if the phrase contains only digits or symbols
        if phrase.isdigit() or not any(char.isalpha() for char in phrase):
            return False

        # Tokenize the phrase and check against stopwords
        tokens = phrase.split()
        if len(tokens) == 0:
            return False

        # Minimum length check (e.g., at least 3 characters)
        if len(phrase) < 3:
            return False

        # Ensure the phrase has meaningful content (not just a single common word)
        # if all(token.lower() in self.stopwords for token in tokens):
        #     return False

        return True

    def filter_similar_keywords(self, keywords, threshold=0.8):
        """Filter out semantically similar keywords to keep only the most relevant."""
        # Calculate TF-IDF vector for all keywords
        vectorizer = TfidfVectorizer().fit_transform([kw["text"] for kw in keywords])
        vectors = vectorizer.toarray()
        # Compute cosine similarity matrix
        cosine_matrix = cosine_similarity(vectors)
        # Initialize a set to keep unique keywords
        unique_keywords = []
        for i, kw in enumerate(keywords):
            # Check if this keyword is not too similar to any selected unique keyword
            if all(
                cosine_matrix[i][j] < threshold
                for j in range(i)
                if keywords[j] in unique_keywords
            ):
                unique_keywords.append(kw)
        return unique_keywords

    def extract_key_phrases(
        self,
        text,
        max_phrases=40,
        min_score=0.5,
        min_phrases=5,
        similarity_threshold=0.8):
        """Extract key phrases with advanced filtering for concise and accurate output, including NER and overlapping management."""

        # Clean the input text
        cleaned_text = self.clean_text(text)

        # Extract keywords using YAKE, BERT, and NER
        yake_extractor = yake.KeywordExtractor(
            lan="en", n=3, dedupLim=0.9, top=max_phrases, features=None
        )
        yake_keywords = yake_extractor.extract_keywords(cleaned_text)
        bert_keywords = self.kw_model.extract_keywords(
            cleaned_text, keyphrase_ngram_range=(1, 3), stop_words="english"
        )
        ner_keywords = self.extract_entities(
            cleaned_text
        )  # Returns list of tuples [(text, type), ...]

        # Combined keywords with faster lookup using a dictionary
        combined_keywords = {}
        existing_keywords_set = set(self.keywords)

        # Process YAKE, BERT, and NER keywords in a single loop
        for kw_list, source_tag, default_score in [
            (yake_keywords, "yake", None),
            (bert_keywords, "bert", None),
            (ner_keywords, "ner", 1.0),
        ]:
            for item in kw_list:
                # Handle NER keywords separately
                if source_tag == "ner":
                    kw, entity_type = item
                    score = 1.0
                    tag = "ner"
                else:
                    kw, score = item
                    entity_type = None
                    tag = source_tag

                # Check if the phrase is valid (not just numbers or symbols) and is a noun phrase
                if not self.is_valid_phrase(kw):
                    continue

                # If the keyword passes the score filter or is an existing keyword
                if score >= min_score or kw in existing_keywords_set:
                    if kw in combined_keywords:
                        # Update existing keyword's score and tags only if the tag is not already added
                        if combined_keywords[kw]["tag"]  == "ner":
                            combined_keywords[kw]["score"] = combined_keywords[kw]["score"]
                        else:
                            combined_keywords[kw]["score"] = max(
                                combined_keywords[kw]["score"], score
                            )
                        if tag not in combined_keywords[kw]["tag"]:
                            combined_keywords[kw]["tag"] += f", {tag}"
                        if entity_type:  # Update type if it's an NER keyword
                            combined_keywords[kw]["type"] = entity_type
                        # Increment the count for existing keywords
                        combined_keywords[kw]["count"] += 1
                    else:
                        # Add new keyword with a count of 1
                        combined_keywords[kw] = {
                            "text": kw,
                            "score": score,
                            "tag": tag,
                            "type": entity_type,
                            "count": 1,  # Initialize count
                        }

        # Convert dictionary to sorted list of dictionaries
        sorted_combined_keywords = sorted(
            combined_keywords.values(), key=lambda x: x["score"], reverse=True
        )

        # Filter similar keywords to remove redundancy using Sentence-BERT
        sorted_combined_keywords = self.filter_similar_keywords(
            sorted_combined_keywords, similarity_threshold
        )

        # # Ensure the number of key phrases is between min_phrases and max_phrases
        # sorted_combined_keywords = sorted_combined_keywords[
        #     : max(max_phrases, min(min_phrases, len(sorted_combined_keywords)))
        # ]

        # Ensure all existing keywords are included, regardless of max_phrases
        final_keywords_dict = {kw["text"]: kw for kw in sorted_combined_keywords}

        return final_keywords_dict

    def extract_entities(self, text):
        """Extract named entities using spaCy."""
        doc = self.nlp(text)
        entities = [(ent.text, ent.label_) for ent in doc.ents]
        return entities

    def update_graph_node(self, url, score, found_keywords):
        """Update the graph node with new score and keywords found."""
        if url in self.graph:
            self.graph.nodes[url]["score"] = score
            self.graph.nodes[url]["keywords"] = found_keywords

    async def _process_feed(self, session, url):
        """Process RSS feed URLs in a separate thread."""
        # Fetch feed in an async way
        text = await self._fetch(session, url)

        # Define the blocking work to be done in a thread
        def process_feed_content(text, url):
            feed = feedparser.parse(text)
            for entry in feed.entries:
                link = entry.link
                if self._is_allowed_domain(self._normalize_url(link)):
                    priority, _ = self.relative_score(entry.title)
                    self._add_to_queue(link, "webpage", priority=priority, from_page=url)

        # If text is available, process it in a thread
        if text:
            await asyncio.to_thread(process_feed_content, text, url)

        
    async def _process_sitemap(self, session, url):
        """Process sitemap XML URLs."""
        text = await self._fetch(session, url)
        def do_work_thread_sitemap(self,text, url, add_to_queue):
            """Process the fetched XML text in a separate thread."""
            if text:
                bs = BeautifulSoup(text, "xml")
                for loc in bs.find_all("loc"):
                    add_to_queue(loc.text, "webpage", priority=0, from_page=url)
                for sitemap in bs.find_all("sitemap"):
                    add_to_queue(sitemap.text, "sitemap", priority=0, from_page=url)
        if text:
            # Create a thread pool executor to run do_work_thread in a separate thread
            with concurrent.futures.ThreadPoolExecutor() as executor:
                # Submit the task to the executor
                future = executor.submit(do_work_thread_sitemap, text, url, self._add_to_queue)
                # Optionally, wait for the result if needed
                await future

    async def crawl(self):
        """Start the crawling process."""
        ok = True
        tasks = []  # List to hold all asynchronous tasks

        with tqdm(total=len(self.urls_to_visit)) as pbar:
            async with aiohttp.ClientSession() as session:
                loop = asyncio.get_running_loop()

                while not self.urls_to_visit.empty():
                    for i in range(300):
                        if self.urls_to_visit.empty():
                            break

                        # Get the next URL to process
                        url_item = self.urls_to_visit.pop()
                        o = urlparse(url_item.url)

                        # Check robots.txt rules
                        if o.hostname in self.rob and not self.rob[o.hostname].can_fetch("*", url_item.url):
                            continue

                        # Skip if URL is already seen
                        if url_item.url in self.seen_urls:
                            continue

                        self.seen_urls.add(url_item.url)

                        # Depending on URL type, choose processing function
                        if url_item.url_type == "feed":
                            task = self._process_feed(session, url_item.url)
                        elif url_item.url_type == "sitemap":
                            task = self._process_sitemap(session, url_item.url)
                        else:
                            task = self._process_url( session, url_item.url)
                        tasks.append(task)  # Add the task to the list
                    
                    # Wait for tasks to complete in batches
                    await asyncio.gather(*tasks)
                    pbar.total = len(self.urls_to_visit) + self.good + self.bad
                    pbar.update(len(tasks))
                    tasks.clear()  # Clear tasks for the next batch
                    self.workout_new_keyphrases()
                
                # After all URLs are processed
            self.workout_new_keyphrases()




from bbc_scripe_cdx import get_all_urls
from ovarit import ovarit_domain_scrape
import utils.keywords as kw


async def main():
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
    await crawler.crawl()


# Run the main function
if __name__ == "__main__":
    asyncio.run(main())
