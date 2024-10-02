import asyncio
import importlib
import os
import re
import sqlite3
import ssl
import sys
import aiohttp
import logging
import csv
import json
from urllib.parse import quote, unquote, urljoin, urlparse, urlunparse
from bs4 import BeautifulSoup
from threading import Lock
import certifi
from readabilipy import simple_json_from_html_string
import urllib3
from bbc_scripe_cdx import get_all_urls_cdx
from utils.ovarit import ovarit_domain_scrape
from utils.reddit import reddit_domain_scrape
import utils.keywords as kw
from utils.RobotsSql import RobotsSql
from utils.SQLDictClass import SQLDictClass
from utils.BackedURLQueue import BackedURLQueue, URLItem
import traceback
import feedparser.parsers
from javascript import require
import requests

processArticle = require("./utils/ProcessArticle.js")

# Initialize logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

headers = {
    "User-Agent": "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:130.0) Gecko/20100101 Firefox/130.0",
    "Accept": "*/*",
    "Accept-Language": "en-US,en;q=0.5",
    "Sec-Fetch-Dest": "script",
    "Sec-Fetch-Mode": "no-cors",
    "Sec-Fetch-Site": "cross-site",
}


class KeyPhraseFocusCrawler:
    def __init__(
        self,
        conn,
        start_urls,
        feeds,
        output_csv="crawler_results_CCC.csv",
        allowed_subdirs=[],
        keywords=[],
        anti_keywords=[],
        irrelevant_for_keywords=[],
        seed_urls=[],
        plugins=[],
    ):
        self.irrelevant_for_keywords = irrelevant_for_keywords
        self.irrelevant_for_anti_keywords = []
        self.conn = conn
        self.robots = RobotsSql(self.conn)
        self.keyphrases = SQLDictClass(conn, table_name="keyphrases")
        self.urls_to_visit = BackedURLQueue(
            conn, min_heap=False, table_name="urls_to_visit"
        )
        self.plugins = plugins
        self.allowed_subdirs = allowed_subdirs
        self.keywords = keywords
        self.anti_keywords = anti_keywords
        self.output_csv = output_csv
        self.semaphore = asyncio.Semaphore(200)
        self.processed_urls_count = 0
        self.lock = Lock()
        self.lockfs = Lock()
        self.start_urls = start_urls
        self._initialize_output_csv()

        # Initialize the URL queue with starting URLs and feeds
        self._initialize_urls(start_urls, feeds, seed_urls)
        self._initialize_plugins()
    
    def _initialize_output_csv(self):
        """Ensure the CSV file is properly initialized with the correct headers."""
        try:
            with open(self.output_csv, mode="a", newline="", buffering=1) as csv_file:
                csv_writer = csv.writer(csv_file)
                if csv_file.tell() == 0:
                    csv_writer.writerow(
                        [
                            "URL", "Score", "Keywords Found", "type_page", "description",
                            "headline", "datePublished", "dateModified", "author", "byline",
                            "excerpt", "lang"
                        ]
                    )
        except Exception as e:
            logger.error(f"Error initializing CSV file: {e}")
        
    def _initialize_plugins(self):
        """Initialize and execute the `on_start` method of all plugins."""
        for plugin in self.plugins:
            plugin.on_start(self)

    def _apply_after_fetch_plugins(self, url, content):
        """Run `after_fetch` on all plugins."""
        for plugin in self.plugins:
            content = plugin.after_fetch(url, content, self)
        return content

    async def _apply_before_fetch_plugins(self):
        """Execute the `before_fetch` method of all plugins."""
        for plugin in self.plugins:
            await plugin.before_fetch(self)

    def relative_score(self, text):
        """
        Calculate the relevance score of a page or anchor text based on keywords and key phrases,
        handling both relevant and irrelevant keywords with context-aware logic and preventing substring conflicts.
        """
        score = 0
        anti_score = 0
        relevant_window = 50  # Number of characters around a keyword to check for irrelevant content

        found_keywords = []
        found_anti_keywords = []

        def contains_irrelevant(phrases, segment):
            return any(
                re.search(r"\b" + re.escape(phrase) + r"\b", segment, re.IGNORECASE)
                for phrase in phrases
            )

        for keyword in self.keywords:
            for match in re.finditer(
                r"\b" + re.escape(keyword) + r"\b", text, re.IGNORECASE
            ):
                start, end = match.start(), match.end()
                if not contains_irrelevant(
                    self.irrelevant_for_keywords,
                    text[max(0, start - relevant_window): end + relevant_window],
                ):
                    found_keywords.append((keyword, start, end))

        processed_positions = set()
        for keyword, start, end in found_keywords:
            if not any(pos in processed_positions for pos in range(start, end)):
                keyword_weight = len(keyword.split())
                score += keyword_weight
                processed_positions.update(range(start, end))

        for anti_keyword in self.anti_keywords:
            for match in re.finditer(
                r"\b" + re.escape(anti_keyword) + r"\b", text, re.IGNORECASE
            ):
                start, end = match.start(), match.end()
                if not contains_irrelevant(
                    self.irrelevant_for_anti_keywords,
                    text[max(0, start - relevant_window): end + relevant_window],
                ):
                    found_anti_keywords.append((anti_keyword, start, end))

        processed_anti_positions = set()
        for anti_keyword, start, end in found_anti_keywords:
            if not any(pos in processed_positions for pos in range(start, end)):
                anti_score += 1
                processed_anti_positions.update(range(start, end))

        final_score = score - anti_score
        return final_score, [kw for kw, _, _ in found_keywords], [kw for kw, _, _ in found_anti_keywords]

    def _initialize_urls(self, start_urls, feeds=[], seed_urls=[]):
        seen = set()
        for url in start_urls:
            self._add_to_queue(url, "start", priority=0)
            o = urlparse(url)
            for new_url in reddit_domain_scrape(o.hostname):
                if new_url[0] in seen:
                    continue
                seen.add(new_url[0])
                final_score, _, _ = self.relative_score(new_url[1])
                self._add_to_queue(new_url[0], "webpage", priority=final_score)
            for new_url in ovarit_domain_scrape(o.hostname):
                if new_url[0] in seen:
                    continue
                seen.add(new_url[0])
                final_score, _, _ = self.relative_score(new_url[1])
                self._add_to_queue(new_url[0], "webpage", priority=final_score)
            for new_url in get_all_urls_cdx(o.hostname):
                if new_url in seen:
                    continue
                seen.add(new_url)
                self._add_to_queue(new_url, "webpage")

        for feed in feeds:
            self._add_to_queue(feed, "feed", priority=0)
        for seed_url in seed_urls:
            self._add_to_queue(seed_url["URL"], "webpage", priority=seed_url["Score"])

    def _add_to_queue(self, url, url_type, priority=0):
        normalized_url = self.normalize_url(url)
        if not self.is_valid_url(normalized_url):
            logger.error(f"Invalid URL found: {url}")
            return
        if not self.urls_to_visit.have_been_seen(normalized_url):
            url_item = URLItem(url=normalized_url, url_score=priority, page_type=url_type)
            self.urls_to_visit.push(url_item)

    def is_valid_url(self, url):
        try:
            parsed = urlparse(url)
            return all([parsed.scheme, parsed.netloc])
        except ValueError:
            return False

    def normalize_url(self, url):
        parsed_url = urlparse(url)
        scheme = parsed_url.scheme.lower()
        hostname = parsed_url.hostname.lower() if parsed_url.hostname else None
        path = unquote(parsed_url.path)
        if not path.endswith("/"):
            path += "/"
        port = parsed_url.port
        if (scheme == "http" and port == 80) or (scheme == "https" and port == 443):
            port = None

        normalized_url = urlunparse(
            (scheme, f"{hostname}:{port}" if port else hostname, quote(path), "", "", "")
        )

        return normalized_url

    async def _fetch(self, session, url_item):
        url = url_item.url
        if not self.is_valid_url(url):
            logger.error(f"Invalid URL fetched from the queue: {url}")
            return None, None, None

        mime = None
        async with self.semaphore:
            for i in range(20):  # 20 retries
                try:
                    ssl_context = ssl.create_default_context(cafile=certifi.where())
                    async with session.get(url, ssl=ssl_context, headers=headers, timeout=60) as response:
                        mime = response.headers.get("Content-Type")

                        if response.status == 200:
                            try:
                                encoding = response.charset or "utf-8"
                                text = await response.text(encoding=encoding)
                                text = self._apply_after_fetch_plugins(url, text)
                                return text, mime, response.status
                            except UnicodeDecodeError as e:
                                logger.error(f"Unicode decode error fetching {url}: {e}")
                                try:
                                    text = await response.text(encoding="latin-1")
                                    text = self._apply_after_fetch_plugins(url, text)
                                    return text, mime, response.status
                                except Exception as fallback_error:
                                    
                                    logger.error(f"Fallback decode error fetching {url}: {fallback_error}")
                                    return None, mime, response.status
                        elif response.status == 404:
                            logger.warning(f"URL not found (404): {url}")
                            self.urls_to_visit.mark_seen(url)
                            text = await response.text()
                            return text, mime, response.status
                        else:
                            logger.warning(f"Failed to fetch {url}, status code: {response.status}, retry count: {i}")
                except Exception as e:
                    logger.error(f"Error fetching {url}, attempt {i + 1}/20: {e}")

            logger.error(f"Max retries reached for {url}, will reattempt later")
            return None, mime, response.status

    async def _process_feed(self, text, url):
        """Process RSS feed URLs."""
        logger.info(f"Processing feed URL: {url}")
        try:
            for plugin in self.plugins:
                plugin.before_process_feed(url, text, self)
            if text:
                feed = feedparser.parse(text)
                for entry in feed.entries:
                    link = entry.link
                    score, _, _ = self.relative_score(entry.title)
                    self._add_to_queue(link, "webpage", priority=score)
            else:
                logger.warning(f"Failed to fetch feed content for URL: {url}")
        except Exception as e:
            logger.error(f"Error processing feed {url}: {e}")
    
    def should_crawl(self, bs, headers=None):
        """
        Determine whether to crawl a page based on its meta robots tags and X-Robots-Tag header.

        Parameters:
        - bs: BeautifulSoup object of the page.
        - headers: HTTP headers from the response.

        Returns:
        - bool: True if the page should be crawled, False otherwise.
        """
        logger.debug("Checking if page should be crawled based on meta robots tags and headers.")
        index_allowed = True  # Assume we can index unless specified otherwise
        follow_allowed = True  # Assume we can follow unless specified otherwise

        # Check for the meta robots tag
        robots_meta = bs.find("meta", attrs={"name": "robots"})
        if robots_meta:
            content = robots_meta.get("content", "").lower()
            if "noindex" in content:
                index_allowed = False  # No indexing is allowed
            if "nofollow" in content:
                follow_allowed = False  # No following is allowed

        # Check for X-Robots-Tag HTTP header
        if headers is not None:
            x_robots_tag = headers.get("X-Robots-Tag", "").lower()
            if "noindex" in x_robots_tag:
                index_allowed = False  # No indexing is allowed
            if "nofollow" in x_robots_tag:
                follow_allowed = False  # No following is allowed

        # Final decision: If following is allowed, we can crawl regardless of indexing
        return follow_allowed or index_allowed
    
    def is_spider_trap(self, url):
        """Detect if a URL is likely a spider trap."""
        return False
    
    async def _process_sitemap(self, text, url):
        if text:
            bs = BeautifulSoup(text, "xml")
            for loc in bs.find_all("loc"):
                self._add_to_queue(loc.text, "webpage", priority=0)
                logger.info(f"Added sitemap URL: {loc.text} to the queue as webpage.")
        else:
            self.urls_to_visit.mark_seen(url)
            logger.warning(f"Failed to fetch sitemap content for URL: {url}")
    
    def extract_metadata(self, bs):
        """Extract metadata from BeautifulSoup object, including RDFa, JSON-LD, Microdata, and social media tags (Twitter, OpenGraph)."""
        metadata = {}

        try:
            # Extract title (from OpenGraph, Twitter, or regular title tag)
            title = (
                bs.find("meta", attrs={"property": "og:title"}) or
                bs.find("meta", attrs={"name": "twitter:title"}) or
                bs.find("meta", attrs={"name": "title"}) or
                bs.find("title")
            )
            if title:
                metadata["title"] = title.get("content") or title.get_text(strip=True)

            # Extract description (OpenGraph, Twitter, or meta description)
            description = (
                bs.find("meta", attrs={"property": "og:description"}) or
                bs.find("meta", attrs={"name": "twitter:description"}) or
                bs.find("meta", attrs={"name": "description"})
            )
            if description:
                metadata["description"] = description.get("content")

            # Extract keywords (meta tag)
            keywords = bs.find("meta", attrs={"name": "keywords"})
            if keywords:
                metadata["keywords"] = keywords.get("content")

            # Extract author (from meta tag)
            author = bs.find("meta", attrs={"name": "author"})
            if author:
                metadata["author"] = author.get("content")

            # Extract OpenGraph data (Facebook and others)
            og_image = bs.find("meta", attrs={"property": "og:image"})
            if og_image:
                metadata["og_image"] = og_image.get("content")

            og_url = bs.find("meta", attrs={"property": "og:url"})
            if og_url:
                metadata["og_url"] = og_url.get("content")

            og_site_name = bs.find("meta", attrs={"property": "og:site_name"})
            if og_site_name:
                metadata["og_site_name"] = og_site_name.get("content")

            # Extract Twitter Card data
            twitter_image = bs.find("meta", attrs={"name": "twitter:image"})
            if twitter_image:
                metadata["twitter_image"] = twitter_image.get("content")

            twitter_site = bs.find("meta", attrs={"name": "twitter:site"})
            if twitter_site:
                metadata["twitter_site"] = twitter_site.get("content")

            # Extract JSON-LD (Linked Data) metadata
            json_ld_elements = bs.find_all("script", attrs={"type": "application/ld+json"})
            json_ld_data = []
            for json_ld in json_ld_elements:
                try:
                    json_data = json.loads(json_ld.string)
                    json_ld_data.append(json_data)
                except (json.JSONDecodeError, TypeError) as e:
                    logger.error(f"Failed to decode JSON-LD: {e}")

            if json_ld_data:
                metadata["json_ld"] = json_ld_data

            # Extract Microdata (using itemprop and itemscope)
            microdata_items = bs.find_all(True, attrs={"itemscope": True})
            microdata = []
            for item in microdata_items:
                properties = {}
                for prop in item.find_all(True, attrs={"itemprop": True}):
                    prop_name = prop.get("itemprop")
                    prop_value = prop.get("content", prop.get_text(strip=True))
                    properties[prop_name] = prop_value
                if properties:
                    microdata.append(properties)

            if microdata:
                metadata["microdata"] = microdata

            # Extract RDFa metadata (using vocab, typeof, and property attributes)
            rdfa_items = []
            for element in bs.find_all(True, attrs={"property": True}):
                rdfa_property = element.get("property")
                rdfa_content = element.get("content", element.get_text(strip=True))
                rdfa_items.append({rdfa_property: rdfa_content})

            if rdfa_items:
                metadata["rdfa"] = rdfa_items

            # Extract publication and modification dates (OpenGraph and standard meta tags)
            pub_date = (
                bs.find("meta", attrs={"property": "article:published_time"}) or
                bs.find("meta", attrs={"name": "pubdate"})
            )
            if pub_date:
                metadata["datePublished"] = pub_date.get("content")

            mod_date = (
                bs.find("meta", attrs={"property": "article:modified_time"}) or
                bs.find("meta", attrs={"name": "dateModified"})
            )
            if mod_date:
                metadata["dateModified"] = mod_date.get("content")

            # Extract language from the HTML tag or meta tag
            language = bs.find("meta", attrs={"http-equiv": "content-language"}) or bs.find("html")
            if language:
                metadata["lang"] = language.get("lang") or language.get("content")

            # Extract headline (useful for articles)
            headline = (
                bs.find("meta", attrs={"property": "og:title"}) or
                bs.find("meta", attrs={"name": "headline"})
            )
            if headline:
                metadata["headline"] = headline.get("content")

            # Extract excerpt or summary
            excerpt = bs.find("meta", attrs={"name": "description"})
            if excerpt:
                metadata["excerpt"] = excerpt.get("content")

        except Exception as e:
            logger.error(f"Failed to extract metadata: {e}")

        return metadata


    
    def write_to_csv(self, url, score, found_keywords, metadata):
        """Write the extracted data to a CSV file."""
        try:
            # Define the CSV file path (you can modify this to your desired file path)
            csv_file = "output_data.csv"

            # Define the header for the CSV
            fieldnames = ["url", "score", "found_keywords", "title", "description", "author", "site_name", "excerpt"]

            # Check if the file already exists to avoid writing the header again
            write_header = not os.path.exists(csv_file)

            # Write the row to CSV
            with open(csv_file, mode='a', newline='', encoding='utf-8') as file:
                writer = csv.DictWriter(file, fieldnames=fieldnames)
                if write_header:
                    writer.writeheader()  # Write header if file is new

                row = {
                    "url": url,
                    "score": score,
                    "found_keywords": ", ".join(found_keywords),
                    "title": metadata.get("title", ""),
                    "description": metadata.get("description", ""),
                    "author": metadata.get("author", ""),
                    "site_name": metadata.get("site_name", ""),
                    "excerpt": metadata.get("excerpt", "")
                }

                writer.writerow(row)
                logger.debug(f"Data written to CSV for {url}")

        except Exception as e:
            logger.error(f"Failed to write to CSV for {url}: {e}")

    def extract_links(self, bs, url, score):
        """Extract and queue new links from the page content."""
        import urllib.parse
        try:
            links = bs.find_all('a', href=True)
            for link in links:
                href = link['href']
                if href.startswith('/'):
                    # Convert relative URLs to absolute
                    href = urllib.parse.urljoin(url, href)
                    logger.debug(f"1Extracted {len(links)} links from {url}")
                
                # Only queue valid URLs (you might want to implement additional validation)
                # if self.is_valid_url(href):
                if True:
                    logger.debug(f" 2Extracted {len(links)} links from {url}")
                    self.urls_to_visit.add(href, score)
                    logger.debug(f"Queued link {href} with score {score}")
        
        except Exception as e:
            logger.error(f"Failed to extract links from {url}: {e}")

    def extract_A(self, bs, url, score):
        """
        Extract anchor tags (<a>) from the webpage for additional metadata or important information.

        Parameters:
            bs (BeautifulSoup): The parsed HTML content.
            url (str): The current URL being processed.
            score (int): The score assigned to the page based on keyword relevance.
        """
        import urllib.parse
        print("Extracting anchor tags")
        try:
            logger.debug(f"Extracting anchor tags from {url}")

            # Initialize a list to store extracted anchor data
            anchor_data = []

            # Find all anchor tags with href attributes
            anchors = bs.find_all('a', href=True)

            for anchor in anchors:
                anchor_url = anchor['href']
                anchor_text = anchor.getText(strip=True)
                print(anchor_url,anchor_text)
                # temp_score = score
                # if anchor_text:
                #     temp_score, found_keywords, _ = self.relative_score(anchor_text)

                # # Skip empty anchor text or invalid links
                # if not anchor_text or not self.is_valid_url(anchor_url):
                #     continue
                
                # Convert relative URLs to absolute URLs
                if anchor_url.startswith('/'):
                    anchor_url = urllib.parse.urljoin(url, anchor_url)

                # You might want to log or process important anchors based on conditions
                logger.debug(f"Found anchor: {anchor_text} ({anchor_url})")


                # Optionally, you could queue these links for further crawling
                self.urls_to_visit.add(anchor_url, 0)
                
                # log the anchor data
                logger.debug(f"Queued link {anchor_url} with score {0}")


        except Exception as e:
            logger.error(f"Error extracting anchor tags from {url}: {e}")

    async def _analyze_page_content(self, text, url):
        """Analyze and process webpage content."""

        def process_text():
            excerpt, siteName, lang, byline, tlite, content = None, None, None, None, None, None
            try:
                bs = BeautifulSoup(text, "html.parser")
                logger.debug("BeautifulSoup parser initialized.")

                # Process the article using the processArticle module
                try:
                    if processArticle:
                        article = processArticle.ProcessArticle(text, url)         
                        if article:
                            excerpt = article.excerpt
                            siteName = article.siteName
                            lang = article.lang
                            byline = getattr(article, 'byline', None)
                            tlite = getattr(article, 'tlite', None)
                            content = BeautifulSoup(str(article.content), "html.parser").get_text(separator=" ") if article.content else None
                except Exception as e:
                    logger.error(f"ProcessArticle failed for {url}: {e}")
                    logger.debug(f"Continuing without ProcessArticle for {url}")

                # Backup metadata extraction if ProcessArticle fails
                if not content:
                    article = simple_json_from_html_string(text)
                    if article:
                        byline = article.get('byline', "No author found")
                        tlite = article.get('tlite', "No title found")
                        content = BeautifulSoup(str(article.get('content')), "html.parser").get_text(separator=" ") if article.get('content') else None

                if not content:
                    logger.debug("Attempting fallback metadata extraction.")
                    content = bs.get_text(separator=" ")
                    excerpt = bs.find("meta", {"name": "description"}) or bs.find("p")
                    if excerpt:
                        excerpt = excerpt.get_text(strip=True)[:255]

                    # Fallback methods to get title, site name, etc.
                    tlite = bs.title.string if bs.title else "No title found"
                    siteName = bs.find("meta", property="og:site_name")
                    siteName = siteName["content"] if siteName else "Unknown site"
                    lang = bs.find("html")["lang"] if bs.find("html") else "Unknown language"
                    byline = bs.find("meta", name="author")
                    byline = byline["content"] if byline else "No author found"

                # Remove unwanted elements
                unwanted_tags = ["script", "style", "footer", "header", "aside", "nav", "noscript", "form", "iframe", "button"]
                unwanted_classes_ids = ["ads", "sponsored", "social", "share", "comment", "comments", "related", "subscription", "sidebar"]

                for tag in unwanted_tags:
                    for element in bs.find_all(tag):
                        element.extract()

                for class_id in unwanted_classes_ids:
                    for element in bs.find_all(True, {"class": class_id}):
                        element.extract()
                    for element in bs.find_all(True, {"id": class_id}):
                        element.extract()

                # Remove hidden content (based on CSS or classes)
                hidden_classes = ["hidden", "hide", "sr-only", "visually-hidden"]
                for hidden in bs.find_all(class_=hidden_classes):
                    hidden.extract()

                # Use heuristic to find the most meaningful content (e.g., <article> tags or large blocks of text)
                article_content = bs.find("article")
                if not article_content:
                    article_content = max(bs.find_all("div"), key=lambda x: len(x.get_text()), default=bs)

                content = article_content.get_text(separator=" ")

                # Further clean the content (remove non-ASCII characters and excess whitespace)
                content = content.encode("ascii", "ignore").decode()  # Remove non-ASCII chars
                content = re.sub(r'\s+', ' ', content).strip()  # Remove extra spaces/newlines

                score, found_keywords, _ = self.relative_score(content)
                self.extract_A(bs, url, score)
                self.extract_links(bs, url, score)
                self.urls_to_visit.set_page_score_page(url, score)
                
                if score > 0:
                    metadata = self.extract_metadata(bs)

                    # Add metadata fields to the extracted data
                    metadata.update({
                        "excerpt": excerpt,
                        "siteName": siteName,
                        "lang": lang,
                        "byline": byline,
                        "tlite": tlite,
                    })

                    self.write_to_csv(url, score, found_keywords, metadata)

            except Exception as e:
                logger.error(f"Error parsing content from {url}: {e}")
                logger.debug(f"Exception details: {traceback.format_exc()}")
                exc_type, exc_obj, exc_tb = sys.exc_info()
                fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                logger.error(f"Exception type: {exc_type}, File: {fname}, Line: {exc_tb.tb_lineno}")

            self.urls_to_visit.mark_seen(url)

        process_text()
        self.urls_to_visit.mark_seen(url)



    def update_csv(self, url, score, found_keywords, metadata):
        """Update the CSV file for revisited URLs."""
        if score <= 0:
            self.remove_from_csv(url)
        else:
            self.write_to_csv(url, score, found_keywords, metadata)

    async def _process_url(self, text, url):
        # self._apply_before_fetch_plugins(url)
        """Fetch and process a webpage."""
        if text is not None:
            text = self._apply_after_fetch_plugins(url, text)
            await self._analyze_page_content(text, url)
        if text is None or not text.strip():  # Ensure text is not None and not empty
            logger.error(f"Empty or None response for URL: {url}")
            return

    async def route_based_on_mime(self, url, mime, text, url_item):
        """Route processing based on the MIME type."""
        
        # Ensure we use `url_item` for fallback handling, not just `url` (the string)
        if "feed.rss" in url_item.url:
            return await self._process_feed(text, url)
        if "feed.atom" in url_item.url:
            return await self._process_feed(text, url)
        if "rss.xml" in url_item.url:
            return await self._process_feed(text, url)
        if "atom.xml" in url_item.url:
            return await self._process_feed(text, url)
        elif url_item.page_type == "feed":
            return await self._process_feed(text, url)
        elif url_item.page_type == "sitemap":
            return await self._process_sitemap(text, url)
        
        mime_type_mapping = {
            "text/html": self._process_url,
            "text/xhtml": self._process_url,
            "application/rss+xml": self._process_feed,
            "application/atom+xml": self._process_feed,
            "application/xml": self._process_sitemap,
            "text/xml": self._process_sitemap,
        }

        if mime is not None:
            for mime_prefix, handler in mime_type_mapping.items():
                if mime.startswith(mime_prefix):
                    return await handler(text, url)
        if url_item.page_type == "webpage":
            return await self._process_url(text, url)
        self.urls_to_visit.mark_seen(url)


        # Default action for unsupported MIME types
        logger.warning(f"Unsupported MIME type: {mime} for URL: {url}")
        return None


    async def crawl_main(self):
        batch_size = 10

        async with aiohttp.ClientSession() as session:
            while not self.urls_to_visit.empty():
                tasks = []
                for _ in range(batch_size):
                    if self.urls_to_visit.empty():
                        break
                    url_item = self.urls_to_visit.pop()
                    if url_item:
                        logger.info(f"Processing URL: {url_item.url} with score {url_item.url_score}")
                    else:
                        logger.info("No URL found in the queue")
                        break
                    
                    # Check if the URL contains one of the allowed subdirectories
                    if not any(subdir in url_item.url for subdir in self.allowed_subdirs):
                        logger.info(f"Skipping URL not in allowed subdirs: {url_item.url}")
                        continue  # Skip this URL if it doesn't match the allowed subdirectories
                    self.urls_to_visit.mark_processing(url_item.url)
                    if url_item is None or not self.is_valid_url(url_item.url):
                        logger.error(f"Invalid or None URL fetched from the queue: {url_item}")
                        continue

                    async def task(url_item, session):
                        try:
                            text, mime, status = await self._fetch(session, url_item)
                            if text:
                                # Pass the correct `url_item` object to the routing function
                                await self.route_based_on_mime(url_item.url, mime, text, url_item)
                            else:
                                self.urls_to_visit.push(url_item)  # Re-queue the `URLItem` object, not a string
                        except Exception as e:
                            logger.error(f"Error processing URL: {url_item.url} {e}")
                            self.urls_to_visit.push(url_item)  # Ensure you re-queue the `URLItem` object


                    tasks.append(asyncio.create_task(task(url_item, session)))

                if tasks:
                    await asyncio.gather(*tasks)

        for plugin in self.plugins:
            plugin.on_finish(self)

    async def crawl_start(self):
        logger.info("calling crawl_main")
        await self.crawl_main()

async def main():
    if not os.path.exists("databaces"):
        os.mkdir("databaces")

    conn = sqlite3.connect("databaces/spider_good2.db", check_same_thread=False)
    start_urls = ["https://www.bbc.co.uk", "https://feeds.bbci.co.uk/"]
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

    plugins = []
    plugins_dir = "plugins"

    for plugin_file in os.listdir(plugins_dir):
        if plugin_file.endswith(".py") and plugin_file != "__init__.py":
            plugin_name = plugin_file[:-3]
            try:
                plugin_module = importlib.import_module(f"plugins.{plugin_name}")
                plugin_class = getattr(plugin_module, "main")
                plugins.append(plugin_class())
            except (ImportError, AttributeError) as e:
                print(f"Failed to load plugin {plugin_name}: {e}")

    allowed_subdirs = ["www.bbc.co.uk/"]
    keywords = kw.Little_List

    crawler = KeyPhraseFocusCrawler(
        conn,
        start_urls,
        feeds,
        allowed_subdirs=allowed_subdirs,
        keywords=keywords,
        irrelevant_for_keywords=kw.irrelevant_for_keywords,
        anti_keywords=kw.ANTI_KEYWORDS,
        # plugins=plugins
    )

    logger.info("Starting crawler...")
    await crawler.crawl_start()

if __name__ == "__main__":
    asyncio.run(main())
