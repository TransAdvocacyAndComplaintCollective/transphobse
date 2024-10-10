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
import certifi
from readabilipy import simple_json_from_html_string
import tqdm

# Removed extruct and w3lib.html imports
# from w3lib.html import get_base_url

# Local imports
from bbc_scripe_cdx import get_all_urls_cdx
from utils.duckduckgo import lookup_duckduckgos
import utils.keywords as kw
from utils.RobotsSql import RobotsSql
from utils.SQLDictClass import SQLDictClass
from utils.BackedURLQueue import BackedURLQueue, URLItem
import traceback
import feedparser
from javascript import require
import requests

from utils.ovarit import ovarit_domain_scrape
from utils.reddit import reddit_domain_scrape

processArticle = require("./utils/ProcessArticle.js")

# Initialize logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

headers = {
    "User-Agent": "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:131.0) Gecko/20100101 Firefox/131.0",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/png,image/svg+xml,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
    "Upgrade-Insecure-Requests": "1",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
    "Sec-Fetch-User": "?1",
    "Sec-GPC": "1",
    "If-None-Match": 'W/"4226e-CWVsNkTGzsL1l//d1mu/AILkgTM"',
    "Priority": "u=0, i",
}


def ensure_int(value):
    if isinstance(value, bytes):
        return int.from_bytes(value, byteorder="big")
    return int(value)


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
        self.semaphore = asyncio.Semaphore(500)
        self.processed_urls_count = 0
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
                    # These are the headers you want to add to the CSV
                    csv_writer.writerow(
                        [
                            "URL",  # The URL of the page
                            "Score",  # The relevance score
                            "Keywords Found",  # The keywords found on the page
                            "type_page",  # The type of the page (e.g., feed, webpage, etc.)
                            "description",  # The page description
                            "headline",  # The headline of the page (if it's an article)
                            "datePublished",  # The publication date of the page
                            "dateModified",  # The modification date of the page
                            "author",  # The author of the page
                            "byline",  # The byline (author details or credits)
                            "excerpt",  # An excerpt from the page (if available)
                            "lang",  # The language of the page
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
        relevant_window = (
            50  # Number of characters around a keyword to check for irrelevant content
        )

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
                    text[max(0, start - relevant_window) : end + relevant_window],
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
                    text[max(0, start - relevant_window) : end + relevant_window],
                ):
                    found_anti_keywords.append((anti_keyword, start, end))

        processed_anti_positions = set()
        for anti_keyword, start, end in found_anti_keywords:
            if not any(pos in processed_positions for pos in range(start, end)):
                anti_score += 1
                processed_anti_positions.update(range(start, end))

        final_score = score - anti_score
        return (
            final_score,
            [kw for kw, _, _ in found_keywords],
            [kw for kw, _, _ in found_anti_keywords],
        )

    def _initialize_urls(self, start_urls, feeds=[], seed_urls=[]):
        seen = set()  # To keep track of seen URLs
        for url in start_urls:
            # Add start URL to the queue
            self._add_to_queue(url, "start", priority=0)

            o = urlparse(url)

            # DuckDuckGo lookup
            for duck in lookup_duckduckgos(self.keywords, o.hostname):
                title = duck.get("title")
                body = duck.get("body")
                href = duck.get("href")
                final_score = 0

                if title:
                    temp_score, _, _ = kw.relative_keywords_score(title)
                if body:
                    temp_score2, _, _ = kw.relative_keywords_score(body)
                    if temp_score2 > temp_score:
                        final_score = temp_score2
                    else:
                        final_score = temp_score
                else:
                    final_score = temp_score

                self._add_to_queue(href, "webpage", priority=final_score)

            # Reddit domain scrape
            for new_url in reddit_domain_scrape(o.hostname):
                if new_url[0] in seen:
                    continue
                seen.add(new_url[0])
                final_score, _, _ = kw.relative_keywords_score(new_url[1])
                self._add_to_queue(new_url[0], "webpage", priority=final_score)

            # Ovarit domain scrape
            for new_url in ovarit_domain_scrape(o.hostname):
                if new_url[0] in seen:
                    continue
                seen.add(new_url[0])
                final_score, _, _ = kw.relative_keywords_score(new_url[1])
                self._add_to_queue(new_url[0], "webpage", priority=final_score)

            # CDX URL scraping
            for new_url in get_all_urls_cdx(o.hostname):
                if new_url in seen:
                    continue
                seen.add(new_url)
                self._add_to_queue(new_url, "webpage")

        # Add feeds
        for feed in feeds:
            self._add_to_queue(feed, "feed", priority=0)

        # Add seed URLs with their scores
        for seed_url in seed_urls:
            self._add_to_queue(seed_url["URL"], "webpage", priority=seed_url["Score"])

    def _add_to_queue(self, url, url_type, priority=0):
        normalized_url = self.normalize_url(url)
        if not self.is_valid_url(normalized_url):
            logger.error(f"Invalid URL found: {url}")
            return
        if not self.urls_to_visit.have_been_seen(normalized_url):
            url_item = URLItem(
                url=normalized_url, url_score=priority, page_type=url_type
            )
            self.urls_to_visit.push(url_item)

    def is_valid_url(self, url):
        try:
            parsed = urlparse(url)
            return all([parsed.scheme, parsed.netloc])
        except ValueError:
            return False

    def convert_archive_url(self, archive_url):
        """Convert a Web Archive URL to its live version."""
        if "web.archive.org" in archive_url:
            # Find the position after the date part in the archive URL
            live_url_start = archive_url.find("/https://") + 1
            # Extract the live URL
            live_url = archive_url[live_url_start:]
            return live_url
        return archive_url

    def normalize_url(self, url):
        """Normalize and clean the URL."""
        # First, convert archive URL if necessary
        url = self.convert_archive_url(url)

        # Then proceed to normalize the URL
        parsed_url = urlparse(url)
        scheme = parsed_url.scheme.lower()
        hostname = parsed_url.hostname.lower() if parsed_url.hostname else None
        path = unquote(parsed_url.path).rstrip("/") + "/"
        port = parsed_url.port

        if (scheme == "http" and port == 80) or (scheme == "https" and port == 443):
            port = None

        normalized_url = urlunparse(
            (
                scheme,
                f"{hostname}:{port}" if port else hostname,
                quote(path),
                "",
                "",
                "",
            )
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
                    async with session.get(
                        url, ssl=ssl_context, headers=headers, timeout=60
                    ) as response:
                        mime = response.headers.get("Content-Type")

                        if response.status == 200:
                            try:
                                encoding = response.charset or "utf-8"
                                text = await response.text(encoding=encoding)
                                text = self._apply_after_fetch_plugins(url, text)
                                return text, mime, response.status
                            except UnicodeDecodeError as e:
                                logger.error(
                                    f"Unicode decode error fetching {url}: {e}"
                                )
                                try:
                                    text = await response.text(encoding="latin-1")
                                    text = self._apply_after_fetch_plugins(url, text)
                                    return text, mime, response.status
                                except Exception as fallback_error:
                                    logger.error(
                                        f"Fallback decode error fetching {url}: {fallback_error}"
                                    )
                                    return None, mime, response.status
                        elif response.status == 404:
                            logger.warning(f"URL not found (404): {url}")
                            self.urls_to_visit.mark_seen(url)
                            text = await response.text()
                            return text, mime, response.status
                        else:
                            logger.warning(
                                f"Failed to fetch {url}, status code: {response.status}, retry count: {i}"
                            )
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
                    score, found_keywords, _ = kw.relative_keywords_score(entry.title)
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
        logger.debug(
            "Checking if page should be crawled based on meta robots tags and headers."
        )
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

    def extract_metadata(self, html_content, url):
        """Extract metadata from HTML content using BeautifulSoup."""
        try:
            bs = BeautifulSoup(html_content, "html.parser")

            # Process microdata using BeautifulSoup
            metadata = self.process_microdata_bs(bs)

            # If microdata is empty, try to extract JSON-LD
            if not metadata:
                metadata = self.process_json_ld_bs(bs)

            # If still no metadata, fallback to standard meta tags
            if not metadata:
                metadata = self.process_standard_meta(bs)

            return metadata
        except Exception as e:
            logger.exception(f"Failed to extract metadata from {url}: {e}")
            return {}

    def process_microdata_bs(self, bs):
        """Process and normalize Microdata using BeautifulSoup."""
        metadata = {}
        # Find all items with itemscope but no itemprop (top-level items)
        items = bs.find_all(attrs={"itemscope": True, "itemprop": False})
        for item in items:
            item_type = item.get("itemtype", "")
            # Normalize the item type
            if item_type.startswith("http"):
                item_type = item_type.split("/")[-1]

            if item_type in ["NewsArticle", "Article"]:
                properties = self.extract_microdata_properties(item)

                # Map the properties to the metadata dictionary
                metadata["@type"] = item_type
                metadata["headline"] = properties.get("headline", "")
                metadata["datePublished"] = properties.get("datePublished", "")
                metadata["dateModified"] = properties.get("dateModified", "")
                metadata["image"] = properties.get("image", "")
                metadata["description"] = properties.get("description", "")

                # Handle authors
                author_items = item.find_all(
                    attrs={"itemprop": "author", "itemscope": True}
                )
                authors = []
                for author_item in author_items:
                    author_props = self.extract_microdata_properties(author_item)
                    authors.append(
                        {
                            "name": author_props.get("name", ""),
                            "url": author_props.get("url", ""),
                            "sameAs": author_props.get("sameAs", ""),
                        }
                    )
                if authors:
                    metadata["author"] = authors
        return metadata

    def extract_microdata_properties(self, item):
        """Extract properties from a microdata item."""
        properties = {}
        # Find all direct children with itemprop
        props = item.find_all(attrs={"itemprop": True}, recursive=False)
        for prop in props:
            prop_name = prop.get("itemprop", "")
            # Check if the property is another nested item
            if prop.has_attr("itemscope"):
                prop_value = self.extract_microdata_properties(prop)
            else:
                prop_value = prop.get("content") or prop.get_text(strip=True)
            properties[prop_name] = prop_value
        return properties

    def process_json_ld_bs(self, bs):
        """Process and normalize JSON-LD using BeautifulSoup."""
        metadata = {}
        scripts = bs.find_all("script", type="application/ld+json")
        for script in scripts:
            try:
                json_content = script.string
                data = json.loads(json_content)
                # Handle cases where data is a list
                if isinstance(data, list):
                    for item in data:
                        self._process_json_ld_item(item, metadata)
                else:
                    self._process_json_ld_item(data, metadata)
            except json.JSONDecodeError as e:
                logger.warning(f"JSON decoding failed: {e}")
        return metadata

    def _process_json_ld_item(self, item, metadata):
        """Helper function to process individual JSON-LD items."""
        if item.get("@type") == "NewsArticle" or item.get("@type") == "Article":
            metadata["@type"] = item.get("@type")
            metadata["headline"] = item.get("headline")
            metadata["datePublished"] = item.get("datePublished")
            metadata["dateModified"] = item.get("dateModified")
            metadata["image"] = item.get("image")
            metadata["description"] = item.get("description")

            # Handle multiple authors
            if "author" in item:
                authors = []
                author_data = item.get("author")
                if isinstance(author_data, dict):
                    author_data = [author_data]
                for author in author_data:
                    authors.append(
                        {
                            "name": author.get("name"),
                            "url": author.get("url"),
                            "sameAs": author.get("sameAs"),
                            "honorificPrefix": author.get("honorificPrefix"),
                            "jobTitle": author.get("jobTitle"),
                        }
                    )
                metadata["author"] = authors

    def process_standard_meta(self, bs):
        """Fallback method to extract standard meta tags."""
        metadata = {}

        # Extract title
        title = bs.find("title")
        if title:
            metadata["headline"] = title.get_text(strip=True)

        # Extract description
        description = bs.find("meta", attrs={"name": "description"})
        if description:
            metadata["description"] = description.get("content")

        # Extract author
        author = bs.find("meta", attrs={"name": "author"})
        if author:
            metadata["author"] = [{"name": author.get("content")}]

        # Extract datePublished
        date_published = bs.find("meta", attrs={"name": "datePublished"})
        if date_published:
            metadata["datePublished"] = date_published.get("content")

        # You can add more meta tags as needed

        return metadata

    def write_to_csv(self, url, score, found_keywords, word_mached, metadata):
        """Write the extracted data to a CSV file."""
        try:
            csv_file = self.output_csv  # Using the defined output file

            # Updated fieldnames to match the required headers
            fieldnames = [
                "URL",
                "Score",
                "Keywords Found",
                "type_page",  # Add type_page for page type
                "description",
                "headline",  # Add headline
                "datePublished",  # Add publication date
                "dateModified",  # Add modification date
                "author",
                "byline",  # Add byline for author details/credits
                "excerpt",
                "lang",  # Add language
            ]
            write_header = not os.path.exists(csv_file)

            with open(csv_file, mode="a", newline="", encoding="utf-8") as file:
                writer = csv.DictWriter(file, fieldnames=fieldnames)
                if write_header:
                    writer.writeheader()

                # Format the author field
                authors = metadata.get("author", "")
                if isinstance(authors, list):
                    authors_str = ", ".join(
                        [a.get("name", "") for a in authors if a.get("name")]
                    )
                elif isinstance(authors, str):
                    authors_str = authors
                else:
                    authors_str = ""

                # Create the row with the necessary metadata fields
                row = {
                    "URL": url,
                    "Score": score,
                    "Keywords Found": ", ".join(found_keywords),
                    "type_page": metadata.get("type_page", ""),  # Type of the page
                    "description": metadata.get("description", ""),
                    "headline": metadata.get("headline", ""),
                    "datePublished": metadata.get("datePublished", ""),
                    "dateModified": metadata.get("dateModified", ""),
                    "author": authors_str,
                    "byline": metadata.get("byline", ""),
                    "excerpt": metadata.get("excerpt", ""),
                    "lang": metadata.get("lang", ""),
                }

                writer.writerow(row)
                logger.debug(f"Data written to CSV for {url}")
        except Exception as e:
            logger.error(f"Failed to write to CSV for {url}: {e}")
            traceback.print_exc()

    def extract_links(self, bs, url, score):
        """Extract and queue new links from the page content."""
        try:
            links = bs.find_all("a", href=True)
            for link in links:
                href = link["href"]
                if href.startswith("/"):
                    # Convert relative URLs to absolute
                    href = urljoin(url, href)

                # Ensure that score is an integer
                score = ensure_int(score)
                self.urls_to_visit.add(href, score)
                logger.debug(f"Queued link {href} with score {score}")
        except Exception as e:
            logger.error(f"Failed to extract links from {url}: {e}")

    def extract_A(self, bs, url, score):
        """
        Extract anchor tags (<a>) from the webpage for additional metadata or important information.
        """
        try:
            logger.debug(f"Extracting anchor tags from {url}")
            anchors = bs.find_all("a", href=True)
            for anchor in anchors:
                anchor_url = anchor.get("href")
                anchor_text = anchor.get_text()
                if anchor_text and isinstance(anchor_text, str) and len(anchor_text):
                    final_score, _, _ = kw.relative_keywords_score(anchor_text)
                else:
                    final_score = score
                if not anchor_url:
                    continue  # Skip empty anchor URLs
                if anchor_url.startswith("/"):
                    anchor_url = urljoin(url, anchor_url)

                # Ensure score is an integer and handle potential bytes type for score
                if isinstance(final_score, bytes):
                    final_score = int.from_bytes(
                        final_score, byteorder="big"
                    )  # Convert bytes to integer
                elif not isinstance(final_score, int):
                    final_score = 0  # Default score to 0 if it's not an integer

                # Ensure anchor_url is a string (decode if necessary)
                if isinstance(anchor_url, bytes):
                    anchor_url = anchor_url.decode("utf-8")

                self.urls_to_visit.add(anchor_url, final_score)
                logger.debug(f"Queued link {anchor_url} with score {final_score}")
        except Exception as e:
            logger.error(f"Error extracting anchor tags from {url}: {e}")

    async def _analyze_page_content(self, text, url):
        """Analyze and process webpage content."""

        def process_text():
            excerpt, siteName, lang, byline, tlite, content = (
                None,
                None,
                None,
                None,
                None,
                None,
            )
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
                            byline = article.byline
                            tlite = article.title
                            content = article.content
                except Exception as e:
                    logger.error(f"ProcessArticle failed for {url}: {e}")
                    logger.debug(f"Continuing without ProcessArticle for {url}")

                # Backup metadata extraction if ProcessArticle fails
                if not content:
                    article = simple_json_from_html_string(text)
                    if article:
                        byline = article.get("byline", "No author found")
                        tlite = article.get("title", "No title found")
                        content = (
                            BeautifulSoup(
                                str(article.get("content")), "html.parser"
                            ).get_text(separator=" ")
                            if article.get("content")
                            else None
                        )

                if not content:
                    logger.debug("Attempting fallback metadata extraction.")
                    content = bs.get_text(separator=" ")
                    excerpt_tag = bs.find("meta", {"name": "description"}) or bs.find(
                        "p"
                    )
                    if excerpt_tag:
                        excerpt = excerpt_tag.get_text(strip=True)[:255]

                    # Fallback methods to get title, site name, etc.
                    tlite = bs.title.string if bs.title else "No title found"
                    siteName_tag = bs.find("meta", property="og:site_name")
                    siteName = (
                        siteName_tag["content"] if siteName_tag else "Unknown site"
                    )
                    lang = (
                        bs.find("html").get("lang")
                        if bs.find("html")
                        else "Unknown language"
                    )
                    byline_tag = bs.find("meta", attrs={"name": "author"})
                    byline = byline_tag["content"] if byline_tag else "No author found"

                # Remove unwanted elements
                unwanted_tags = [
                    "script",
                    "style",
                    "footer",
                    "header",
                    "aside",
                    "nav",
                    "noscript",
                    "form",
                    "iframe",
                    "button",
                ]
                unwanted_classes_ids = [
                    "ads",
                    "sponsored",
                    "social",
                    "share",
                    "comment",
                    "comments",
                    "related",
                    "subscription",
                    "sidebar",
                ]

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

                # Use heuristic to find the most meaningful content
                article_content = bs.find("article")
                if not article_content:
                    article_content = max(
                        bs.find_all("div"), key=lambda x: len(x.get_text()), default=bs
                    )

                content = article_content.get_text(separator=" ")
                # Further clean the content
                content = content.encode("ascii", "ignore").decode()
                content = re.sub(r"\s+", " ", content).strip()
                word_mached = []
                score, found_keywords, _ = kw.relative_keywords_score(content)
                self.extract_A(bs, url, score)
                self.extract_links(bs, url, score)
                self.urls_to_visit.set_page_score_page(url, score)

                if score > 0:
                    metadata = self.extract_metadata(text, url)

                    # Add metadata fields to the extracted data
                    metadata.update(
                        {
                            "excerpt": excerpt,
                            "siteName": siteName,
                            "lang": lang,
                            "byline": byline,
                            "tlite": tlite,
                            "type_page": "webpage",
                        }
                    )

                    self.write_to_csv(url, score, found_keywords, word_mached, metadata)

            except Exception as e:
                logger.error(f"Error parsing content from {url}: {e}")
                logger.debug(f"Exception details: {traceback.format_exc()}")
                exc_type, exc_obj, exc_tb = sys.exc_info()
                fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                logger.error(
                    f"Exception type: {exc_type}, File: {fname}, Line: {exc_tb.tb_lineno}"
                )

            self.urls_to_visit.mark_seen(url)

        process_text()

    def update_csv(self, url, score, found_keywords, word_mached, metadata):
        """Update the CSV file for revisited URLs."""
        if score <= 0:
            self.remove_from_csv(url)
        else:
            self.write_to_csv(url, score, found_keywords, word_mached, metadata)

    async def _process_url(self, text, url):
        """Fetch and process a webpage."""
        if text is not None:
            text = self._apply_after_fetch_plugins(url, text)
            await self._analyze_page_content(text, url)
        if text is None or not text.strip():
            logger.error(f"Empty or None response for URL: {url}")
            return

    async def route_based_on_mime(self, url, mime, text, url_item):
        """Route processing based on the MIME type."""
        try:
            if "feed.rss" in url_item.url:
                return await self._process_feed(text, url)
            if "feed.atom" in url_item.url:
                return await self._process_feed(text, url)
            elif url_item.page_type == "feed":
                return await self._process_feed(text, url)
            elif url_item.page_type == "sitemap":
                return await self._process_sitemap(text, url)
            mime_type_mapping = {
                "text/html": self._process_url,
                "application/rss+xml": self._process_feed,
                "application/xml": self._process_sitemap,
            }
            if mime:
                for mime_prefix, handler in mime_type_mapping.items():
                    if mime.startswith(mime_prefix):
                        return await handler(text, url)
            if url_item.page_type == "webpage":
                return await self._process_url(text, url)
            logger.warning(f"Unsupported MIME type: {mime} for URL: {url}")
            return None
        except Exception as e:
            logger.error(f"Error in routing based on MIME: {e}")
            traceback.print_exc()

    async def crawl_main(self):
        batch_size = 10
        conn = aiohttp.TCPConnector(limit_per_host=10, limit=100)
        async with aiohttp.ClientSession(connector=conn) as session:
            max_count = self.urls_to_visit.count_all()

            # Wrap tqdm around the total count
            with tqdm.tqdm(
                total=max_count, desc="Crawling Progress", unit="url"
            ) as pbar:
                total_done = self.urls_to_visit.count_seen()
                pbar.n = total_done
                while not self.urls_to_visit.empty():
                    tasks = []
                    for _ in range(batch_size):
                        if self.urls_to_visit.empty():
                            break
                        url_item = self.urls_to_visit.pop()
                        if not url_item:
                            break

                        # Check if the URL contains one of the allowed subdirectories
                        if not any(
                            subdir in url_item.url for subdir in self.allowed_subdirs
                        ):
                            continue  # Skip this URL if it doesn't match the allowed subdirectories
                        self.urls_to_visit.mark_processing(url_item.url)
                        if url_item is None or not self.is_valid_url(url_item.url):
                            logger.error(
                                f"Invalid or None URL fetched from the queue: {url_item}"
                            )
                            continue

                        async def task(url_item, session):
                            try:
                                text, mime, status = await self._fetch(
                                    session, url_item
                                )
                                if text:
                                    # Pass the correct `url_item` object to the routing function
                                    await self.route_based_on_mime(
                                        url_item.url, mime, text, url_item
                                    )
                                else:
                                    self.urls_to_visit.push(url_item)
                            except Exception as e:
                                logger.error(
                                    f"Error processing URL: {url_item.url} {e}"
                                )
                                self.urls_to_visit.push(url_item)
                            pbar.update(1)  # Update progress bar on task completion

                        tasks.append(asyncio.create_task(task(url_item, session)))

                    max_count = self.urls_to_visit.count_all()
                    pbar.total = max_count  # Update total count if it changes
                    if tasks:
                        await asyncio.gather(*tasks)

        for plugin in self.plugins:
            plugin.on_finish(self)

    async def crawl_start(self):
        logger.info("Starting crawl_main")
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

    allowed_subdirs = ["www.bbc.co.uk/"]
    keywords = kw.KEYWORDS

    crawler = KeyPhraseFocusCrawler(
        conn,
        start_urls,
        feeds,
        allowed_subdirs=allowed_subdirs,
        keywords=keywords,
        irrelevant_for_keywords=kw.irrelevant_for_keywords,
        anti_keywords=kw.ANTI_KEYWORDS,
    )

    logger.info("Starting crawler...")
    await crawler.crawl_start()


if __name__ == "__main__":
    asyncio.run(main())
