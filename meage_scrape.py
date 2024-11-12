import asyncio
from asyncio import subprocess
from collections import defaultdict
import math
import multiprocessing
import os
import logging
import csv
import json
import random
import socket
import ssl
import time
from urllib.parse import urljoin, urlparse, urlunparse, quote, unquote
import resource
import aiohttp
import aiosqlite
import certifi
from bs4 import BeautifulSoup
import feedparser
import chardet
from concurrent.futures import ThreadPoolExecutor
import numpy as np

# Local imports
from keyword_search import lookup_keyword
from utils.RL import ConfigManager
from utils.bbc_scripe_cdx import get_all_urls_cdx
import utils.keywords as kw
from utils.RobotsSql import RobotsSql
from utils.SQLDictClass import SQLDictClass
from utils.BackedURLQueue import BackedURLQueue, URLItem
import utils.keywords_finder as kw_finder
from utils.ovarit import ovarit_domain_scrape
from utils.reddit import reddit_domain_scrape

# Initialize logging
logging.basicConfig(
    filename='meage_scrape.log',
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,"
    "image/png,image/svg+xml,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
    "Upgrade-Insecure-Requests": "1",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
    "Sec-Fetch-User": "?1",
    "Sec-GPC": "1",
}


async def check_internet(host="8.8.8.8", port=53, timeout=3):
    """
    Asynchronously checks if the internet is accessible by attempting to connect to a DNS server.
    
    Args:
        host (str): The remote host to connect to (default is 8.8.8.8 - Google DNS).
        port (int): The port to connect on (default is 53 - DNS port).
        timeout (int): Timeout for the connection attempt in seconds.

    Returns:
        bool: True if the connection was successful, indicating internet is up; False otherwise.
    """
    try:
        # Create an asynchronous socket
        reader, writer = await asyncio.wait_for(
            asyncio.open_connection(host, port), timeout
        )
        writer.close()
        await writer.wait_closed()
        return True
    except (asyncio.TimeoutError, socket.gaierror, ConnectionRefusedError, OSError):
        return False


def normalize_url_format(url: str) -> str:
    """Standardize and clean URL format by enforcing https://, removing www prefix, 
    handling web.archive.org links, and ensuring consistent formatting of the path."""
    # Check and handle web.archive.org URLs
    if "web.archive.org" in url:
        live_url_start = url.find("/https://")
        if live_url_start != -1:
            url = url[live_url_start + 1:]

    parsed = urlparse(url)
    scheme = "https"
    
    # Normalize the hostname by removing "www" and ensuring lowercase
    hostname = parsed.hostname.lower().replace("www.", "") if parsed.hostname else ""
    
    # Handle paths, preserving any file extensions dynamically
    path = parsed.path
    if not path or not path.split('/')[-1].count('.'):
        path = quote(unquote(path.rstrip("/"))) + "/"
    else:
        path = quote(unquote(path))

    # Manage port normalization based on scheme
    port = (
        None
        if (scheme == "http" and parsed.port == 80) or (scheme == "https" and parsed.port == 443)
        else parsed.port
    )
    netloc = f"{hostname}:{port}" if port else hostname

    return urlunparse((scheme, netloc, path, "", parsed.query, ""))


class Crawler:
    def __init__(
            self,
            start_urls,
            feeds,
            name,
            keywords=kw.KEYWORDS,
            anti_keywords=kw.ANTI_KEYWORDS,
            plugins=[],
            allowed_subdirs=None,
            start_date=None,
            end_date=None,
            exclude_subdirs=None,
            exclude_scrape_subdirs=None,
            exclude_lag=[],
            piloting=True,
                show_bar=True
    ):  
        self    .config_manager = ConfigManager(f"data/config_{name}.json")
        self    .start_urls = start_urls
        self.feeds = feeds
        self.name = name
        self.keywords = keywords
        self.anti_keywords = anti_keywords
        self.key_finder = kw_finder.KeypaceFinder(keywords)
        self.plugins = plugins
        self.output_csv = f"data/{name}.csv"
        self.file_db = f"databases/{name}.db"
        self.cpu_cores = multiprocessing.cpu_count()
    
        # Initialize parameters using ConfigManager
        self.max_limit = self.config_manager.initialize_parameter("max_limit", int, 100, 1, 1500)
        self.max_workers = self.config_manager.initialize_parameter("max_workers", int, self.cpu_cores * 2, 1, self.cpu_cores * 4)
        self.batch_size = self.config_manager.initialize_parameter("batch_size", int, 500, 1, 3000)
        self.batch_size2 = self.config_manager.initialize_parameter("batch_size2", int, 500, 1, 3000)
        self.batch_size3 = self.config_manager.initialize_parameter("batch_size3", int, 500, 1, 3000)
        self.batch_size4 = self.config_manager.initialize_parameter("batch_size4", int, 500, 1, 3000)
        self.concurrent_task_limit = self.config_manager.initialize_parameter("concurrent_task_limit", int, 100, 1, 500)
        self.collect_csv_row_limit = self.config_manager.initialize_parameter("collect_csv_row", int, self.cpu_cores, 1, self.cpu_cores * 2)
        if piloting:
            self.batch_size_data = []
            self.batch_size2_data = []
            self.batch_size3_data = []
            self.batch_size4_data = []
    
        # Initialize semaphores based on batch sizes
        self.semaphore = asyncio.Semaphore(self.batch_size)
        self.semaphore2 = asyncio.Semaphore(self.batch_size2)
        self.semaphore3 = asyncio.Semaphore(self.batch_size3)
        self.semaphore4 = asyncio.Semaphore(self.batch_size4)
    
        # Initialize thread pool with the configured max workers
        self.thread_pool = ThreadPoolExecutor(max_workers=self.max_workers)
    
        # Database and URL queue initialization
        self.conn = None
        self.robots = None
        self.urls_to_visit = None
    
        # Directory and subdirectory settings
        self.allowed_subdirs = allowed_subdirs or []
        self.start_date = start_date
        self.end_date = end_date
        self.exclude_scrape_subdirs = exclude_scrape_subdirs or []
        self.exclude_subdirs = exclude_subdirs or []
    
        # CSV row collection and locking mechanism
        self.csv_rows = []
        self.lock = asyncio.Lock()
        self.has_internet_error = False
        self.piloting = piloting
        self.show_bar = show_bar
    
        # Initialize piloting metrics if enabled
        if piloting:
            self.batch_numbers = []
            self.rewards = []
            self.batch_size_data = []
            self.max_workers_data = []
            self.concurrent_limit_data = []
            self.time_taken_data = []
            self.batch_size_data1 = []
            self.batch_size_data2 = []
            self.batch_size_data3 = []
            self.batch_size_data4 = []
            self.total_discovered_links = 0
    
        # Attributes for estimation
        self.total_discovered_links = 0
        self.average_links_per_page = 10  # Starting estimate
        self.filtering_factor = 0.8  # Assumes we skip/filter 20% of pages
        self.estimated_total_pages = 1000  # Initial rough estimate
        self.growth_rate = 0.1  # Growth rate for logistic model
        self.midpoint_batch = 5  # Midpoint for logistic growth
        self.discovery_rate = []  # Store discovery rate (links found per batch)
        self.exponential_phase = True  # Start in exponential growth phase

        

    def update_estimated_total_pages(self, batch_number):
        """
        Update the estimated total number of pages using a combined exponential and logistic growth model.
        """
        current_links = self.total_discovered_links

        if self.exponential_phase:
            # Exponential growth model
            estimated_total = current_links * math.exp(self.growth_rate * batch_number)

            # Check if we need to switch to logistic growth phase
            if len(self.discovery_rate) >= 5:
                recent_rate = sum(self.discovery_rate[-5:]) / 5
                overall_rate = sum(self.discovery_rate) / len(self.discovery_rate)
                if recent_rate < 0.8 * overall_rate:
                    self.exponential_phase = False
                    logger.info("Switching to logistic growth phase for estimation.")
        else:
            # Logistic growth model
            L = self.estimated_total_pages
            k = self.growth_rate
            t = batch_number
            t0 = self.midpoint_batch
            estimated_total = L / (1 + math.exp(-k * (t - t0)))

        # Update the estimate
        self.estimated_total_pages = int(round(max(self.estimated_total_pages, estimated_total)))

    def update_discovery_rate(self, extracted_links_count):
        self.discovery_rate.append(extracted_links_count)
        if len(self.discovery_rate) > 10:
            self.discovery_rate.pop(0)  # Limit the size of the history

    async def process_article(self, html, url):
        try:
            async with self.semaphore3:
                # Run the JavaScript code asynchronously with Node.js
                process = await asyncio.create_subprocess_exec(
                    "node", "utils/ProcessArticle.js", url,
                    stdin=asyncio.subprocess.PIPE,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE
                )

                # Send the HTML content via stdin and close stdin
                stdout, stderr = await process.communicate(input=html.encode('utf-8'))

                if process.returncode == 0:
                    article = json.loads(stdout.decode('utf-8'))  # Parse the output as JSON
                    return article
                else:
                    print(f"Error: {stderr.decode('utf-8')}")
                    return None
        except Exception as e:
            print(f"Exception occurred: {e}")
            return None

    async def initialize_db(self):
        """Set up the database and required tables."""
        self.robots = RobotsSql(self.conn, self.thread_pool)
        self.urls_to_visit = BackedURLQueue(self.conn, table_name="urls_to_visit")
        await self.urls_to_visit.initialize()
        await self.robots.initialize()

    def initialize_output_csv(self):
        os.makedirs(os.path.dirname(self.output_csv), exist_ok=True)
        fieldnames = [
            "Root Domain",
            "URL",
            "Score",
            "Keywords Found",
            "Headline",
            "Name",
            "Date Published",
            "Date Modified",
            "Author",
            "Byline",
            "Type",
            "Crawled At",
        ]
        if not os.path.exists(self.output_csv) or os.path.getsize(self.output_csv) ==  0:
            with open(self.output_csv, mode="w", newline="", encoding="utf-8") as csv_file:
                csv_writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
                csv_writer.writeheader()

    async def initialize_urls(self):
        if await self.urls_to_visit.count_seen() > 0:
            logger.info("URLs have already been initialized.")
            return

        for url in self.start_urls:
            await self.add_to_queue(url, "webpage", priority=0)
        for feed in self.feeds:
            await self.add_to_queue(feed, "feed", priority=0)

    def _scrape_related_domains(self, url):
        o = urlparse(url)
        scraped_urls = (
            list(reddit_domain_scrape(o.hostname))
            + list(ovarit_domain_scrape(o.hostname))
            + list(get_all_urls_cdx(o.hostname))
        )
        return [
            (link[0], self.key_finder.relative_keywords_score(link[1])[0])
            for link in scraped_urls
        ]

    async def pre_seed(self):
        for url in self.start_urls:
            for keyword in self.keywords:
                await lookup_keyword(keyword, url)

    async def add_to_queue(self, url, url_type, priority=0):
        try:
            normalized_url = normalize_url_format(url)

            if not self.is_valid_url(normalized_url):
                return
            if await self.urls_to_visit.have_been_seen(normalized_url):
                return

            if not any(normalized_url.startswith(subdir) for subdir in self.allowed_subdirs):
                return
            if any(normalized_url.startswith(subdir) for subdir in self.exclude_subdirs):
                return

            robot = await self.robots.get(normalized_url)
            if robot and not robot.can_fetch("*", normalized_url):
                return

            url_item = URLItem(
                url=normalized_url,
                url_score=priority,
                page_type=url_type,
                status="unseen"
            )
            await self.urls_to_visit.push(url_item)

            # Update the estimate dynamically
            self.total_discovered_links += 1
            self.estimated_total_pages = self.total_discovered_links * self.average_links_per_page * self.filtering_factor

        except Exception as e:
            logger.error(f"Failed to add URL to queue: {url} - Error: {e}")

    def is_valid_url(self, url):
        parsed = urlparse(url)
        return all([parsed.scheme, parsed.netloc])

    async def fetch_content(self, session, url_item):
        url = url_item.url
        ssl_context = ssl.create_default_context(cafile=certifi.where())
        retries = 5
        attempt = 0
        while attempt < retries:
            try:
                async with session.get(url, ssl=ssl_context, timeout=60) as response:
                    if response.status == 200:
                        content_bytes = await response.read()
                        encoding = chardet.detect(content_bytes)["encoding"] or "utf-8"
                        text = content_bytes.decode(encoding, errors="replace")
                        if text.strip():
                            await self.urls_to_visit.update_status(url, "seen")
                            return text, response.headers.get("Content-Type", "")
                    intent_check = await check_internet()
                    if intent_check:
                        attempt += 1
                        await asyncio.sleep(2 ** attempt)  # Exponential backoff
                    else:
                        while not await check_internet():
                            logger.error("Internet connection lost. Retrying...")
                            await asyncio.sleep(5)
            except Exception as e:
                logger.error(f"Error fetching content for {url}: {e}")
                attempt += 1
        await self.urls_to_visit.update_status(url, "error")
        return None, None

    async def process_content(self, url_item, text, mime):
        metadata = {}

        if "application/rss+xml" in mime or url_item.page_type == "feed":
            await self.process_feed(text, url_item.url)
        elif "application/xml" in mime or url_item.page_type == "sitemap":
            await self.process_sitemap(text, url_item.url)
        elif "text/html" in mime:
            await self.analyze_webpage_content(text, url_item, metadata)
        else:
            await self.urls_to_visit.update_error(
                url_item.url, f"Unsupported MIME type: {mime}"
            )
            logger.warning(f"Unsupported MIME type for {url_item.url}: {mime}")

    def update_discovery_metrics(self, links_to_queue):
        """
        Update the total discovered links and the average number of links per page.
        """
        extracted_links_count = len(links_to_queue)
        self.total_discovered_links += extracted_links_count
    
        # Update the average number of links per page
        if self.total_discovered_links > 0:
            self.average_links_per_page = (
                (self.average_links_per_page + extracted_links_count) / 2
            )

    async def analyze_webpage_content(self, text, url_item, metadata):
        loop = asyncio.get_event_loop()
        try:
            if any(url_item.url.startswith(subdir) for subdir in self.exclude_scrape_subdirs):
                await self.urls_to_visit.mark_seen(url_item.url)
                return
            
            article = await self.process_article(text, url_item.url)
            async with self.semaphore4:
                result = await loop.run_in_executor(
                    self.thread_pool,
                    self._analyze_webpage_content_sync,
                    text,
                    url_item,
                    metadata,
                    article
                )

            if result is None:
                await self.urls_to_visit.mark_seen(url_item.url)
                return

            score, keywords, metadata, links_to_queue, canonical_url = result

            # Update link discovery metrics
            self.update_discovery_metrics(links_to_queue)

            # Update the estimated total number of pages
            self.update_estimated_total_pages(batch_number=len(self.batch_numbers))

            # Add discovered links to the queue
            for link_info in links_to_queue:
                await self.add_to_queue(*link_info)

            # Check for canonical URL and add to queue if different
            if canonical_url and url_item.url != canonical_url:
                await self.add_to_queue(canonical_url, "webpage", priority=score)
            await self.urls_to_visit.mark_seen(url_item.url)
                
            # Update page score and collect CSV row if the score is positive
            if score > 0:
                await self.urls_to_visit.set_page_score(url_item.url, score)
                self.collect_csv_row(url_item, score, keywords, metadata)

        except Exception as e:
            logger.error(f"Error in analyze_webpage_content for {url_item.url}: {e}")
            await self.urls_to_visit.update_error(url_item.url, str(e))

    def _analyze_webpage_content_sync(self, text, url_item, metadata, article):
        try:
            bs = BeautifulSoup(text, "html.parser")

            # Extract metadata
            metadata = self.extract_metadata(bs, text, url_item.url, metadata)
            if article is not None:
                content_to_score = article.get('content', text)
                lang = article.get('lang', 'unknown')
            else:
                content_to_score = bs.find("body").get_text(separator=" ")
                try:
                    lang = bs.html.get("lang", "unknown")
                except:
                    lang = "unknown"
            # Compute score and find keywords
            bs_content = BeautifulSoup(content_to_score, "html.parser")
            text_content = bs_content.get_text(separator=" ")
            score, keywords, _ = self.key_finder.relative_keywords_score(text_content)

            # Extract and prepare links to queue
            links = {a["href"] for a in bs.find_all("a", href=True)}

            links_to_queue = [
                (urljoin(url_item.url, link), "webpage", score) for link in links
            ]

            # Check for canonical URL
            canonical_url = None
            if lang not in {"en_GB", "en_US"}:
                for link in bs.find_all("link", rel=lambda x: x and "canonical" in x):
                    canonical_href = link.get("href")
                    if canonical_href:
                        canonical_url = normalize_url_format(canonical_href)
                        break

            return score, keywords, metadata, links_to_queue, canonical_url
        except Exception as e:
            logger.error(f"Error in _analyze_webpage_content_sync: {e}")
            return None

    def _extract_canonical_links_sync(self, bs):
        try:
            canonical_links = bs.find_all("link", rel=lambda x: x and "canonical" in x)
            return canonical_links
        except Exception as e:
            logger.error(f"Error extracting canonical links: {e}")
            return []
        
    async def process_feed(self, text, url):
        feed = feedparser.parse(text)
        for entry in feed.entries:
            score, keywords, _ = self.key_finder.relative_keywords_score(
                entry.title
            )
            await self.add_to_queue(entry.link, "webpage", priority=score)

    async def process_sitemap(self, text, url):
        bs = BeautifulSoup(text, "html.parser")
        urls = [loc.text for loc in bs.find_all("loc")]
        for link in urls:
            await self.add_to_queue(link, "webpage")

    def extract_metadata(self, bs, text, url, metadata={}):
        try:
            json_ld = self.extract_json_ld(bs)
            if json_ld and isinstance(json_ld, dict):  # Ensure json_ld is a dictionary
                metadata.update({
                    **json_ld,
                    "headline": json_ld.get("headline", ""),
                    "datePublished": json_ld.get("datePublished", ""),
                    "dateModified": json_ld.get("dateModified", ""),
                    "author": ", ".join(author["name"] for author in json_ld.get("author", []) if isinstance(author, dict) and "name" in author),
                    "keywords": ", ".join(json_ld["keywords"]) if isinstance(json_ld.get("keywords", ""), list) else json_ld.get("keywords", ""),
                    "publisher": json_ld.get("publisher", {}).get("name", "") if isinstance(json_ld.get("publisher", {}), dict) else "",
                    "url": json_ld.get("url", ""),
                    "type": ", ".join(json_ld["@type"]) if isinstance(json_ld.get("@type", ""), list) else json_ld.get("@type", "WebPage"),
                })
            else:
                microdata = self.extract_microdata(bs)
                if microdata and isinstance(microdata, dict):  # Ensure microdata is a dictionary
                    metadata.update(microdata)
        except Exception as e:
            logger.error(f"Failed to extract metadata for {url}: {e}")
        return metadata

    def _merge_jsonld_data(self, target, source):
        """Helper function to merge JSON-LD data into the target dictionary."""
        for key, value in source.items():
            if key == "@type" and isinstance(value, str):
                value = [value]

            if isinstance(target.get(key), list) and isinstance(value, list):
                target[key].extend(value)
            elif isinstance(target.get(key), list):
                target[key].append(value)
            elif isinstance(value, list):
                target[key] = [target.get(key, ""), *value]
            elif key in target:
                target[key] = [target[key], value]
            else:
                target[key] = value

    def extract_json_ld(self, bs):
        all_data = defaultdict(list)

        try:
            scripts = bs.find_all("script", type="application/ld+json")
            for script in scripts:
                try:
                    data = json.loads(script.string)

                    # Ensure data is a dictionary, if it's a list, process each item individually
                    if isinstance(data, list):
                        for item in data:
                            if isinstance(item, dict):
                                self._merge_jsonld_data(all_data, item)
                    elif isinstance(data, dict):
                        self._merge_jsonld_data(all_data, data)
                    else:
                        logger.warning(f"Unexpected JSON-LD structure: {type(data)}")
                        continue

                except json.JSONDecodeError:
                    logger.warning("JSON-LD extraction failed due to JSON decoding error.")
                except (TypeError, KeyError) as e:
                    logger.warning(f"Error while processing JSON-LD data: {e}")
        except Exception as e:
            logger.error(f"Error extracting JSON-LD: {e}")

        return dict(all_data)

    def extract_microdata(self, bs):
        microdata_items = {}

        for tag in bs.find_all(True):
            if tag.has_attr("itemprop"):
                prop_name = tag["itemprop"]
                prop_value = tag["content"] if tag.has_attr("content") else tag.get_text(strip=True)
                microdata_items[prop_name] = prop_value

        return microdata_items

    def collect_csv_row(self, url_item, score, keywords, metadata):
        root_url = urlparse(url_item.url).netloc
        crawled_at = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        row = {
            "Root Domain": root_url,
            "URL": url_item.url,
            "Score": score,
            "Keywords Found": ", ".join(keywords),
            "Headline": metadata.get("headline"),
            "Name": metadata.get("name"),
            "Date Published": metadata.get("datePublished", ""),
            "Date Modified": metadata.get("dateModified", ""),
            "Author": metadata.get("author", ""),
            "Byline": metadata.get("byline", ""),
            "Type": metadata.get("@type", ""),
            "Crawled At": crawled_at,
        }
        self.csv_rows.append(row)
        if len(self.csv_rows) >= self.collect_csv_row_limit:
            self.write_to_csv()

    def write_to_csv(self):
        if not self.csv_rows:
            return
        with open(self.output_csv, mode="a", newline="", encoding="utf-8") as csv_file:
            writer = csv.DictWriter(csv_file, fieldnames=self.csv_rows[0].keys())
            writer.writerows(self.csv_rows)
        self.csv_rows.clear()

    async def crawl_main(self):
        conn = aiohttp.TCPConnector(limit=100, ssl=False, keepalive_timeout=60)
        async with aiohttp.ClientSession(
            connector=conn, headers=HEADERS
        ) as session:
            await self._crawl_loop(session)

    async def _url_generator(self):
        while not await self.urls_to_visit.empty():
            url_item = await self.urls_to_visit.pop()
            if url_item:
                await self.urls_to_visit.mark_processing(url_item.url)
                yield url_item

    def adjust_parameters(self):
        """
        Adjust task parameters based on updated Q-values from the ConfigManager.
        """
        try:
            # Retrieve updated values from ConfigManager
            self.max_limit = self.config_manager.get_value("max_limit", default=100)
            self.max_workers = self.config_manager.get_value("max_workers", default=self.cpu_cores * 2)
            self.batch_size = self.config_manager.get_value("batch_size", default=500)
            self.batch_size2 = self.config_manager.get_value("batch_size2", default=500)
            self.batch_size3 = self.config_manager.get_value("batch_size3", default=500)
            self.batch_size4 = self.config_manager.get_value("batch_size4", default=500)
            self.concurrent_task_limit = self.config_manager.get_value("concurrent_task_limit", default=100)
            self.collect_csv_row_limit = self.config_manager.get_value("collect_csv_row", default=self.cpu_cores)

            # Correct semaphore initialization without directly modifying _value
            self.semaphore = asyncio.Semaphore(self.batch_size)
            self.semaphore2 = asyncio.Semaphore(self.batch_size2)
            self.semaphore3 = asyncio.Semaphore(self.batch_size3)
            self.semaphore4 = asyncio.Semaphore(self.batch_size4)

            # Update the thread pool with the new max workers
            self.thread_pool._max_workers = self.max_workers

            # Log the updated parameters
            logger.info(
                f"Adjusted parameters: batch_size={self.batch_size}, "
                f"max_workers={self.max_workers}, concurrent_task_limit={self.concurrent_task_limit}, "
                f"batch_size2={self.batch_size2}, batch_size3={self.batch_size3}, batch_size4={self.batch_size4}"
            )

        except Exception as e:
            logger.error(f"Error adjusting parameters: {e}")

    async def initialize_plot(self):
        """Initialize the live plot for tracking progress and metrics."""
        if not self.piloting:
            return  # Skip plotting if piloting is False
        from matplotlib import pyplot as plt
        plt.ion()  # Enable interactive mode
        fig, ax = plt.subplots()
        ax.set_title("Task Parameters and Time Taken vs. Batch Number")
        ax.set_xlabel("Batch Number")
        ax.set_ylabel("Metrics")

        # Initialize lines for different metrics
        self.batch_size_line, = ax.plot([], [], label="Batch Size", color="blue")
        self.batch_size2_line, = ax.plot([], [], label="Batch Size 2", color="cyan")
        self.batch_size3_line, = ax.plot([], [], label="Batch Size 3", color="magenta")
        self.batch_size4_line, = ax.plot([], [], label="Batch Size 4", color="orange")
        self.max_workers_line, = ax.plot([], [], label="Max Workers", color="green")
        self.concurrent_limit_line, = ax.plot([], [], label="Concurrent Task Limit", color="brown")
        self.rewards_line, = ax.plot([], [], label="Rewards", color="pink")
        self.time_line, = ax.plot([], [], label="Time Taken (seconds)", color="red")
        ax.legend()
        self.fig, self.ax = fig, ax

    async def log_parameters(self, batch_number, batch_size, max_workers, concurrent_task_limit, batch_size2, batch_size3, batch_size4, reward):
        """Log the current parameters for this batch."""
        logger.info(
            f"Batch {batch_number}: batch_size={batch_size}, max_workers={max_workers}, "
            f"concurrent_task_limit={concurrent_task_limit}, batch_size2={batch_size2}, "
            f"batch_size3={batch_size3}, batch_size4={batch_size4}, reward={reward}"
        )

    def update_plot_data(self, batch_number, reward, batch_size, max_workers, concurrent_task_limit, batch_size2, batch_size3, batch_size4, time_taken):
        """Update the live plot data with all relevant parameters."""
        if not self.piloting:
            return  # Skip updating plot if piloting is False
        self.batch_numbers.append(batch_number)
        self.rewards.append(reward)
        self.batch_size_data.append(batch_size)
        self.batch_size2_data.append(batch_size2)
        self.batch_size3_data.append(batch_size3)
        self.batch_size4_data.append(batch_size4)
        self.max_workers_data.append(max_workers)
        self.concurrent_limit_data.append(concurrent_task_limit)
        self.time_taken_data.append(time_taken)

        # Update lines with new data
        self.batch_size_line.set_data(self.batch_numbers, self.batch_size_data)
        self.batch_size2_line.set_data(self.batch_numbers, self.batch_size2_data)
        self.batch_size3_line.set_data(self.batch_numbers, self.batch_size3_data)
        self.batch_size4_line.set_data(self.batch_numbers, self.batch_size4_data)
        self.max_workers_line.set_data(self.batch_numbers, self.max_workers_data)
        self.concurrent_limit_line.set_data(self.batch_numbers, self.concurrent_limit_data)
        self.rewards_line.set_data(self.batch_numbers, self.rewards)
        self.time_line.set_data(self.batch_numbers, self.time_taken_data)

        # Refresh plot
        self.ax.relim()
        self.ax.autoscale_view()
        self.fig.canvas.draw()
        self.fig.canvas.flush_events()

    async def gather_tasks(self, session, tasks, concurrent_task_limit, pbar):
        """Collect tasks up to the concurrency limit."""
        async for url_item in self._url_generator():
            await self.urls_to_visit.mark_processing(url_item.url)
            task = asyncio.create_task(self.fetch_and_process_url(url_item, session, pbar))
            tasks.add(task)
            if len(tasks) >= concurrent_task_limit:
                break
        return tasks

    async def update_q_learning(self, completed_tasks, time_taken, memory_in_bytes):
        """
        Calculate the reward and update Q-learning for multiple parameters.
        """
        try:
            # Convert memory usage to MB
            memory_in_mb = memory_in_bytes / (1024 ** 2)

            # Calculate reward based on tasks, time, and memory usage
            reward = (completed_tasks / max(time_taken, 1e-6)) * np.exp(-memory_in_mb / 100)

            # Update Q-learning for all relevant parameters
            parameter_names = ["batch_size", "max_workers", "concurrent_task_limit", "batch_size2", "batch_size3", "batch_size4"]
            self.config_manager.step(reward, parameter_names)

            # Save the updated configuration
            self.config_manager.save_config()

            # Adjust parameters based on updated Q-values
            self.adjust_parameters()

            return reward
        except Exception as e:
            logger.error(f"Error updating Q-learning: {e}")
            return 0

    async def fetch_and_process(self, session, tasks, pbar):
        """Fetch and process tasks, handling completed ones."""
        if tasks:
            done, _ = await asyncio.wait(tasks)
            completed_tasks = len(done)
            tasks.difference_update(done)
            if self.show_bar and pbar is not None:
                pbar.update(completed_tasks)
        else:
            completed_tasks = 0
        return completed_tasks

    async def _crawl_loop(self, session):
        """Main loop for crawling with adaptive Q-learning and live plotting."""
        if await self.urls_to_visit.empty():
            logger.info("No URLs left to visit. Exiting crawl loop.")
            return

        await self.initialize_plot()

        start_time = time.time()
        tasks = set()
        batch_number = 0
        memory_in_bytes = 0

        if self.show_bar:
            import tqdm
            pbar = tqdm.tqdm(
                total=max(await self.urls_to_visit.count_all(), self.estimated_total_pages),
                desc="Crawling Progress",
                unit="url"
            )
        else:
            pbar = None

        while not await self.urls_to_visit.empty() or tasks:
            # Retrieve adaptive parameter values
            batch_size = self.config_manager.get_value("batch_size", default=500, value_type=int)
            max_workers = self.config_manager.get_value("max_workers", default=self.cpu_cores * 2, value_type=int)
            concurrent_task_limit = self.config_manager.get_value("concurrent_task_limit", default=100, value_type=int)
            batch_size2 = self.config_manager.get_value("batch_size2", default=500, value_type=int)
            batch_size3 = self.config_manager.get_value("batch_size3", default=500, value_type=int)
            batch_size4 = self.config_manager.get_value("batch_size4", default=500, value_type=int)
            memory_in_bytes = max(memory_in_bytes, resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)

            tasks = await self.gather_tasks(session, tasks, concurrent_task_limit, pbar)
            completed_tasks = await self.fetch_and_process(session, tasks, pbar)

            if completed_tasks > 0:
                if self.show_bar and pbar is not None:
                    pbar.total = max(await self.urls_to_visit.count_all(), self.estimated_total_pages)
                time_taken = time.time() - start_time
                reward = await self.update_q_learning(completed_tasks, time_taken, memory_in_bytes)
                await self.log_parameters(batch_number, batch_size, max_workers, concurrent_task_limit, batch_size2, batch_size3, batch_size4, reward)
                self.adjust_parameters()
                self.config_manager.save_config()

                self.update_plot_data(batch_number, reward, batch_size, max_workers, concurrent_task_limit, batch_size2, batch_size3, batch_size4, time_taken)

                # Reset start time for the next batch
                start_time = time.time()
                batch_number += 1
                memory_in_bytes = 0

        if self.piloting:
            from matplotlib import pyplot as plt
            plt.ioff()  # Turn off interactive mode
            plt.show()

    async def fetch_and_process_url(self, url_item, session, pbar):
        if not url_item:
            return

        try:
            async with self.semaphore:
                text, mime = await self.fetch_content(session, url_item)
            async with self.semaphore2:
                if text:
                    await self.process_content(url_item, text, mime)
                else:
                    logger.error(f"Empty content for {url_item.url}")

        except Exception as e:
            logger.error(f"Error processing {url_item.url}: {e}")

        finally:
            if self.show_bar and pbar is not None:
                pbar.update(1)
            
    async def crawl_start(self):
        os.makedirs(os.path.dirname(self.file_db), exist_ok=True)
        async with aiosqlite.connect(self.file_db) as self.conn:
            self.conn.row_factory = aiosqlite.Row
            await self.initialize_db()
            self.initialize_output_csv()
            await self.initialize_urls()
            await self.crawl_main()
