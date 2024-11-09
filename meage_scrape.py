import asyncio
from asyncio import subprocess
from collections import defaultdict
import multiprocessing
import os
import logging
import csv
import json
import random
import socket
import ssl
import tempfile
import time
from urllib.parse import urljoin, urlparse, urlunparse, quote, unquote

import aiohttp
import aiosqlite
import certifi
from bs4 import BeautifulSoup, SoupStrainer
import feedparser
import chardet
from concurrent.futures import ThreadPoolExecutor
from matplotlib import pyplot as plt
import tqdm

# Local imports
from ffffff import lookup_keyword
from utils.RL import ConfigManager
from utils.bbc_scripe_cdx import get_all_urls_cdx
from utils.duckduckgo import lookup_duckduckgos
import utils.keywords as kw
from utils.RobotsSql import RobotsSql
from utils.SQLDictClass import SQLDictClass
from utils.BackedURLQueue import BackedURLQueue, URLItem
import utils.keywords_finder as kw_finder
from utils.ovarit import ovarit_domain_scrape
from utils.reddit import reddit_domain_scrape
# from javascript import require

import logging
def handler_SIGINT(signum, frame):
    c = input("Do you really want to quit? (y/n) ")
    if c == "y":
        print(f"Signal {signum} received")
        exit()
    else:
        print("Signal ignored")
def handler_SIGHUP(signum, frame):
    print(f"Signal {signum} received")

# log to file
logging.basicConfig(filename='meage_scrape.log', level=logging.INFO)


async def process_article(html, url):
    try:
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

# Initialize logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
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

from urllib.parse import urlparse, urlunparse, quote, unquote

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
            exclude_lag=[]
    ):
        self.config_manager = ConfigManager(f"data/config_{name}.json")
        self.start_urls = start_urls
        self.feeds = feeds
        self.name = name
        self.keywords = keywords
        self.anti_keywords = anti_keywords
        self.key_finder = kw_finder.KeypaceFinder(keywords)
        self.plugins = plugins
        self.output_csv = f"data/{name}.csv"
        self.file_db = f"databases/{name}.db"
        self.cpu_cores = multiprocessing.cpu_count()
        self.max_limit = self.config_manager.get_value("max_limit", default=100, value_type=int, min_value=1,
                                                       max_value=3000, policy="decay_epsilon_greedy")
        self.max_workers = self.config_manager.get_value("max_workers", default=self.cpu_cores * 2, value_type=int,
                                                         min_value=1, max_value=self.cpu_cores*4, policy="decay_epsilon_greedy")
        self.bach_size = self.config_manager.get_value("bach_size", default=500, value_type=int,
                                                         min_value=1, max_value=3000, policy="decay_epsilon_greedy")
        self.semaphore = asyncio.Semaphore(self.bach_size)
        self.thread_pool = ThreadPoolExecutor(max_workers=self.max_workers)
        self.conn = None
        self.robots = None
        self.urls_to_visit = None
        self.allowed_subdirs = allowed_subdirs or []
        self.start_date = start_date
        self.end_date = end_date
        self.exclude_subdirs = exclude_subdirs or []
        self.csv_rows = []
        self.lock = asyncio.Lock()
        self.has_internet_error = False
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
        if not os.path.exists(self.output_csv) or os.path.getsize(self.output_csv) == 0:
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
            # Normalize the URL first
            normalized_url = normalize_url_format(url)
    
            # Validate URL and check if it has been seen before
            if not self.is_valid_url(normalized_url):
                return
            if await self.urls_to_visit.have_been_seen(normalized_url):
                return
    
            # Check allowed subdirectories and exclude subdirectories
            if not any(
                normalized_url.startswith(subdir) for subdir in self.allowed_subdirs
            ):
                return
            if any(
                normalized_url.startswith(subdir) for subdir in self.exclude_subdirs
            ):
                return
    
            # Check robots.txt rules
            robot = await self.robots.get(normalized_url)
            if robot and not robot.can_fetch("*", normalized_url):
                return
    
            # Create a URL item and add it to the queue
            url_item = URLItem(
                url=normalized_url,
                url_score=priority,
                page_type=url_type,
                status="unseen"
            )
            await self.urls_to_visit.push(url_item)
    
        except Exception as e:
            # Log any errors encountered during queue addition
            logger.error(f"Failed to add URL to queue: {url} - Error: {e}")

    def is_valid_url(self, url):
        parsed = urlparse(url)
        return all([parsed.scheme, parsed.netloc])
    
    def is_valid_url(self, url):
        parsed = urlparse(url)
        return all([parsed.scheme, parsed.netloc])

    async def fetch_content(self, session, url_item):
        url = url_item.url
        ssl_context = ssl.create_default_context(cafile=certifi.where())
        retries = 5
        attempt = 0
        backoff_base = 2  # Base for exponential backoff
        while attempt < retries:
            try:
                async with session.get(
                    url, ssl=ssl_context, timeout=60
                ) as response:
                    if response.status == 200:
                        content_bytes = await response.read()
                        encoding = chardet.detect(content_bytes)["encoding"] or "utf-8"
                        text = content_bytes.decode(encoding, errors="replace")
                        await self.urls_to_visit.update_status(url, "seen")
                        return text, response.headers.get("Content-Type", "")
                    elif response.status == 404:
                        logger.warning(f"URL not found (404): {url}")
                        await self.urls_to_visit.update_status(url, "error")
                        return None, None
                    else:
                        logger.warning(
                            f"Failed to fetch {url}, status code {response.status}, retrying... ({attempt + 1}/{retries})"
                        )
                        attempt += 1
                        await asyncio.sleep(backoff_base ** attempt + random.uniform(0, 1))
            except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                # Network-related exceptions
                logger.error(
                    f"Network error while fetching {url}: {e}, retrying... ({attempt + 1}/{retries})"
                )
                await asyncio.sleep(backoff_base ** attempt + random.uniform(0, 1))

                # Check if internet connection is lost
                if not await check_internet():
                    self.has_internet_error= True
                    logger.error(f"Internet connection lost while fetching {url}. Waiting for connection to be restored...")
                    # Wait until the internet connection is restored
                    while not await check_internet():
                        await asyncio.sleep(5)  # Wait 5 seconds before checking again
                    logger.info(f"Internet connection restored. Resuming fetch for {url}.")
                else: # Internet connection is still active
                    attempt += 1
            except Exception as e:
                logger.error(f"Unexpected error while fetching {url}: {e}")
                break  # Exit the retry loop for unexpected exceptions

        await self.urls_to_visit.update_status(url, "error")
        logger.error(f"Max retries reached for {url}")
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

    async def analyze_webpage_content(self, text, url_item, metadata):
        loop = asyncio.get_event_loop()
        try:
            article = await process_article(text, url_item.url)
            # Offload the entire content analysis to the thread pool
            result = await loop.run_in_executor(
                self.thread_pool,
                self._analyze_webpage_content_sync,
                text,
                url_item,
                metadata,
                article
            )

            if result is None:
                return

            # Unpack the results
            score, keywords, metadata, links_to_queue, canonical_url = result

            # Queue extracted links for further crawling
            for link_info in links_to_queue:
                await self.add_to_queue(*link_info)

            # Check if we need to process the canonical URL instead
            if canonical_url and url_item.url != canonical_url:
                await self.add_to_queue(canonical_url, "webpage", priority=score)
                return  # Exit processing for the original URL

            # If the score is positive, update the score and save to CSV
            if score > 0:
                await self.urls_to_visit.set_page_score(url_item.url, score)
                self.collect_csv_row(url_item, score, keywords, metadata)

        except Exception as e:
            logger.error(f"Error in analyze_webpage_content for {url_item.url}: {e}")
            await self.urls_to_visit.update_error(url_item.url, str(e))

    def _analyze_webpage_content_sync(self, text, url_item, metadata,article):
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
            links = set(a["href"] for a in bs.find_all("a", href=True))
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
            # await self.urls_to_visit.update_error(
            #     url, f"Metadata extraction failed: {e}"
            # )
            logger.error(f"Failed to extract metadata for {url}: {e}")
        return metadata

    def _merge_jsonld_data(self, target, source):
        """Helper function to merge JSON-LD data into the target dictionary."""
        for key, value in source.items():
            if key == "@type" and isinstance(value, str):
                value = [value]

            if isinstance(target[key], list) and isinstance(value, list):
                target[key].extend(value)
            elif isinstance(target[key], list):
                target[key].append(value)
            elif isinstance(value, list):
                target[key] = [target[key]] + value
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

    async def extract_and_queue_links(self, bs, url, score):
        loop = asyncio.get_event_loop()
        # Extract and queue links for further crawling
        links = await loop.run_in_executor(
            self.thread_pool,
            self._extract_links_sync,
            bs
        )
        for link in links:
            full_url = urljoin(url, link)
            await self.add_to_queue(full_url, "webpage", priority=score)
    
    def _analyze_content_sync(self, content_to_score):
        try:
            bs_content = BeautifulSoup(content_to_score, "html.parser")
            text_content = bs_content.get_text(separator=" ")
            score, keywords, _ = self.key_finder.relative_keywords_score(text_content)
            return text_content, score, keywords
        except Exception as e:
            logger.error(f"Error in _analyze_content_sync: {e}")
            return None
        
    def _extract_links_sync(self, bs):
        try:
            links = set(a["href"] for a in bs.find_all("a", href=True))
            return links
        except Exception as e:
            logger.error(f"Error extracting links: {e}")
            return set()


    def collect_csv_row(self, url_item, score, keywords, metadata):
        root_url = urlparse(url_item.url).netloc
        crawled_at = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        row = {
            "Root Domain": root_url,
            "URL": url_item.url,
            "Score": score,
            "Keywords Found": ", ".join(keywords),
            "Headline": metadata.get("headline"),
            "name": metadata.get("name"),
            "Date Published": metadata.get("datePublished", ""),
            "Date Modified": metadata.get("dateModified", ""),
            "Author": metadata.get("author", ""),
            "Byline": metadata.get("byline", ""),
            "type": metadata.get("@type", ""),
            "crawled at": crawled_at,
        }
        self.csv_rows.append(row)
        if len(self.csv_rows) >= 100:
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
        print("adjust_parameters")
        """Adjust parameters based on updated Q-values."""
        # Retrieve updated values for 'max_limit' and 'max_workers'
        
        self.bach_size = self.config_manager.get_value("max_workers", default=500, value_type=int,
                                                         min_value=1, max_value=3000, policy="decay_epsilon_greedy")
        self.max_workers = self.config_manager.get_value(
            "max_workers", default=self.cpu_cores*2, value_type=int, min_value=1, max_value=self.cpu_cores*4, policy="decay_epsilon_greedy"
        )

        # Update semaphore and thread pool if values have changed
        self.semaphore._value = self.bach_size
        logger.info(f"Updated max_limit to {self.bach_size}")
        self.thread_pool._max_workers = self.max_workers
        logger.info(f"Updated max_workers to {self.max_workers}")


    async def _crawl_loop(self, session):
        draw_plot = True
        loop = asyncio.get_event_loop()
        max_count = await self.urls_to_visit.count_all()
        done_count = await self.urls_to_visit.count_seen()
        await self.urls_to_visit.reload()

        # Set up live plot with 4 lines
        if draw_plot:
            plt.ion()
            fig, ax = plt.subplots()
            ax.set_title("Task Parameters and Time Taken vs. Batch Number")
            ax.set_xlabel("Batch Number")
            ax.set_ylabel("Metrics")

            max_limit_line, = ax.plot([], [], label="Max Limit", color="blue")
            max_workers_line, = ax.plot([], [], label="Max Workers", color="green")
            concurrent_limit_line, = ax.plot([], [], label="Concurrent Task Limit", color="purple")
            rewards_line, = ax.plot([], [], label="rewards", color="pink")
            time_line, = ax.plot([], [], label="Time Taken (seconds)", color="red")
            ax.legend()

        # Initialize lists to store plot data
        batch_numbers = []
        max_limit_data = []
        max_workers_data = []
        concurrent_limit_data = []
        time_taken_data = []
        rewards = []
        with tqdm.tqdm(
            total=max_count, initial=done_count, desc="Crawling Progress", unit="url"
        ) as pbar:
            tasks = set()
            start = time.time()  # Initialize start time for reward calculation
            concurrent_task_limit = self.config_manager.get_value("concurrent_task_limit", default=100, value_type=int, min_value=1, max_value=1300, policy="decay_epsilon_greedy")
            batch_number = 0
            
            bach_size = self.config_manager.get_value("bach_size", default=200, value_type=int, min_value=100, max_value=1300,policy="decay_epsilon_greedy")
            max_workers = self.config_manager.get_value("max_workers", default=200, value_type=int, min_value=100, max_value=1300,policy="decay_epsilon_greedy")
            concurrent_task_limit = self.config_manager.get_value("concurrent_task_limit", default=100, value_type=int, min_value=1, max_value=1300,policy="decay_epsilon_greedy")
            
            print("batch_number->",batch_number)
            print("bach_size->",bach_size)
            print("max_workers->",max_workers)
            print("concurrent_task_limit->",concurrent_task_limit)

            while (await self.urls_to_visit.count_unseen()) or (await self.urls_to_visit.count_processing()):
                async for url_item in self._url_generator():
                    max_count = await self.urls_to_visit.count_all()
                    pbar.total = max_count
                    task = loop.create_task(self.fetch_and_process_url(url_item, session, pbar))
                    tasks.add(task)

                    if len(tasks) >= concurrent_task_limit:
                        done, _ = await asyncio.wait(tasks)

                        # Calculate reward and update Q-learning step
                        time_taken = time.time() - start
                        completed_tasks = len(done)

                        if not self.has_internet_error:
                            reward = completed_tasks / time_taken if time_taken > 0 else 0
                            self.config_manager.step(["max_limit", "max_workers", "concurrent_task_limit"], reward)
                            self.config_manager.save_config()

                            # Retrieve new values from ConfigManager
                            max_limit = self.config_manager.get_value("bach_size", default=100, value_type=int, min_value=50, max_value=300,policy="decay_epsilon_greedy")
                            max_workers = self.config_manager.get_value("max_workers",default=min(self.cpu_cores//2,1), value_type=int, min_value=self.cpu_cores, max_value=self.cpu_cores*4,policy="decay_epsilon_greedy")
                            concurrent_task_limit = self.config_manager.get_value("concurrent_task_limit", default=100, value_type=int, min_value=50, max_value=300,policy="decay_epsilon_greedy")
                            
                            print("batch_number->",batch_number)
                            print("max_limit->",max_limit)
                            print("max_workers->",max_workers)
                            print("concurrent_task_limit->",concurrent_task_limit)
                            print("reward->",reward)
                            self.adjust_parameters()
                            if draw_plot:
                                # Update plot data
                                batch_numbers.append(batch_number)
                                rewards.append(reward)
                                max_limit_data.append(max_limit)
                                max_workers_data.append(max_workers)
                                concurrent_limit_data.append(concurrent_task_limit)
                                time_taken_data.append(time_taken)

                                # Set data for each line
                                max_limit_line.set_data(batch_numbers, max_limit_data)
                                max_workers_line.set_data(batch_numbers, max_workers_data)
                                concurrent_limit_line.set_data(batch_numbers, concurrent_limit_data)
                                rewards_line.set_data(batch_numbers,rewards)
                                time_line.set_data(batch_numbers, time_taken_data)

                                # Update plot
                                ax.relim()
                                ax.autoscale_view()
                                fig.canvas.draw()
                                fig.canvas.flush_events()

                            batch_number += 1

                        # Reset start time for the next batch
                        start = time.time()
                        tasks.difference_update(done)

                if tasks:
                    await asyncio.gather(*tasks)

                self.write_to_csv()
        if draw_plot:
            plt.ioff()  # Turn off interactive mode at the end
            plt.show()  # Display the final plot




    async def fetch_and_process_url(self, url_item, session,pbar):
        if not url_item:
            return

        # async with self.semaphore:
        if True:
            try:
                text, mime = await self.fetch_content(session, url_item)
                if text:
                    await self.process_content(url_item, text, mime)
                else:
                    logger.error(f"Empty content for {url_item.url}")
            except Exception as e:
                logger.error(f"Error processing {url_item.url}: {e}")
        
        pbar.update(1)

    async def crawl_start(self):
        os.makedirs(os.path.dirname(self.file_db), exist_ok=True)
        async with aiosqlite.connect(self.file_db) as self.conn:
            self.conn.row_factory = aiosqlite.Row
            await self.initialize_db()
            self.initialize_output_csv()
            await self.initialize_urls()
            await self.crawl_main()
