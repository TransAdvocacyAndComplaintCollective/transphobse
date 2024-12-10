import asyncio
import os
import ssl
import time
import math
import orjson
import logging
import aiohttp
import aiosqlite
import feedparser
import multiprocessing
import signal
import traceback
from aiohttp import ClientSession
from aiofiles import open as aio_open
from aiocsv import AsyncDictWriter
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse, urlunparse, quote, unquote
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Dict, List, Optional, Set, Tuple
from asyncio.subprocess import PIPE

# Local imports (Ensure these modules are correctly implemented)
from utils.bbc_scripe_cdx import get_all_urls_cdx
from utils.ovarit import ovarit_domain_scrape
from utils.RobotsSql import RobotsSql
from utils.BackedURLQueue import BackedURLQueue, URLItem
from utils.keyword_search import searx_search, searx_search_news
import utils.keywords as kw
import utils.keywords_finder as kw_finder
from utils.reddit import reddit_domain_scrape

# Attempt to use uvloop for faster event loop
try:
    import uvloop
    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
except ImportError:
    logging.warning("uvloop is not installed. Falling back to the default asyncio event loop.")

# Initialize asynchronous logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36"
    ),
    "Accept": (
        "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,"
        "image/png,image/svg+xml,*/*;q=0.8"
    ),
    "Accept-Language": "en-US,en;q=0.5",
    "Upgrade-Insecure-Requests": "1",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
    "Sec-Fetch-User": "?1",
    "Sec-GPC": "1",
    "Accept-Encoding": "gzip, deflate, br",
}

def normalize_url(url: str) -> str:
    """Standardize and clean URL format."""
    try:
        if "web.archive.org" in url:
            parsed = urlparse(url)
            if parsed.path.startswith("/web/"):
                parts = parsed.path.split("/", 4)
                if len(parts) == 5:
                    url = parts[4]

        parsed = urlparse(url)
        scheme = parsed.scheme.lower() if parsed.scheme else "https"
        hostname = parsed.hostname.lower().replace("www.", "") if parsed.hostname else ""
        path = quote(unquote(parsed.path.rstrip("/")), safe="/")

        # Remove default ports
        port = ""
        if parsed.port and not ((scheme == "http" and parsed.port == 80) or (scheme == "https" and parsed.port == 443)):
            port = f":{parsed.port}"

        netloc = f"{hostname}{port}"
        normalized = urlunparse((scheme, netloc, path + "/", "", parsed.query, ""))

        return normalized
    except Exception as e:
        logger.error(f"Error normalizing URL {url}: {e}")
        return url  # Return the original URL if normalization fails

class Crawler:
    def __init__(
        self,
        start_urls: List[str],
        feeds: List[str],
        name: str,
        keywords: Optional[List[str]] = None,
        anti_keywords: Optional[List[str]] = None,
        plugins: Optional[List[Any]] = None,
        allowed_subdirs: Optional[List[str]] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        exclude_subdirs: Optional[List[str]] = None,
        exclude_scrape_subdirs: Optional[List[str]] = None,
        piloting: bool = False,
        show_bar: bool = False,
        find_new_site: bool = False,
    ):
        """
        Initialize the crawler with necessary configurations.
        """
        self.start_urls = start_urls
        self.feeds = feeds
        self.name = name
        self.keywords = keywords or kw.KEYWORDS
        self.anti_keywords = anti_keywords or kw.ANTI_KEYWORDS
        self.key_finder = kw_finder.KeypaceFinder(self.keywords)
        self.plugins = plugins or []
        self.output_csv = f"data/{self.name}.csv"
        self.file_db = f"databases/{self.name}.db"
        self.cpu_cores = multiprocessing.cpu_count()
        self.find_new_site = find_new_site

        # Configuration parameters
        self.max_workers = min(self.cpu_cores * 5, 1000)
        self.concurrent_task_limit = 100  # Adjust based on system capabilities
        self.collect_csv_row_limit = self.cpu_cores * 2

        # Piloting related
        self.piloting = piloting
        self.show_bar = show_bar

        # Semaphores for concurrency control
        self.semaphore = asyncio.Semaphore(self.concurrent_task_limit)

        # Thread pool for blocking operations
        self.thread_pool = ThreadPoolExecutor(max_workers=self.max_workers)

        # Database and URL queue placeholders
        self.conn: Optional[aiosqlite.Connection] = None
        self.robots: Optional[RobotsSql] = None
        self.urls_to_visit: Optional[BackedURLQueue] = None

        # Directory and subdirectory settings
        self.allowed_subdirs = allowed_subdirs or []
        self.start_date = start_date
        self.end_date = end_date
        self.exclude_scrape_subdirs = exclude_scrape_subdirs or []
        self.exclude_subdirs = exclude_subdirs or []

        # CSV row collection and locking mechanism
        self.csv_rows: List[Dict[str, Any]] = []
        self.lock = asyncio.Lock()

        # Internet connectivity flag
        self.has_internet_error = False

        # Estimation attributes
        self.total_discovered_links = 0
        self.average_links_per_page = 10
        self.filtering_factor = 0.8
        self.estimated_total_pages = 1000
        self.growth_rate = 0.1
        self.midpoint_batch = 5
        self.discovery_rate: List[int] = []
        self.exponential_phase = True

        # Subprocess pool for article processing
        self.process_article_semaphore = asyncio.Semaphore(50)  # Limit concurrent subprocesses
        self.article_process_pool = asyncio.Queue(maxsize=1000)  # Queue to manage subprocess tasks

    async def initialize_db(self) -> None:
        """Set up the database and required tables."""
        self.robots = RobotsSql(self.conn, table_name="robot_dict")
        self.urls_to_visit = BackedURLQueue(self.conn, table_name="urls_to_visit")
        await self.urls_to_visit.initialize()
        await self.robots.initialize()

    async def initialize_output_csv(self) -> None:
        """Initialize the output CSV file with headers asynchronously."""
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
            async with aio_open(self.output_csv, mode="w", newline="", encoding="utf-8") as csv_file:
                writer = AsyncDictWriter(csv_file, fieldnames=fieldnames)
                await writer.writeheader()

    async def initialize_urls(self) -> None:
        """Seed the initial URLs into the queue if it's empty."""
        if await self.urls_to_visit.count_seen() > 0:
            logger.info("URLs have already been initialized.")
            return

        initial_tasks = []
        for url in self.start_urls:
            initial_tasks.append(self.add_to_queue(url, "webpage", priority=0))
        for feed in self.feeds:
            initial_tasks.append(self.add_to_queue(feed, "feed", priority=0))
        await asyncio.gather(*initial_tasks)

    async def _scrape_related_domains(self) -> None:
        """
        Scrape related domains based on keywords and add them to the queue.
        """
        if self.find_new_site:
            related_tasks = []
            for keyword in self.keywords:
                related_tasks.append(searx_search_news(keyword))
            results = await asyncio.gather(*related_tasks, return_exceptions=True)
            for res in results:
                if isinstance(res, Exception):
                    logger.error(f"Error during searx_search_news: {res}")
                    continue
                for result in res:
                    link = result.get("link")
                    if link:
                        score, _, _ = self.key_finder.relative_keywords_score(result.get("link_text", ""))
                        await self.add_to_queue(link, "webpage", min(score, 1))

        related_domain_tasks = []
        for url in self.start_urls:
            parsed_url = urlparse(url)
            hostname = parsed_url.hostname
            if not hostname:
                continue

            related_domain_tasks.append(reddit_domain_scrape(hostname))
            related_domain_tasks.append(ovarit_domain_scrape(hostname))

        domain_results = await asyncio.gather(*related_domain_tasks, return_exceptions=True)
        for res in domain_results:
            if isinstance(res, Exception):
                logger.error(f"Error during domain scraping: {res}")
                continue
            for domain in res:
                await self.add_to_queue(domain[0], "webpage", priority=1)

        keyword_tasks = []
        for keyword in self.keywords:
            keyword_tasks.append(searx_search(keyword))
            keyword_tasks.append(searx_search_news(keyword))
        keyword_results = await asyncio.gather(*keyword_tasks, return_exceptions=True)
        for res in keyword_results:
            if isinstance(res, Exception):
                logger.error(f"Error during keyword search: {res}")
                continue
            for result in res:
                if result.get("link_text") == "cached":
                    continue
                score, _, _ = self.key_finder.relative_keywords_score(result.get("link_text", ""))
                await self.add_to_queue(result.get("link"), "webpage", min(score, 1))

    async def add_to_queue(self, url: str, url_type: str, priority: int = 0) -> None:
        """
        Add a URL to the queue after normalization and validation.
        """
        try:
            normalized_url = normalize_url(url)

            if not self.is_valid_url(normalized_url):
                return
            if await self.urls_to_visit.have_been_seen(normalized_url):
                return

            if self.allowed_subdirs and not any(normalized_url.startswith(subdir) for subdir in self.allowed_subdirs):
                return
            if any(normalized_url.startswith(subdir) for subdir in self.exclude_subdirs):
                return

            robot = await self.robots.get_robot_parser(normalized_url)
            if robot and not robot.can_fetch("*", normalized_url):
                return

            url_item = URLItem(
                url=normalized_url,
                url_score=priority,
                page_type=url_type,
                status="unseen",
            )
            await self.urls_to_visit.push(url_item)

            # Update discovery metrics
            self.total_discovered_links += 1
            self.estimated_total_pages = int(
                self.total_discovered_links * self.average_links_per_page * self.filtering_factor
            )

            # Update the estimated total pages
            await self.update_estimated_total_pages(batch_number=len(self.discovery_rate))
        except Exception as e:
            logger.error(f"Failed to add URL to queue: {url} - Error: {e}")

    def is_valid_url(self, url: str) -> bool:
        """
        Check if a URL is valid.
        """
        parsed = urlparse(url)
        return all([parsed.scheme, parsed.netloc])

    async def fetch_content(self, session: ClientSession, url_item: URLItem) -> Tuple[Optional[str], Optional[str]]:
        """
        Fetch the content of a URL.
        """
        url = url_item.url
        retries = 5
        backoff = 3
        ssl_context = ssl.create_default_context(cafile=ssl.get_default_verify_paths().cafile)

        for attempt in range(retries):
            try:
                async with session.get(url, ssl=ssl_context, timeout=30) as response:
                    if response.status == 200:
                        text = await response.text(encoding='utf-8', errors='replace')
                        if text.strip():
                            return text, response.headers.get("Content-Type", "")
                    elif response.status in {429, 503}:
                        await asyncio.sleep(backoff)
                        backoff *= 2
                    else:
                        return None, None
            except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                logger.warning(f"Error fetching {url}: {e}. Retrying in {backoff} seconds.")
                await asyncio.sleep(backoff)
                backoff *= 2
            except Exception as e:
                logger.error(f"Unexpected error fetching {url}: {e}")
                return None, None

        return None, None

    async def process_article(self, html: str, url: str) -> Dict[str, Any]:
        """
        Process the article using a subprocess.
        """
        async with self.process_article_semaphore:
            try:
                process = await asyncio.create_subprocess_exec(
                    "node",
                    "utils/ProcessArticle.js",
                    url,
                    stdin=PIPE,
                    stdout=PIPE,
                    stderr=PIPE,
                )
                stdout, stderr = await process.communicate(input=html.encode("utf-8"))
                if process.returncode == 0:
                    return orjson.loads(stdout.decode("utf-8"))
                else:
                    logger.error(f"Error processing article {url}: {stderr.decode('utf-8')}")
                    return {}
            except Exception as e:
                logger.exception(f"Exception occurred while processing article {url}: {e}")
                return {}

    async def process_content(self, url_item: URLItem, text: str, mime: str) -> None:
        """
        Process the fetched content based on MIME type.
        """
        if "application/rss+xml" in mime or url_item.page_type == "feed":
            await self.process_feed(text, url_item.url)
        elif "application/xml" in mime or url_item.page_type == "sitemap":
            await self.process_sitemap(text, url_item.url)
        elif "text/html" in mime:
            await self.analyze_webpage_content(text, url_item)
        else:
            await self.urls_to_visit.update_error(url_item.url, f"Unsupported MIME type: {mime}")
            logger.warning(f"Unsupported MIME type for {url_item.url}: {mime}")

    async def update_estimated_total_pages(self, batch_number: int) -> None:
        """
        Update the estimated total number of pages using a growth model.
        """
        current_links = self.total_discovered_links

        if self.exponential_phase:
            estimated_total = current_links * math.exp(self.growth_rate * batch_number)

            if len(self.discovery_rate) >= 5:
                recent_rate = sum(self.discovery_rate[-5:]) / 5
                overall_rate = sum(self.discovery_rate) / len(self.discovery_rate)
                if recent_rate < 0.8 * overall_rate:
                    self.exponential_phase = False
                    logger.info("Switching to logistic growth phase for estimation.")
        else:
            L = self.estimated_total_pages
            k = self.growth_rate
            t = batch_number
            t0 = self.midpoint_batch
            estimated_total = L / (1 + math.exp(-k * (t - t0)))

        self.estimated_total_pages = int(round(max(self.estimated_total_pages, estimated_total)))

    async def analyze_webpage_content(self, text: str, url_item: URLItem, metadata: Optional[Dict[str, Any]] = None) -> None:
        """
        Analyze the content of a webpage.
        """
        metadata = metadata or {}
        try:
            if any(url_item.url.startswith(subdir) for subdir in self.exclude_scrape_subdirs):
                await self.urls_to_visit.mark_seen(url_item.url)
                return

            article = await self.process_article(text, url_item.url)
            if not article:
                await self.urls_to_visit.mark_seen(url_item.url)
                return

            score, keywords, links_to_queue, canonical_url, metadata = await self._analyze_webpage_content_sync(text, url_item, article)

            if score is None:
                await self.urls_to_visit.mark_seen(url_item.url)
                return

            # Update discovery metrics
            self.update_discovery_metrics(len(links_to_queue))

            # Update estimated total pages
            await self.update_estimated_total_pages(batch_number=len(self.discovery_rate))

            # Add discovered links to the queue
            add_tasks = [self.add_to_queue(*link_info) for link_info in links_to_queue]
            await asyncio.gather(*add_tasks, return_exceptions=True)

            # Handle canonical URL
            if canonical_url and url_item.url != canonical_url:
                await self.add_to_queue(canonical_url, "webpage", priority=score)

            await self.urls_to_visit.mark_seen(url_item.url)
            # Collect CSV row if score is positive
            if score > 0:
                await self.urls_to_visit.set_page_score(url_item.url, score)
                await self.collect_csv_row(url_item, score, keywords, metadata)
        except Exception as e:
            logger.exception(f"Error in analyze_webpage_content for {url_item.url}: {e}")
            await self.urls_to_visit.update_error(url_item.url, str(e))

    async def _analyze_webpage_content_sync(
        self, text: str, url_item: URLItem, article: Dict[str, Any]
    ) -> Optional[Tuple[float, List[str], List[Tuple[str, str, float]], Optional[str], Dict[str, Any]]]:
        """
        Synchronous method to analyze webpage content.
        """
        try:
            bs = BeautifulSoup(text, "lxml")
            metadata = self.extract_metadata(bs, url_item.url)

            content_to_score = article.get("content", bs.get_text(separator=" "))
            score, keywords, _ = self.key_finder.relative_keywords_score(content_to_score)

            # Extract and prepare links to queue
            links = {a["href"] for a in bs.find_all("a", href=True)}
            links_to_queue = [
                (urljoin(url_item.url, link), "webpage", score) for link in links if self.is_valid_url(urljoin(url_item.url, link))
            ]

            # Check for canonical URL
            canonical_url = None
            for link in bs.find_all("link", rel=lambda x: x and "canonical" in x):
                canonical_href = link.get("href")
                if canonical_href:
                    canonical_url = normalize_url(canonical_href)
                    break

            return score, keywords, links_to_queue, canonical_url, metadata
        except Exception as e:
            logger.exception(f"Error in _analyze_webpage_content_sync: {e}")
            return None

    def extract_metadata(self, bs: BeautifulSoup, url: str) -> Dict[str, Any]:
        """
        Extract metadata from the HTML content.
        """
        metadata = {}
        try:
            json_ld = self.extract_json_ld(bs)
            if json_ld and isinstance(json_ld, dict):
                metadata.update({
                    "headline": json_ld.get("headline", ""),
                    "datePublished": json_ld.get("datePublished", ""),
                    "dateModified": json_ld.get("dateModified", ""),
                    "author": json_ld.get("author", ""),
                    "keywords": json_ld.get("keywords", ""),
                    "publisher": json_ld.get("publisher", {}).get("name", ""),
                    "url": json_ld.get("url", ""),
                    "type": json_ld.get("@type", "WebPage"),
                })
        except Exception as e:
            logger.error(f"Failed to extract metadata for {url}: {e}")
        return metadata

    def extract_json_ld(self, bs: BeautifulSoup) -> Optional[Dict[str, Any]]:
        """
        Extract JSON-LD data from HTML.
        """
        try:
            scripts = bs.find_all("script", type="application/ld+json")
            for script in scripts:
                try:
                    data = orjson.loads(script.get_text() or "{}")
                    if isinstance(data, list):
                        for item in data:
                            if isinstance(item, dict):
                                return item
                    elif isinstance(data, dict):
                        return data
                except orjson.JSONDecodeError:
                    logger.warning("JSON-LD extraction failed due to JSON decoding error.")
        except Exception as e:
            logger.error(f"Error extracting JSON-LD: {e}")
        return None

    def extract_microdata(self, bs: BeautifulSoup) -> Dict[str, Any]:
        """
        Extract microdata from HTML.
        """
        microdata_items = {}
        try:
            for tag in bs.find_all(True):
                if tag.has_attr("itemprop"):
                    prop_name = tag["itemprop"]
                    prop_value = tag["content"] if tag.has_attr("content") else tag.get_text(strip=True)
                    microdata_items[prop_name] = prop_value
        except Exception as e:
            logger.error(f"Error extracting microdata: {e}")
        return microdata_items

    async def collect_csv_row(
        self, url_item: URLItem, score: float, keywords: List[str], metadata: Dict[str, Any]
    ) -> None:
        """
        Collect data for CSV output asynchronously.
        """
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
            "Type": metadata.get("type", ""),
            "Crawled At": crawled_at,
        }

        async with self.lock:
            self.csv_rows.append(row)
            if len(self.csv_rows) >= self.collect_csv_row_limit:
                await self.write_to_csv()

    async def write_to_csv(self) -> None:
        """Asynchronously write collected CSV rows to the file."""
        if not self.csv_rows:
            return
        try:
            async with aio_open(self.output_csv, mode="a", newline="", encoding="utf-8") as csv_file:
                writer = AsyncDictWriter(csv_file, fieldnames=self.csv_rows[0].keys())
                await writer.writerows(self.csv_rows)
            self.csv_rows.clear()
        except Exception as e:
            logger.error(f"Failed to write to CSV: {e}")

    async def process_feed(self, text: str, url: str) -> None:
        """
        Process RSS feed content.
        """
        feed = feedparser.parse(text)
        add_tasks = []
        for entry in feed.entries:
            score, keywords, _ = self.key_finder.relative_keywords_score(entry.title)
            add_tasks.append(self.add_to_queue(entry.link, "webpage", priority=score))
        await asyncio.gather(*add_tasks, return_exceptions=True)

    async def process_sitemap(self, text: str, url: str) -> None:
        """
        Process sitemap XML content.
        """
        bs = BeautifulSoup(text, "lxml")
        urls = [loc.text for loc in bs.find_all("loc")]
        add_tasks = [self.add_to_queue(link, "webpage") for link in urls]
        await asyncio.gather(*add_tasks, return_exceptions=True)

    def update_discovery_metrics(self, links_to_queue_count: int) -> None:
        """
        Update discovery metrics based on the number of links queued.
        """
        self.total_discovered_links += links_to_queue_count
        if self.total_discovered_links > 0:
            self.average_links_per_page = (
                (self.average_links_per_page + links_to_queue_count) / 2
            )
        self.discovery_rate.append(links_to_queue_count)
        if len(self.discovery_rate) > 10:
            self.discovery_rate.pop(0)

    async def crawl_main(self) -> None:
        """Main crawling loop."""
        connector = aiohttp.TCPConnector(
            limit=1000,
            ssl=False,
            keepalive_timeout=300,
            limit_per_host=10,
        )
        async with aiohttp.ClientSession(connector=connector, headers=HEADERS) as session:
            await self._crawl_loop(session)

    async def _crawl_loop(self, session: aiohttp.ClientSession) -> None:
        """
        Main loop for crawling with adaptive Q-learning.
        """
        item_count = 0
        start_time = time.time()

        while not  await self.urls_to_visit.empty():  # Await unnecessary here for `queue.empty()`
            urls = []

            # Collect URLs to process
            async for url_item in self._url_generator():
                item_count += 1
                urls.append(url_item)
                if len(urls) >= self.concurrent_task_limit:
                    break
                
            if not urls:  # Break if no URLs are collected
                break
            
            # Create and gather fetch tasks
            tasks = [self.fetch_and_process_url(url_item, session) for url_item in urls]
            results = await asyncio.gather(*tasks, return_exceptions=True)

            # Handle exceptions from tasks
            for idx, result in enumerate(results):
                if isinstance(result, Exception):
                    logger.error(f"Error processing URL {urls[idx]}: {result}")

            # Log progress
            if self.show_bar:
                now_time = time.time()
                time_elapsed = now_time - start_time
                avg_time_per_page = time_elapsed / item_count if item_count > 0 else 0
                # pages_left = await self.urls_to_visit.qsize()
                # estimated_time_left = avg_time_per_page * pages_left

                logger.info(f"Crawled {len(urls)} URLs. Total discovered links: {self.total_discovered_links}")
                logger.info(f"Time elapsed: {time_elapsed:.2f} seconds")
                logger.info(f"Pages per second: {1 / avg_time_per_page:.2f}" if avg_time_per_page > 0 else "N/A")

    async def _url_generator(self):
        """Generator to yield URLs from the queue."""
        while not await self.urls_to_visit.empty():
            url_item = await self.urls_to_visit.pop()
            if url_item:
                yield url_item
            else:
                break

    async def fetch_and_process_url(self, url_item: URLItem, session: aiohttp.ClientSession) -> None:
        """
        Fetch and process a single URL.
        """
        if not url_item:
            return

        try:
            text, mime = await self.fetch_content(session, url_item)
            if text:
                await self.process_content(url_item, text, mime)
            else:
                await self.urls_to_visit.update_error(url_item.url, "Empty content or unsupported status code.")
                logger.error(f"Empty content for {url_item.url}")
        except Exception as e:
            logger.exception(f"Error processing {url_item.url}: {e}")
            await self.urls_to_visit.update_error(url_item.url, str(e))

    async def collect_csv_row(
        self, url_item: URLItem, score: float, keywords: List[str], metadata: Dict[str, Any]
    ) -> None:
        """
        Collect data for CSV output asynchronously.
        """
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
            "Type": metadata.get("type", ""),
            "Crawled At": crawled_at,
        }

        async with self.lock:
            self.csv_rows.append(row)
            if len(self.csv_rows) >= self.collect_csv_row_limit:
                await self.write_to_csv()

    async def write_to_csv(self) -> None:
        """Asynchronously write collected CSV rows to the file."""
        if not self.csv_rows:
            return
        try:
            async with aio_open(self.output_csv, mode="a", newline="", encoding="utf-8") as csv_file:
                writer = AsyncDictWriter(csv_file, fieldnames=self.csv_rows[0].keys())
                await writer.writerows(self.csv_rows)
            self.csv_rows.clear()
        except Exception as e:
            logger.error(f"Failed to write to CSV: {e}")

    async def crawl_start(self):
        """Start the crawling process."""
        os.makedirs(os.path.dirname(self.file_db), exist_ok=True)
        async with aiosqlite.connect(self.file_db) as self.conn:
            self.conn.row_factory = aiosqlite.Row
            await self.initialize_db()
            await self.initialize_output_csv()
            await self.initialize_urls()
            asyncio.create_task(self._scrape_related_domains())

            # Setup signal handlers for graceful shutdown
            loop = asyncio.get_running_loop()
            stop_event = asyncio.Event()

            for sig in (signal.SIGINT, signal.SIGTERM):
                loop.add_signal_handler(sig, lambda: asyncio.create_task(stop_event.set()))

            crawl_task = asyncio.create_task(self.crawl_main())
            stop_event_task = asyncio.create_task(stop_event.wait())

            done, pending = await asyncio.wait(
                [crawl_task, stop_event_task],
                return_when=asyncio.FIRST_COMPLETED
            )

            # Cancel the pending tasks if stop_event was triggered
            for task in pending:
                task.cancel()

            if stop_event.is_set():
                crawl_task.cancel()
                await self.handle_shutdown()

        logger.info("Crawling process completed.")
        await self.handle_shutdown()

    async def handle_shutdown(self):
        """Handle shutdown procedures."""
        if self.urls_to_visit:
            await self.urls_to_visit.close()
        if self.conn:
            await self.conn.close()
        self.thread_pool.shutdown(wait=False)
        logger.info("Shutdown complete.")

# Example usage:
# Ensure that this script is run as the main module.
# if __name__ == "__main__":
#     start_urls = ["https://www.bbc.co.uk"]
#     feeds = [
#         "https://feeds.bbci.co.uk/news/rss.xml",
#         "https://feeds.bbci.co.uk/news/uk/rss.xml",
#         # Add more feeds as needed
#     ]
#     crawler = Crawler(
#         start_urls=start_urls,
#         feeds=feeds,
#         name="BBC_news_mage_scrape",
#         allowed_subdirs=["https://bbc.co.uk/news/", "https://feeds.bbci.co.uk/news"],
#         exclude_subdirs=[
#             "www.bbc.co.uk/news/world-",
#             "www.bbc.co.uk/news/election-",
#         ],
#         exclude_scrape_subdirs=[
#             "www.bbc.co.uk/news/resources/",
#             "www.bbc.co.uk/news/topics/",
#             "www.bbc.co.uk/news/world-",
#             "www.bbc.co.uk/news/election-",
#         ],
#         find_new_site=True,
#     )
#     asyncio.run(crawler.crawl_start())
