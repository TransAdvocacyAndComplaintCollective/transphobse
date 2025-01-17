import asyncio
from collections import defaultdict
import json
import os
import random
import ssl
import time
import math
import traceback
import orjson
import logging
import aiohttp
import aiosqlite
import feedparser
import multiprocessing
from aiohttp import ClientSession
from aiofiles import open as aio_open
from aiocsv import AsyncDictWriter
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse, urlunparse, quote, unquote
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Dict, List, Optional, Set, Tuple
from asyncio.subprocess import PIPE

import tqdm
from utils.keywords import queries_keywords

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
    logging.warning("uvloop is not installed. Falling back to default asyncio event loop.")

# Initialize asynchronous logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)

PROXIES = [
    # Add proxy URLs here if needed
]

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36",
    "Mozilla/5.0 (Macintosh; U; Intel Mac OS X; en-US; rv:1.9.2.2) "
    "Gecko/20100316 Firefox/3.6.2",
    "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:52.0) "
    "Gecko/20100101 Firefox/52.0",
    # ... add more user agents for better rotation
]
BASE_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36"
    ),
    "Accept": (
        "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,"
        "image/png,image/svg+xml,*/*;q=0.8"
    ),
    "Accept-Language": "en-UK,en;q=0.5",
    "Upgrade-Insecure-Requests": "1",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
    "Sec-Fetch-User": "?1",
    "Sec-GPC": "1",
}


def normalize_url(url: str) -> str:
    """Standardize and clean URL format."""
    try:
        # Remove any trailing semicolons or whitespace from the URL
        url = url.rstrip(" ;\n\t")

        if "web.archive.org" in url:
            parsed = urlparse(url)
            if parsed.path.startswith("/web/"):
                parts = parsed.path.split("/", 4)
                if len(parts) == 5:
                    url = parts[4]

        parsed = urlparse(url)
        scheme = parsed.scheme.lower() if parsed.scheme else "https"
        hostname = parsed.hostname.lower().replace("www.", "") if parsed.hostname else ""

        # Remove trailing slashes and semicolons from path before quoting
        clean_path = unquote(parsed.path.rstrip("/").rstrip(";"))
        path = quote(clean_path, safe="/")

        # Remove default ports
        port = ""
        if parsed.port and not (
            (scheme == "http" and parsed.port == 80)
            or (scheme == "https" and parsed.port == 443)
        ):
            port = f":{parsed.port}"

        netloc = f"{hostname}{port}"

        # Preserve query parameters if they exist
        query = parsed.query

        # Ensure there's at least a "/" if path is empty
        if not path:
            path = "/"

        normalized = urlunparse((scheme, netloc, path, "", query, ""))

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
        allowed_url: Optional[List[str]] = None,
        keywords: Optional[List[str]] = None,
        anti_keywords: Optional[List[str]] = None,
        plugins: Optional[List[Any]] = None,
        allowed_subdirs: Optional[List[str]] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        exclude_subdirs: Optional[List[str]] = None,
        exclude_scrape_subdirs: Optional[List[str]] = None,
        find_new_site: bool = False,
    ):
        """
        Initialize the crawler with necessary configurations.
        """
        if allowed_url:
            self.allowed_url = set(allowed_url)
        else:
            self.allowed_url = set()
        if not find_new_site or not start_urls:
            print("You must allow the code to find new sites or have a starting URL.")
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
        
        # Per-domain locks to ensure we only fetch from one URL of the same domain at a time
        self.domain_locks = defaultdict(asyncio.Lock)
        self.crawl_delay = defaultdict(lambda: 0.0)

        # Track the earliest "next fetch time" by domain
        # We will not fetch from a domain before this time
        self.next_domain_fetch_time = defaultdict(lambda: 0.0)

        # Configuration parameters
        self.max_workers = min(self.cpu_cores * 5, 1000)
        self.concurrent_task_limit = 100  # Adjust based on system capabilities
        self.collect_csv_row_limit = self.cpu_cores * 2

        # Semaphores for concurrency control
        self.semaphore = asyncio.Semaphore(self.concurrent_task_limit)

        # Thread pool for blocking operations
        self.thread_pool = ThreadPoolExecutor(max_workers=self.max_workers)

        # Database and URL queue placeholders
        self.conn: Optional[aiosqlite.Connection] = None
        self.robots: Optional[RobotsSql] = None
        self.urls_to_visit: Optional[BackedURLQueue] = None
        self.domain_locks = defaultdict(asyncio.Lock)
        self.next_domain_fetch_time = defaultdict(lambda: 0.0)


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

        self.node_restart_count = 0
        self.max_restart_attempts = 5
        self.restart_reset_interval = 60  # seconds
        self.last_restart_time = time.time()

        # Subprocess pool for article processing
        self.process_article_semaphore = asyncio.Semaphore(50)  # Limit concurrency
        self.article_process_pool = asyncio.Queue(maxsize=1000)  # Manage subprocess tasks

    async def initialize_db(self) -> None:
        """Set up the database and required tables."""
        self.robots = RobotsSql(self.conn, table_name="robot_dict")
        self.urls_to_visit = BackedURLQueue(self.conn, table_name="urls_to_visit")
        await self.urls_to_visit.initialize()
        await self.robots.initialize()

    async def initialize_sql_table(self) -> None:
        """Initialize the SQL table for storing crawled data."""
        create_table_query = """
        CREATE TABLE IF NOT EXISTS crawled_data (
            root_domain TEXT,
            url TEXT PRIMARY KEY,
            score REAL,
            keywords_found TEXT,
            headline TEXT,
            name TEXT,
            date_published TEXT,
            date_modified TEXT,
            author TEXT,
            byline TEXT,
            type TEXT,
            crawled_at TEXT
        );
        """
        async with self.conn.cursor() as cursor:
            await cursor.execute(create_table_query)
        await self.conn.commit()

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
        # Only write the header if the file doesn't exist or is empty
        if not os.path.exists(self.output_csv) or os.path.getsize(self.output_csv) == 0:
            async with aio_open(self.output_csv, mode="w", newline="", encoding="utf-8") as csv_file:
                writer = AsyncDictWriter(csv_file, fieldnames=fieldnames)
                await writer.writeheader()

    async def initialize_urls(self) -> None:
        """
        Seed the initial URLs into the queue if it's empty.
        This includes both start_urls and feed URLs.
        """
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
        Example function that fetches new domains/links for the queue
        from Searx, Reddit, Ovarit, etc.
        """
        try:
            for keyword in queries_keywords:
                try:
                    async for result in searx_search_news(keyword):
                        link = result.get("link")
                        link_text = result.get("link_text", "")
                        if link_text == "cached":
                            continue
                        if link:
                            score, _, _ = self.key_finder.relative_keywords_score(link_text)
                            await self.add_to_queue(link, "webpage", min(score, 1), add_domain=True)
                except Exception as e:
                    logger.error(f"Error during searx_search_news: {e}")

            # For domain scraping
            for url in self.start_urls:
                parsed_url = urlparse(url)
                hostname = parsed_url.hostname
                if not hostname:
                    continue

                try:
                    async for domain_info in reddit_domain_scrape(hostname):
                        await self.add_to_queue(domain_info[0], "webpage", priority=1)
                except Exception as e:
                    logger.error(f"Error during reddit_domain_scrape: {e}")

                try:
                    async for domain_info in ovarit_domain_scrape(hostname):
                        await self.add_to_queue(domain_info[0], "webpage", priority=1)
                except Exception as e:
                    logger.error(f"Error during ovarit_domain_scrape: {e}")

        except Exception as e:
            logger.error(f"Error during _scrape_related_domains: {e}")
            traceback.print_exc()

    async def add_to_queue(
        self,
        url: str,
        url_type: str,
        priority: int = 0,
        lastmod=None,
        priority_sitemap=None,
        changefreq_sitemap=None,
        add_domain=False,
    ) -> None:
        """
        Add a URL to the queue after normalization, policy application, and validation.
        """
        is_good = False
        try:
            # Apply No-Vary-Search policy before normalization
            url = await self.canonicalize_url_with_policy(url)
            d = urlparse(url)
            # Validate domain
            if self.allowed_url:
                for allowed in self.allowed_url:
                    if d.netloc in allowed:
                        is_good = True
                        break

            # If add_domain is True, assume we want to widen the domain
            # set to include any new domain we discover
            if add_domain and d.netloc not in self.allowed_url:
                self.allowed_url.add(d.netloc)
                self.start_urls.append(url)
            else:
                if not is_good:
                    return

            normalized_url = normalize_url(url)
            if not self.is_valid_url(normalized_url):
                return
            if await self.urls_to_visit.have_been_seen(normalized_url):
                return

            # Optionally restrict to certain subdirs
            if self.allowed_subdirs and not any(
                normalized_url.startswith(subdir) for subdir in self.allowed_subdirs
            ):
                return
            # Optionally exclude certain subdirs
            if any(normalized_url.startswith(subdir) for subdir in self.exclude_subdirs):
                return

            robot = await self.robots.get_robot_parser(normalized_url)
            if robot and not robot.can_fetch("*", normalized_url):
                return

            url_item = URLItem(
                url=normalized_url,
                url_score=priority,
                page_type=url_type,
                lastmod_html=lastmod,
                priority=priority_sitemap,
                changefreq_sitemap=changefreq_sitemap,
                status="unseen",
            )
            await self.urls_to_visit.push(url_item)

            # Update discovery metrics
            self.total_discovered_links += 1
            self.estimated_total_pages = int(
                self.total_discovered_links * self.average_links_per_page * self.filtering_factor
            )
        except Exception as e:
            logger.error(f"Failed to add URL to queue: {url} - Error: {e}")

    async def canonicalize_url_with_policy(self, url: str) -> str:
        """
        Apply No-Vary-Search policy to the given URL if available.
        - If key-order: sorting query parameters by key.
        - If params boolean: remove all query parameters if True
        - If params is a list: remove all query parameters not in that list
        - If except is a list: when params=True, remove all params except those listed
        """
        parsed = urlparse(url)
        domain = parsed.hostname.lower().replace("www.", "") if parsed.hostname else ""
        path = parsed.path or ""
        policy = await self.urls_to_visit.get_no_vary_search_policy(domain, path)
        if not policy:
            return url

        query_params = []
        for q in parsed.query.split("&"):
            if q.strip():
                query_params.append(q.split("=", 1))

        # Apply params/except logic
        if policy["params"] is True:
            if policy["except"]:
                # Keep only params in 'except'
                query_params = [p for p in query_params if p[0] in policy["except"]]
            else:
                query_params = []
        elif isinstance(policy["params"], list):
            # Keep only listed params
            query_params = [p for p in query_params if p[0] in policy["params"]]

        # Apply key-order if set
        if policy["key-order"]:
            query_params.sort(key=lambda x: x[0])

        new_query = "&".join(f"{k}={v}" for k, v in query_params)
        canonical_url = urlunparse(
            (
                parsed.scheme,
                parsed.netloc,
                parsed.path,
                parsed.params,
                new_query,
                parsed.fragment,
            )
        )
        return canonical_url

    def is_valid_url(self, url: str) -> bool:
        """
        Check if a URL is valid.
        """
        parsed = urlparse(url)
        return bool(parsed.scheme in ("http", "https") and parsed.netloc)

    @staticmethod
    async def random_jitter(base: float = 0.5, spread: float = 2.0):
        """Sleep for a random amount of time in [base, base + spread]."""
        delay = base + random.uniform(0, spread)
        await asyncio.sleep(delay)


    def get_random_user_agent(self) -> str:
        """
        Returns a randomly selected user-agent string from USER_AGENTS.
        """
        return random.choice(USER_AGENTS)

    def get_random_proxy(self) -> Optional[str]:
        """
        Returns a randomly selected proxy from PROXIES if available, else None.
        """
        if PROXIES:
            return random.choice(PROXIES)
        return None

    async def fetch_content(
        self,
        session: aiohttp.ClientSession,
        url_item: "URLItem",
        use_proxy: bool = False
    ):
        """
        Fetch raw content from a URL with a per-domain Crawl-Delay (from robots.txt).
        Additionally handles the 'Retry-After' header for rate-limiting responses.
        Returns (text, mime, last_modified).
        """
        domain = urlparse(url_item.url).netloc.lower()
        ssl_context = ssl.create_default_context()
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.CERT_NONE

        # --- NEW: Retrieve crawl_delay from robots.txt ---
        robot = await self.robots.get_robot_parser(url_item.url)
        crawl_delay = 0.0
        if robot:
            # RobotFileParser.crawl_delay(useragent) returns None if not set
            cd = robot.crawl_delay("*")
            if cd is not None:
                crawl_delay = cd

        # We'll make up to 5 retries (with exponential backoff) inside this lock
        retries = 10
        backoff = 3.0

        # Acquire the domain-level lock to enforce single-threaded fetch + crawl-delay
        async with self.domain_locks[domain]:
            # Wait if we’re trying to fetch sooner than allowed
            now = time.time()
            if now < self.next_domain_fetch_time[domain]:
                wait_time = self.next_domain_fetch_time[domain] - now
                logger.info(f"Sleeping {wait_time:.2f}s for domain {domain} due to Crawl-Delay.")
                await asyncio.sleep(wait_time)

            # Set the next fetch time optimistically
            self.next_domain_fetch_time[domain] = time.time() + crawl_delay + self.crawl_delay[domain]

            for attempt in range(retries):
                # Rotate User-Agent (unchanged from your code)
                headers = BASE_HEADERS.copy()
                headers["User-Agent"] = self.get_random_user_agent()

                try:
                    async with session.get(
                        url_item.url,
                        ssl=ssl_context,
                        timeout=30,
                        headers=headers,
                        # proxy=proxy if use_proxy else None
                    ) as response:
                        if response.status == 200:
                            # Reset crawl_delay on successful fetch
                            self.crawl_delay[domain] = max(self.crawl_delay[domain] / 2, 0)
                            text = await response.text(errors="replace")
                            if text.strip():
                                last_modified = response.headers.get("Last-Modified")
                                mime = response.headers.get("Content-Type", "")
                                return text, mime, last_modified
                            else:
                                logger.warning(f"Empty response text for {url_item.url}.")
                                return None, None, None

                        elif response.status in {429, 503}:
                            # Check for 'Retry-After' header
                            retry_after = response.headers.get("Retry-After")
                            if retry_after:
                                try:
                                    retry_after_seconds = int(retry_after)
                                except ValueError:
                                    retry_after_seconds = 0
                                    logger.warning(
                                        f"Received invalid Retry-After header '{retry_after}' for {url_item.url}. Defaulting to backoff."
                                    )
                            else:
                                retry_after_seconds = 0

                            # Use Retry-After or exponential backoff, whichever is longer
                            self.crawl_delay[domain] = max(
                                self.crawl_delay[domain] * 2, retry_after_seconds, 300
                            )  # Cap at 5 minutes

                            logger.warning(
                                f"Rate-limited on {url_item.url}. "
                                f"Increased crawl delay for {domain} to {self.crawl_delay[domain]:.2f}s."
                            )

                            await asyncio.sleep(max(backoff, retry_after_seconds))
                            backoff = min(backoff * 2, 60.0)  # Cap the backoff at 60s
                        else:
                            # For other codes, just return None
                            logger.warning(
                                f"Unexpected status code {response.status} for {url_item.url}. "
                                f"No more retries."
                            )
                            return None, None, None

                except Exception as e:
                    logger.warning(
                        f"Error fetching {url_item.url} (attempt {attempt+1}/{retries}): {e}. "
                        f"Retrying after {backoff}s."
                    )
                    await asyncio.sleep(backoff)
                    backoff = min(backoff * 2, 60.0)

                # Add random jitter after each attempt
                await Crawler.random_jitter(base=0.5, spread=1.0)

            # If we exhausted all retries, we return None
            logger.error(f"Failed to fetch content for {url_item.url} after {retries} retries.")
            return None, None, None


    async def start_node(self):
        """
        Start the Node.js subprocess that runs 'ProcessArticle.js'.
        """
        self.node_process = await asyncio.create_subprocess_exec(
            "node",
            "utils/ProcessArticle.js",
            stdin=PIPE,
            stdout=PIPE,
            stderr=PIPE,
        )

        # Increase stream read limits to handle large articles
        for stream in (self.node_process.stdout, self.node_process.stderr):
            if stream:
                try:
                    transport = stream._transport
                    protocol = transport._protocol if hasattr(transport, "_protocol") else None
                    if protocol and hasattr(protocol, "_stream_reader"):
                        # Increase the limit to 200 MB
                        protocol._stream_reader._limit = 200 * 1024 * 1024
                except Exception as e:
                    logger.warning(f"Failed to increase stream limit: {e}")

        asyncio.create_task(self.log_node_stderr())

    async def log_node_stderr(self):
        """
        Continuously read and log the stderr of the Node process.
        This helps to capture warnings or error messages from the JS side.
        """
        if self.node_process.stderr is None:
            return
        while True:
            line = await self.node_process.stderr.readline()
            if not line:
                break
            decoded_line = line.decode("utf-8").strip()
            # Filter out JSON lines to avoid double-logging
            if "{" not in decoded_line:
                logger.error(f"Node stderr: {decoded_line}")

    async def process_article(self, html: str, url: str) -> Dict[str, Any]:
        """
        Send HTML content and URL to the Node.js 'ProcessArticlePersistent.js' script
        for parsing via Mozilla Readability, then wait for the parsed result.
        If the Node.js process is not running or has exited, restart it (with a retry cap).
        Returns a dictionary with 'content' (sanitized HTML) or 'error' fields.
        """
        async with self.lock:
            try:
                request = json.dumps({"html": html, "url": url}) + "\n"

                # Ensure Node process is running
                if (self.node_process.returncode is not None
                    or self.node_process.stdin.is_closing()):

                    if self.node_restart_count >= self.max_restart_attempts:
                        logger.error("Node process cannot be restarted; limit reached.")
                        return {}

                    self.node_restart_count += 1
                    logger.info("Node process closed. Restarting node process.")
                    await self.start_node()

                self.node_process.stdin.write(request.encode("utf-8"))
                await self.node_process.stdin.drain()

                try:
                    line = await self.node_process.stdout.readline()
                    if not line:
                        raise ValueError(f"No response from Node process for {url}")

                    return json.loads(line.decode("utf-8").strip())

                except asyncio.LimitOverrunError as loe:
                    logger.warning(f"LimitOverrunError for {url}: {loe}. Reading large chunk.")
                    chunk = await self.node_process.stdout.read(2000 * 1024 * 1024)  # 400 MB
                    return json.loads(chunk.decode("utf-8").strip())

            except Exception as e:
                logger.error(f"Error processing article {url}: {e}")
                traceback.print_exc()
                return {}

    async def process_content(self, url_item: URLItem, text: str, mime: str) -> None:
        """
        Based on the MIME type (HTML, RSS, Sitemap, etc.), decide how to handle the content.
        """
        if not text:
            logger.warning(f"Skipping empty content for URL: {url_item.url}")
            return

        if "text/html" in mime:
            await self.analyze_webpage_content(text, url_item)
        elif "application/rss+xml" in mime or url_item.page_type == "feed":
            await self.process_feed(text, url_item.url)
        elif "application/xml" in mime or url_item.page_type == "sitemap":
            await self.process_sitemap(text, url_item.url)
        else:
            logger.warning(f"Unsupported MIME type for {url_item.url}: {mime}")

    async def analyze_webpage_content(
        self, text: str, url_item: URLItem, metadata: Optional[Dict[str, Any]] = None
    ) -> None:
        """
        Analyze HTML content of a webpage using the Node.js readability service,
        then extract links, metadata, and compute a keyword score.
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

            score, keywords, links_to_queue, canonical_url, metadata = (
                await self._analyze_webpage_content_sync(text, url_item, article)
            )
            if score is None:
                await self.urls_to_visit.mark_seen(url_item.url)
                return

            self.update_discovery_metrics(len(links_to_queue))
            await self.update_estimated_total_pages(batch_number=len(self.discovery_rate))

            add_tasks = [self.add_to_queue(*link_info) for link_info in links_to_queue]
            await asyncio.gather(*add_tasks, return_exceptions=True)

            if canonical_url and url_item.url != canonical_url:
                await self.add_to_queue(canonical_url, "webpage", priority=score)
                await self.urls_to_visit.mark_seen(url_item.url)
                return

            await self.urls_to_visit.mark_seen(url_item.url)

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
        Helper to parse the HTML with BeautifulSoup, extract links, compute keywords,
        and find a canonical URL. Returns (score, keywords, links_to_queue, canonical_url, metadata).
        """
        try:
            bs = BeautifulSoup(text, "lxml")
            metadata = self.extract_metadata(bs, url_item.url)

            content_to_score = article.get("content", bs.get_text(separator=" "))
            score, keywords, _ = self.key_finder.relative_keywords_score(content_to_score)

            links = {a["href"] for a in bs.find_all("a", href=True)}
            links_to_queue = [
                (urljoin(url_item.url, link), "webpage", score)
                for link in links
                if self.is_valid_url(urljoin(url_item.url, link))
            ]

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
        Attempt to extract JSON-LD or other metadata from the HTML.
        """
        metadata = {}
        try:
            json_ld = self.extract_json_ld(bs)
            if json_ld and isinstance(json_ld, dict):
                metadata.update(
                    {
                        "headline": json_ld.get("headline", ""),
                        "datePublished": json_ld.get("datePublished", ""),
                        "dateModified": json_ld.get("dateModified", ""),
                        "author": json_ld.get("author", ""),
                        "keywords": json_ld.get("keywords", ""),
                        "publisher": json_ld.get("publisher", {}).get("name", ""),
                        "url": json_ld.get("url", ""),
                        "type": json_ld.get("@type", "WebPage"),
                    }
                )
        except Exception as e:
            logger.error(f"Failed to extract metadata for {url}: {e}")
        return metadata

    def extract_json_ld(self, bs: BeautifulSoup) -> Optional[Dict[str, Any]]:
        """
        Extract JSON-LD data from <script type="application/ld+json"> tags.
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

    async def collect_csv_row(self, url_item: URLItem, score: float, keywords: List[str], metadata: Dict[str, Any]) -> None:
        """
        Collect data for CSV output and store it in the SQL database asynchronously.
        """
        def sanitize(field_value):
            """Convert dictionaries or lists to JSON strings; otherwise, return a string."""
            if isinstance(field_value, (dict, list)):
                return json.dumps(field_value, ensure_ascii=False)
            return str(field_value) if field_value is not None else ""

        root_url = urlparse(url_item.url).netloc
        crawled_at = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())

        row = {
            "root_domain": root_url,
            "url": url_item.url,
            "score": score,
            "keywords_found": ", ".join(keywords),
            "headline": sanitize(metadata.get("headline", "")),
            "name": sanitize(metadata.get("name", "")),
            "date_published": sanitize(metadata.get("datePublished", "")),
            "date_modified": sanitize(metadata.get("dateModified", "")),
            "author": sanitize(metadata.get("author", "")),
            "byline": sanitize(metadata.get("byline", "")),
            "type": sanitize(metadata.get("type", "")),
            "crawled_at": crawled_at,
        }

        async with self.lock:
            # Add to CSV buffer
            self.csv_rows.append(row)
            if len(self.csv_rows) >= self.collect_csv_row_limit:
                await self.write_to_csv()

            # Insert into SQL database
            try:
                insert_query = """
                INSERT INTO crawled_data (
                    root_domain, url, score, keywords_found, headline, name,
                    date_published, date_modified, author, byline, type, crawled_at
                ) VALUES (
                    :root_domain, :url, :score, :keywords_found, :headline, :name,
                    :date_published, :date_modified, :author, :byline, :type, :crawled_at
                )
                ON CONFLICT(url) DO UPDATE SET
                    score=excluded.score,
                    keywords_found=excluded.keywords_found,
                    headline=excluded.headline,
                    name=excluded.name,
                    date_published=excluded.date_published,
                    date_modified=excluded.date_modified,
                    author=excluded.author,
                    byline=excluded.byline,
                    type=excluded.type,
                    crawled_at=excluded.crawled_at;
                """
                async with self.conn.cursor() as cursor:
                    await cursor.execute(insert_query, row)
                await self.conn.commit()
            except Exception as e:
                logger.error(f"Failed to insert into SQL table: {e}")

    async def write_to_csv(self) -> None:
        """
        Asynchronously write collected CSV rows to the file in batches.
        """
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
        Process RSS/Atom feed content. Extract entries and enqueue them as "webpage".
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
        urls = bs.find_all("url")

        add_tasks = []
        for url_tag in urls:
            loc = url_tag.find("loc").text if url_tag.find("loc") else None
            lastmod = url_tag.find("lastmod").text if url_tag.find("lastmod") else None
            priority = float(url_tag.find("priority").text) if url_tag.find("priority") else None
            changefreq = url_tag.find("changefreq").text if url_tag.find("changefreq") else None

            if loc:
                add_tasks.append(
                    self.add_to_queue(
                        loc,
                        "webpage",
                        priority=priority,
                        lastmod=lastmod,
                        changefreq_sitemap=changefreq,
                    )
                )
        await asyncio.gather(*add_tasks, return_exceptions=True)

    def update_discovery_metrics(self, links_to_queue_count: int) -> None:
        """
        Update discovery metrics based on how many new links were found.
        """
        self.total_discovered_links += links_to_queue_count
        if self.total_discovered_links > 0:
            # A rolling average approach
            self.average_links_per_page = (self.average_links_per_page + links_to_queue_count) / 2

        self.discovery_rate.append(links_to_queue_count)
        if len(self.discovery_rate) > 10:
            self.discovery_rate.pop(0)

    async def update_estimated_total_pages(self, batch_number: int) -> None:
        """
        Dynamically estimate total pages using an exponential/logistic model.
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

    async def crawl_main(self) -> None:
        """
        Main crawl loop that orchestrates fetching, processing, and discovering new URLs.
        """
        connector = aiohttp.TCPConnector(
            limit=10,
            ssl=False,
            keepalive_timeout=300,
            limit_per_host=5,
        )
        async with aiohttp.ClientSession(connector=connector, headers=BASE_HEADERS) as session:
            await self._crawl_loop(session)

    async def _crawl_loop(self, session: ClientSession) -> None:
        """
        Repeatedly pop URLs from the queue, fetch them, and process.
        """
        item_count = 0
        start_time = time.time()

        retry_queue = asyncio.Queue()
        max_retries = 3
        retry_delay = 3

        print("Crawling started.")
        while not await self.urls_to_visit.empty() or not retry_queue.empty():
            urls = []

            # Pull from the main queue
            async for url_item in self._url_generator():
                item_count += 1
                urls.append(url_item)
                if len(urls) >= self.concurrent_task_limit:
                    break

            # If main queue is empty, try the retry queue
            if not urls:
                while not retry_queue.empty() and len(urls) < self.concurrent_task_limit:
                    urls.append(await retry_queue.get())

            # Break if no URLs to process
            if not urls:
                break

            tasks = [self.fetch_and_process_url(url_item, session) for url_item in urls]
            results = await asyncio.gather(*tasks, return_exceptions=True)

            for idx, result in enumerate(results):
                url_item = urls[idx]
                if isinstance(result, Exception):
                    logger.error(f"Error processing URL {url_item.url}: {result}")
                    if url_item.retries < max_retries:
                        url_item.retries += 1
                        logger.warning(
                            f"Retrying URL {url_item.url} (attempt {url_item.retries}/{max_retries}) in {retry_delay} seconds."
                        )
                        await asyncio.sleep(retry_delay)
                        await retry_queue.put(url_item)
                    else:
                        logger.error(f"Max retries reached for {url_item.url}. Skipping.")

            # Log progress (unconditional now that show_bar is removed)
            now_time = time.time()
            time_elapsed = now_time - start_time
            avg_time_per_page = time_elapsed / item_count if item_count > 0 else 0

            logger.info(f"Crawled {len(urls)} URLs this batch. "
                        f"Total discovered links: {self.total_discovered_links}")
            logger.info(f"Time elapsed: {time_elapsed:.2f} seconds")
            if avg_time_per_page > 0:
                logger.info(f"Approx. pages/second: {1 / avg_time_per_page:.2f}")

        logger.info("Crawling completed.")

    async def _url_generator(self):
        """
        Yield URLs from the queue until it's empty or a break condition occurs.
        Excludes domains under active delay-wait periods.
        """
        exclude_domains = [
            domain for domain, next_fetch in self.next_domain_fetch_time.items()
            if time.time() < next_fetch
        ]

        while not await self.urls_to_visit.empty():
            urls = await self.urls_to_visit.pop(count=self.concurrent_task_limit, exclude_domains=exclude_domains)

            if not urls:
                break  # Exit loop if no URLs are available

            for url_item in urls:
                yield url_item


    async def fetch_and_process_url(self, url_item: URLItem, session: aiohttp.ClientSession) -> None:
        """
        Fetch the content of a single URL and process it.
        """
        if not url_item:
            return

        try:
            text, mime, last_modified = await self.fetch_content(session, url_item)
            if text:
                await self.process_content(url_item, text, mime)
            else:
                await self.urls_to_visit.update_error(url_item.url, "Empty content or unsupported status code.")
                logger.error(f"Empty content for {url_item.url}")
        except Exception as e:
            logger.exception(f"Error processing {url_item.url}: {e}")
            await self.urls_to_visit.update_error(url_item.url, str(e))

    async def crawl_start(self):
        """
        Entry point: Initialize everything and start crawling.
        """
        await self.start_node()  # Start the Node.js parser process

        os.makedirs(os.path.dirname(self.file_db), exist_ok=True)
        async with aiosqlite.connect(self.file_db) as self.conn:
            self.conn.row_factory = aiosqlite.Row
            await self.initialize_db()
            await self.initialize_sql_table()
            await self.initialize_output_csv()
            await self.initialize_urls()

            print("Initialization complete.")
            task_related = asyncio.create_task(self._scrape_related_domains())
            print("Crawling started.")

            loop = asyncio.get_running_loop()
            stop_event = asyncio.Event()

            crawl_task = asyncio.create_task(self.crawl_main())
            stop_event_task = asyncio.create_task(stop_event.wait())

            done, pending = await asyncio.wait(
                [crawl_task, stop_event_task, task_related], return_when=asyncio.FIRST_COMPLETED
            )
            # Graceful shutdown or additional handling can go here
