import asyncio
from collections import defaultdict
import os
import logging
import csv
import json
import random
import socket
import ssl
import time
from urllib.parse import urljoin, urlparse, urlunparse, quote, unquote

import aiohttp
import aiosqlite
import certifi
from bs4 import BeautifulSoup, SoupStrainer
import feedparser
import chardet
from concurrent.futures import ThreadPoolExecutor
import tqdm

# Local imports
from ffffff import lookup_keyword
from utils.bbc_scripe_cdx import get_all_urls_cdx
from utils.duckduckgo import lookup_duckduckgos
import utils.keywords as kw
from utils.RobotsSql import RobotsSql
from utils.SQLDictClass import SQLDictClass
from utils.BackedURLQueue import BackedURLQueue, URLItem
import utils.keywords_finder as kw_finder
from utils.ovarit import ovarit_domain_scrape
from utils.reddit import reddit_domain_scrape
from javascript import require

processArticle = require("./utils/ProcessArticle.js")

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

def normalize_url(url: str) -> str:
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



class KeyPhraseFocusCrawler:
    def __init__(
        self,
        start_urls,
        feeds,
        name,
        keywords=kw.KEYWORDS,
        anti_keywords=kw.ANTI_KEYWORDS,
        plugins=[],
        max_limit=100,  # Adjusted for optimal concurrency
        allowed_subdirs_cruel=None,
        start_data=None,
        end_data=None,
        exclude_lag=None,
        exclude_subdirs_cruel=None,
    ):
        self.start_urls = start_urls
        self.feeds = feeds
        self.name = name
        self.keywords = keywords
        self.anti_keywords = anti_keywords
        self.keypaceFinder = kw_finder.KeypaceFinder(keywords)
        self.plugins = plugins
        self.output_csv = f"data/{name}.csv"
        self.file_db = f"databases/{name}.db"
        self.semaphore = asyncio.Semaphore(max_limit)
        self.thread_pool = ThreadPoolExecutor(max_workers=10)
        self.conn = None
        self.robots = None
        self.urls_to_visit = None
        self.allowed_subdirs_cruel = allowed_subdirs_cruel or []
        self.start_data = start_data
        self.end_data = end_data
        self.exclude_lag = exclude_lag or []
        self.exclude_subdirs_cruel = exclude_subdirs_cruel or []
        self.csv_rows = []  # For batching CSV writes
        self.lock = asyncio.Lock()

    async def initialize_db(self):
        """Initialize the database and required tables."""
        self.robots = RobotsSql(self.conn, self.thread_pool)
        self.urls_to_visit = BackedURLQueue(self.conn, table_name="urls_to_visit")
        await self.urls_to_visit.initialize()
        await self.robots.initialize()

    def _initialize_output_csv(self):
        os.makedirs(os.path.dirname(self.output_csv), exist_ok=True)
        fieldnames = [
            "Root Domain",
            "URL",
            "Score",
            "Keywords Found",
            "Headline",
            "name",
            "Date Published",
            "Date Modified",
            "Author",
            "Byline",
            "type",
            "crawled at",
        ]
        if not os.path.exists(self.output_csv) or os.path.getsize(self.output_csv) == 0:
            with open(
                self.output_csv, mode="w", newline="", buffering=1, encoding="utf-8"
            ) as csv_file:
                csv_writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
                csv_writer.writeheader()

    async def _initialize_urls(self):
        if await self.urls_to_visit.count_seen() > 0:
            logger.info("URLs have already been initialized.")
            return

        # for url in self.start_urls:
        #     await self._add_to_queue(url, "start", priority=0)
        #     for new_url in self._scrape_related_domains(url):
        #         await self._add_to_queue(new_url[0], "webpage", priority=new_url[1])


        for url in self.start_urls:
            await self._add_to_queue(url, "webpage", priority=0)
        for feed in self.feeds:
            await self._add_to_queue(feed, "feed", priority=0)

    def _scrape_related_domains(self, url):
        o = urlparse(url)
        scraped_urls = (
            list(reddit_domain_scrape(o.hostname))
            + list(ovarit_domain_scrape(o.hostname))
            + list(get_all_urls_cdx(o.hostname))
        )
        return [
            (link[0], self.keypaceFinder.relative_keywords_score(link[1])[0])
            for link in scraped_urls
        ]

    async def pre_seed(self):
        for url in self.start_urls:
            for keyword in self.keywords:
                await lookup_keyword(keyword, url)

    async def _add_to_queue(self, url, url_type, priority=0):
        normalized_url = normalize_url(url)

        if (
            not self.is_valid_url(normalized_url)
            or await self.urls_to_visit.have_been_seen(normalized_url)
        ):
            return

        if not any(
            normalized_url.startswith(subdir) for subdir in self.allowed_subdirs_cruel
        ) or any(
            normalized_url.startswith(subdir) for subdir in self.exclude_subdirs_cruel
        ):
            return

        robot = await self.robots.get(normalized_url)
        if robot and not robot.can_fetch("*", normalized_url):
            return

        url_item = URLItem(
            url=normalized_url, url_score=priority, page_type=url_type, status="unseen"
        )
        await self.urls_to_visit.push(url_item)

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
                            f"Failed to fetch {url}, retrying... ({attempt + 1}/{retries})"
                        )
                        attempt = attempt + 1
            except Exception as e:
                if await check_internet():
                    attempt = attempt + 1
                    logger.error(
                        f"Error fetching {url}: {e}, retrying... ({attempt + 1}/{retries})"
                    )
                    await asyncio.sleep(2 ** attempt + random.random())
                else:
                    logger.error(f"Internet connection lost while fetching {url}")
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
            # Use SoupStrainer with parse_only to limit parsing to necessary elements
            bs = BeautifulSoup(text, "html.parser")

            # Ensure bs is a BeautifulSoup object to avoid 'is_xml' issues
            if not isinstance(bs, BeautifulSoup):
                raise TypeError(f"Unexpected object type {type(bs)} for URL: {url_item.url}")

            # Extract metadata for the current URL
            metadata = await self.extract_metadata(bs, text, url_item.url, metadata)
            content_to_score = bs.get_text(separator=" ")
            lang = "unknown"  # Default if not found

            # Process the article content with the external processArticle module
            async with self.lock:
                try:
                    db_cat = processArticle.ProcessArticle(text, url_item.url)
                    if db_cat and hasattr(db_cat, 'content') and db_cat.content:
                        content_to_score = db_cat.content
                    else:
                        content_to_score = text
                    lang = db_cat.lang if db_cat and db_cat.lang else bs.html.get("lang", "unknown")
                except Exception as e:
                    logger.error(f"Error in processArticle for {url_item.url}: {e}")
                    content_to_score = text

            # Extract text, compute score, and find keywords
            try:
                bs_content = await loop.run_in_executor(
                    self.thread_pool, BeautifulSoup, content_to_score, "html.parser"
                )
                text_content = bs_content.get_text(separator=" ")
                score, keywords, _ = self.keypaceFinder.relative_keywords_score(text_content)
            except Exception as e:
                logger.error(f"Error computing score for {url_item.url}: {e}")
                score, keywords = 0, []

            # Extract and queue links for further crawling
            await self.extract_and_queue_links(bs, url_item.url, score)

            # Check the language and canonical URL before finalizing
            if lang not in {"en_GB", "en_US"}:
                for link in bs.find_all("link", rel=lambda x: x and "canonical" in x):
                    canonical_href = link.get("href")
                    if canonical_href:
                        url_normalized_canonical = normalize_url(canonical_href)
                        if url_item.url != url_normalized_canonical:
                            await self._add_to_queue(
                                url_normalized_canonical, "webpage", priority=score
                            )
                            return  # Exit processing for the original URL

            # If the score is positive, update the score and save to CSV
            if score > 0:
                await self.urls_to_visit.set_page_score(url_item.url, score)
                self.collect_csv_row(url_item, score, keywords, metadata)

        except Exception as e:
            logger.error(f"Error in analyze_webpage_content for {url_item.url}: {e}")
            await self.urls_to_visit.update_error(url_item.url, str(e))


    async def process_feed(self, text, url):
        feed = feedparser.parse(text)
        for entry in feed.entries:
            score, keywords, _ = self.keypaceFinder.relative_keywords_score(
                entry.title
            )
            await self._add_to_queue(entry.link, "webpage", priority=score)

    async def process_sitemap(self, text, url):
        bs = BeautifulSoup(text, "html.parser")
        urls = [loc.text for loc in bs.find_all("loc")]
        for link in urls:
            await self._add_to_queue(link, "webpage")

    async def extract_metadata(self, bs, text, url, metadata={}):
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
            await self.urls_to_visit.update_error(
                url, f"Metadata extraction failed: {e}"
            )
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
        links = set(a["href"] for a in bs.find_all("a", href=True))
        for link in links:
            full_url = urljoin(url, link)
            await self._add_to_queue(full_url, "webpage", priority=score)

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

    async def _crawl_loop(self, session):
        loop = asyncio.get_event_loop()
        max_count = await self.urls_to_visit.count_all()
        done_count = await self.urls_to_visit.count_seen()
        with tqdm.tqdm(
            total=max_count, initial=done_count, desc="Crawling Progress", unit="url"
        ) as pbar:
            tasks = set()

            async for url_item in self._url_generator():
                max_count = await self.urls_to_visit.count_all()
                pbar.total = max_count
                task = loop.create_task(self.fetch_and_process_url(url_item, session))
                tasks.add(task)

                if len(tasks) >= self.semaphore._value:
                    _done, tasks = await asyncio.wait(
                        tasks, return_when=asyncio.FIRST_COMPLETED
                    )

                pbar.update(1)

            if tasks:
                await asyncio.wait(tasks)

            self.write_to_csv()

    async def fetch_and_process_url(self, url_item, session):
        if not url_item:
            return

        async with self.semaphore:
            try:
                text, mime = await self.fetch_content(session, url_item)
                if text:
                    await self.process_content(url_item, text, mime)
                else:
                    logger.error(f"Empty content for {url_item.url}")
            except Exception as e:
                logger.error(f"Error processing {url_item.url}: {e}")

    async def crawl_start(self):
        os.makedirs(os.path.dirname(self.file_db), exist_ok=True)
        async with aiosqlite.connect(self.file_db) as self.conn:
            self.conn.row_factory = aiosqlite.Row
            await self.initialize_db()
            self._initialize_output_csv()
            await self._initialize_urls()
            await self.crawl_main()
