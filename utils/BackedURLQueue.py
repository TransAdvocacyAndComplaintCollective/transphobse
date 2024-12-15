import asyncio
import json
from urllib.parse import urlparse, urlunparse, quote, unquote
import aiosqlite
import datetime
import logging
from typing import Any, Optional, Dict

# Initialize logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Define change frequency intervals
CHANGEFREQ_INTERVALS = {
    "always": datetime.timedelta(minutes=5),
    "hourly": datetime.timedelta(hours=1),
    "daily": datetime.timedelta(days=1),
    "weekly": datetime.timedelta(weeks=1),
    "monthly": datetime.timedelta(days=30),
    "yearly": datetime.timedelta(days=365),
    "never": None,  # Indicates that the page should not be revisited
}
def parse_no_vary_search_header(header_value: str) -> dict:
    """
    Parse the No-Vary-Search header value into a structured policy dict.
    This is a simplified parser that identifies key-order, params, and except directives.
    Example outputs:
    {
        "key-order": True,
        "params": True or ["param1", "param2"] or False,
        "except": ["param1", "param2"]
    }
    """
    policy = {
        "key-order": False,
        "params": False,
        "except": []
    }

    # Split by commas first
    parts = [p.strip() for p in header_value.split(',')]
    for part in parts:
        if part.startswith("key-order"):
            policy["key-order"] = True
        elif part.startswith("params"):
            # check if we have params=("param1" "param2") or just params
            if "=(" in part:
                # Extract parameters inside quotes
                start = part.find('=("') + 3
                end = part.rfind('")')
                if start > 2 and end > start:
                    param_str = part[start:end]
                    policy["params"] = param_str.split('" "')
            else:
                # boolean params
                policy["params"] = True
        elif part.startswith("except"):
            # except=("param1" "param2")
            start = part.find('=("') + 3
            end = part.rfind('")')
            if start > 2 and end > start:
                param_str = part[start:end]
                policy["except"] = param_str.split('" "')

    return policy

def normalize_url(url: str) -> str:
    """
    Standardize and clean URL format by enforcing https://, removing www prefix,
    handling web.archive.org links, and ensuring consistent formatting of the path.
    """
    try:
        # Handle web.archive.org URLs
        if "web.archive.org" in url:
            live_url_start = url.find("/https://")
            if live_url_start != -1:
                url = url[live_url_start + 1:]

        parsed = urlparse(url)
        scheme = "https"

        # Normalize hostname
        hostname = parsed.hostname.lower().replace("www.", "") if parsed.hostname else ""

        # Normalize path
        path = parsed.path
        if not path or not any(part.count('.') for part in path.split('/')):
            path = quote(unquote(path.rstrip("/"))) + "/"
        else:
            path = quote(unquote(path))

        # Normalize port
        port = (
            None
            if (scheme == "http" and parsed.port == 80) or (scheme == "https" and parsed.port == 443)
            else parsed.port
        )
        netloc = f"{hostname}:{port}" if port else hostname

        normalized = urlunparse((scheme, netloc, path, "", parsed.query, ""))
        return normalized
    except Exception as e:
        logger.error(f"Error normalizing URL {url}: {e}")
        return url  # Return original URL if normalization fails

class URLItem:
    def __init__(
        self,
        url: str,
        url_score: float = 0.0,
        page_score: float = 0.0,
        page_type: Optional[str] = None,
        lastmod_sitemap: Optional[str] = None,
        changefreq_sitemap: Optional[str] = None,
        priority_sitemap: Optional[float] = None,
        time_last_visited: Optional[str] = None,
        next_revisit_time: Optional[str] = None,
        revisit_count: int = 0,
        flair: Optional[str] = None,
        reddit_score: Optional[float] = None,
        error: Optional[str] = None,
        changefreq: Optional[str] = None,
        lastmod: Optional[str] = None,
        priority: Optional[float] = None,
        status: str = "unseen",
        etag: Optional[str] = None,
        last_modified: Optional[str] = None
    ):
        self.url = url
        self.url_score = url_score
        self.page_score = page_score
        self.page_type = page_type
        self.lastmod_sitemap = lastmod_sitemap
        self.changefreq_sitemap = changefreq_sitemap
        self.priority_sitemap = priority_sitemap
        self.time_last_visited = time_last_visited
        self.next_revisit_time = next_revisit_time
        self.revisit_count = revisit_count
        self.flair = flair
        self.reddit_score = reddit_score
        self.error = error
        self.changefreq = changefreq
        self.lastmod = lastmod
        self.priority = priority
        self.status = status
        self.etag = etag
        self.last_modified = last_modified

    def to_dict(self) -> Dict[str, Any]:
        return {
            "url": self.url,
            "url_score": self.url_score,
            "page_score": self.page_score,
            "page_type": self.page_type,
            "lastmod_sitemap": self.lastmod_sitemap,
            "changefreq_sitemap": self.changefreq_sitemap,
            "priority_sitemap": self.priority_sitemap,
            "time_last_visited": self.time_last_visited,
            "next_revisit_time": self.next_revisit_time,
            "revisit_count": self.revisit_count,
            "flair": self.flair,
            "reddit_score": self.reddit_score,
            "error": self.error,
            "changefreq": self.changefreq,
            "lastmod": self.lastmod,
            "priority": self.priority,
            "status": self.status,
            "etag": self.etag,
            "last_modified": self.last_modified
        }

    @staticmethod
    def from_row(row: aiosqlite.Row) -> 'URLItem':
        return URLItem(
            url=row["url"],
            url_score=row["url_score"],
            page_score=row["page_score"],
            page_type=row["page_type"],
            lastmod_sitemap=row["lastmod_sitemap"],
            changefreq_sitemap=row["changefreq_sitemap"],
            priority_sitemap=row["priority_sitemap"],
            time_last_visited=row["time_last_visited"],
            next_revisit_time=row["next_revisit_time"],
            revisit_count=row["revisit_count"],
            flair=row["flair"],
            reddit_score=row["reddit_score"],
            error=row["error"],
            changefreq=row["changefreq"],
            lastmod=row["lastmod"],
            priority=row["priority"],
            status=row["status"],
            etag=row["etag"],
            last_modified=row["last_modified"]
        )


class BackedURLQueue:
    def __init__(self, conn: aiosqlite.Connection, table_name: str = "urls_to_visit"):
        self.conn = conn
        self.table_name = table_name
        self.processed_urls = 0
        self.revisit_event = asyncio.Event()

    async def initialize(self) -> None:
        """Initialize the database table and indexes."""
        await self._setup_db()

    async def _setup_db(self) -> None:
        """Create the necessary tables and indexes."""
        try:
            
            # Create the no_vary_search_policies table
            await self.conn.execute(
                """
                CREATE TABLE IF NOT EXISTS no_vary_search_policies (
                    domain TEXT,
                    path TEXT,
                    policy TEXT,
                    PRIMARY KEY (domain, path)
                );
                """
            )
            await self.conn.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {self.table_name} (
                    url TEXT PRIMARY KEY,
                    url_score REAL,
                    page_score REAL,
                    page_type TEXT,
                    lastmod_sitemap TEXT,
                    changefreq_sitemap TEXT,
                    priority_sitemap REAL,
                    time_last_visited TEXT,
                    next_revisit_time TEXT,
                    revisit_count INTEGER DEFAULT 0,
                    flair TEXT,
                    reddit_score REAL,
                    error TEXT,
                    changefreq TEXT,
                    lastmod TEXT,
                    priority REAL,
                    status TEXT DEFAULT 'unseen',
                    etag TEXT,
                    last_modified TEXT
                );
                """
            )
            await self.conn.execute(
                f"CREATE INDEX IF NOT EXISTS idx_status_url_score ON {self.table_name} (status, url_score DESC);"
            )
            await self.conn.execute(
                f"CREATE INDEX IF NOT EXISTS idx_next_revisit_time ON {self.table_name} (next_revisit_time);"
            )
            await self.conn.execute(
                f"CREATE INDEX IF NOT EXISTS idx_url ON {self.table_name} (url);"
            )
            await self.conn.commit()
            logger.info(f"Database table '{self.table_name}' initialized successfully.")
        except aiosqlite.Error as e:
            logger.error(f"Error setting up database table '{self.table_name}': {e}")


    async def get(self, url: str) -> Optional[URLItem]:
        """Retrieve a URLItem by URL."""
        try:
            normalized_url = normalize_url(url)
            cursor = await self.conn.execute(
                f"""
                SELECT *
                FROM {self.table_name}
                WHERE url = ?
                """,
                (normalized_url,),
            )
            row = await cursor.fetchone()
            await cursor.close()
            if row:
                return URLItem.from_row(row)
            return None
        except aiosqlite.Error as e:
            logger.error(f"Error fetching URL '{url}': {e}")
            return None

    async def push(self, url_item: URLItem) -> None:
        """Insert or update a URLItem in the database."""
        try:
            data = url_item.to_dict()
            columns = ", ".join(data.keys())
            placeholders = ", ".join(["?"] * len(data))
            values = tuple(data.values())
            print(values)

            await self.conn.execute(
                f"""
                INSERT OR REPLACE INTO {self.table_name} ({columns})
                VALUES ({placeholders})
                """,
                values,
            )
            await self.conn.commit()
            logger.debug(f"Pushed URL to queue: {url_item.url}")
        except aiosqlite.Error as e:
            logger.error(f"Failed to push/update URL '{url_item.url}': {e}")

    async def pop(self) -> Optional[URLItem]:
        """Retrieve and mark the next unseen URLItem as processing."""
        try:
            cursor = await self.conn.execute(
                f"""
                SELECT *
                FROM {self.table_name}
                WHERE status = 'unseen'
                ORDER BY url_score DESC
                LIMIT 1
                """
            )
            row = await cursor.fetchone()
            await cursor.close()

            if row:
                url_item = URLItem.from_row(row)
                await self.conn.execute(
                    f"""
                    UPDATE {self.table_name}
                    SET status = 'processing'
                    WHERE url = ?
                    """,
                    (url_item.url,),
                )
                await self.conn.commit()
                logger.debug(f"Popped URL for processing: {url_item.url}")
                return url_item
            return None
        except aiosqlite.Error as e:
            logger.error(f"SQLite error during pop: {e}")
            return None

    async def url_known(self, url: str) -> bool:
        """Check if a URL is already known in the queue."""
        try:
            normalized_url = normalize_url(url)
            cursor = await self.conn.execute(
                f"SELECT 1 FROM {self.table_name} WHERE url = ? LIMIT 1",
                (normalized_url,),
            )
            exists = await cursor.fetchone()
            await cursor.close()
            return bool(exists)
        except aiosqlite.Error as e:
            logger.error(f"Error checking if URL is known '{url}': {e}")
            return False

    async def count_seen(self) -> int:
        """Return the number of URLs that have been seen."""
        try:
            cursor = await self.conn.execute(
                f"SELECT COUNT(*) FROM {self.table_name} WHERE status = 'seen'"
            )
            result = await cursor.fetchone()
            await cursor.close()
            return result[0] if result else 0
        except aiosqlite.Error as e:
            logger.error(f"Failed to count seen URLs: {e}")
            return 0

    async def set_page_score(self, url: str, score: float) -> None:
        """Set the page score for a given URL."""
        try:
            normalized_url = normalize_url(url)
            await self.conn.execute(
                f"""
                UPDATE {self.table_name}
                SET page_score = ?
                WHERE url = ?
                """,
                (score, normalized_url),
            )
            await self.conn.commit()
            logger.debug(f"Set page score for URL '{url}' to {score}")
        except aiosqlite.Error as e:
            logger.error(f"Failed to set page score for '{url}': {e}")

    async def mark_error(self, url: str, status: str) -> None:
        """Mark a URL as having an error."""
        try:
            normalized_url = normalize_url(url)
            await self.conn.execute(
                f"""
                UPDATE {self.table_name}
                SET status = 'error', error = ?
                WHERE url = ?
                """,
                (status, normalized_url),
            )
            await self.conn.commit()
            logger.debug(f"Marked URL '{url}' as error with status '{status}'")
        except aiosqlite.Error as e:
            logger.error(f"Failed to mark error for '{url}': {e}")

    async def mark_seen(self, url: str) -> None:
        """Mark a URL as seen and calculate the next revisit time."""
        try:
            normalized_url = normalize_url(url)
            time_now = datetime.datetime.utcnow()
            time_last_visited_str = time_now.strftime("%Y-%m-%d %H:%M:%S")

            # Fetch change frequency
            cursor = await self.conn.execute(
                f"SELECT changefreq_sitemap, changefreq FROM {self.table_name} WHERE url = ?",
                (normalized_url,),
            )
            row = await cursor.fetchone()
            await cursor.close()

            changefreq_sitemap, changefreq = row if row else (None, None)
            changefreq_value = (changefreq_sitemap or changefreq or "daily").lower()
            revisit_interval = CHANGEFREQ_INTERVALS.get(changefreq_value, datetime.timedelta(days=1))

            if revisit_interval:
                next_revisit_time = time_now + revisit_interval
                next_revisit_time_str = next_revisit_time.strftime("%Y-%m-%d %H:%M:%S")
            else:
                next_revisit_time_str = None  # For 'never'

            await self.conn.execute(
                f"""
                UPDATE {self.table_name}
                SET status = 'seen',
                    time_last_visited = ?,
                    next_revisit_time = ?
                WHERE url = ?
                """,
                (time_last_visited_str, next_revisit_time_str, normalized_url),
            )
            await self.conn.commit()
            self.processed_urls += 1
            logger.debug(f"Marked URL '{url}' as seen.")
        except aiosqlite.Error as e:
            logger.error(f"Failed to mark '{url}' as seen: {e}")

    async def update_error(self, url: str, error_message: str) -> None:
        """Log an error message for a specific URL in the database."""
        await self.mark_error(url, error_message)
    
    async def get_no_vary_search_policy(self, domain: str, path: str) -> Optional[dict]:
        """
        Retrieve the No-Vary-Search policy for a given domain and path.
        Returns the policy as a dictionary, or None if no policy exists.
        """
        try:
            cursor = await self.conn.execute(
                """
                SELECT policy
                FROM no_vary_search_policies
                WHERE domain = ? AND path = ?
                """,
                (domain, path),
            )
            row = await cursor.fetchone()
            await cursor.close()

            if row:
                policy_json = row[0]
                return json.loads(policy_json)  # Convert JSON string back to a dictionary
            else:
                return None  # No policy found
        except aiosqlite.Error as e:
            logger.error(f"Failed to retrieve No-Vary-Search policy for {domain}{path}: {e}")
            return None


    async def mark_revisit(self, url: str) -> None:
        """Mark a URL as needing to be revisited."""
        try:
            normalized_url = normalize_url(url)
            await self.conn.execute(
                f"""
                UPDATE {self.table_name}
                SET status = 'revisiting'
                WHERE url = ?
                """,
                (normalized_url,),
            )
            await self.conn.commit()
            self.revisit_event.set()  # Signal that a URL is ready for revisiting
            logger.debug(f"Marked URL '{url}' for revisit.")
        except aiosqlite.Error as e:
            logger.error(f"Failed to mark '{url}' for revisit: {e}")

    async def pop_revisit(self) -> Optional[URLItem]:
        """Yield URLItems that need to be revisited asynchronously."""
        while True:
            try:
                now_str = datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
                cursor = await self.conn.execute(
                    f"""
                    SELECT *
                    FROM {self.table_name}
                    WHERE status = 'seen' AND
                          next_revisit_time IS NOT NULL AND
                          next_revisit_time <= ?
                    ORDER BY page_score DESC
                    LIMIT 1
                    """,
                    (now_str,),
                )
                row = await cursor.fetchone()
                await cursor.close()

                if row:
                    url_item = URLItem.from_row(row)
                    await self.conn.execute(
                        f"UPDATE {self.table_name} SET status = 'processing' WHERE url = ?",
                        (url_item.url,),
                    )
                    await self.conn.commit()
                    url_item.status = "processing"
                    yield url_item
                else:
                    self.revisit_event.clear()
                await self.revisit_event.wait()
            except aiosqlite.Error as e:
                logger.error(f"SQLite error during pop_revisit: {e}")
                await asyncio.sleep(1)  # Prevent tight loop on error

    async def have_been_seen(self, url: str) -> bool:
        """Check if a URL has already been seen."""
        return await self.url_known(url)

    async def count_processing(self) -> int:
        """Return the number of URLs currently being processed."""
        try:
            cursor = await self.conn.execute(
                f"SELECT COUNT(*) FROM {self.table_name} WHERE status = 'processing'"
            )
            result = await cursor.fetchone()
            await cursor.close()
            return result[0] if result else 0
        except aiosqlite.Error as e:
            logger.error(f"Failed to count processing URLs: {e}")
            return 0

    async def count_unseen(self) -> int:
        """Return the number of URLs that have not been seen yet."""
        try:
            cursor = await self.conn.execute(
                f"SELECT COUNT(*) FROM {self.table_name} WHERE status = 'unseen'"
            )
            result = await cursor.fetchone()
            await cursor.close()
            return result[0] if result else 0
        except aiosqlite.Error as e:
            logger.error(f"Failed to count unseen URLs: {e}")
            return 0

    async def update_status(self, url: str, status: str) -> None:
        """Update the status of a URL in the database."""
        try:
            normalized_url = normalize_url(url)
            await self.conn.execute(
                f"""
                UPDATE {self.table_name}
                SET status = ?
                WHERE url = ?
                """,
                (status, normalized_url),
            )
            await self.conn.commit()
            logger.debug(f"Updated status for URL '{url}' to '{status}'.")
        except aiosqlite.Error as e:
            logger.error(f"Failed to update status for '{url}' to '{status}': {e}")

    async def count_all(self) -> int:
        """Return the total number of URLs in the queue."""
        try:
            cursor = await self.conn.execute(f"SELECT COUNT(*) FROM {self.table_name}")
            result = await cursor.fetchone()
            await cursor.close()
            return result[0] if result else 0
        except aiosqlite.Error as e:
            logger.error(f"Failed to count all URLs: {e}")
            return 0

    async def empty(self) -> bool:
        """Check if the queue is empty."""
        unseen_count = await self.count_unseen()
        return unseen_count == 0

    async def reset(self) -> None:
        """Reset the status of all URLs to 'unseen'."""
        try:
            await self.conn.execute(f"UPDATE {self.table_name} SET status = 'unseen'")
            await self.conn.commit()
            logger.info("Reset all URLs to 'unseen'.")
        except aiosqlite.Error as e:
            logger.error(f"Failed to reset the queue: {e}")

    async def update_etag_and_last_modified(self, url: str, etag: Optional[str], last_modified: Optional[str]) -> None:
        """Update the ETag and Last-Modified values for a given URL."""
        try:
            normalized_url = normalize_url(url)
            await self.conn.execute(
                f"""
                UPDATE {self.table_name}
                SET etag = ?, last_modified = ?
                WHERE url = ?
                """,
                (etag, last_modified, normalized_url),
            )
            await self.conn.commit()
            logger.debug(f"Updated ETag and Last-Modified for URL '{url}'")
        except aiosqlite.Error as e:
            logger.error(f"Failed to update ETag and Last-Modified for '{url}': {e}")


    async def store_no_vary_search_policy(self, domain: str, path: str, policy: dict) -> None:
        """
        Store a No-Vary-Search policy for a given domain and path.
        Policy is stored as a JSON string.
        """
        try:
            policy_json = json.dumps(policy)
            await self.conn.execute(
                """
                INSERT OR REPLACE INTO no_vary_search_policies (domain, path, policy)
                VALUES (?, ?, ?)
                """,
                (domain, path, policy_json),
            )
            await self.conn.commit()
            logger.debug(f"Stored No-Vary-Search policy for {domain}{path}")
        except aiosqlite.Error as e:
            logger.error(f"Failed to store No-Vary-Search policy for {domain}{path}: {e}")


    async def apply_no_vary_search_policy(self, url: str, no_vary_search: str):
        """
        Parse the No-Vary-Search policy and store it in the DB for the given domain and path.
        We'll store policies per (domain, path) basis.
        """
        # Parse the URL
        parsed = urlparse(url)
        domain = parsed.hostname.lower().replace("www.", "") if parsed.hostname else ""
        # We'll use the path as-is for specificity. If you want a more general approach,
        # consider truncating the path to a certain level.
        path = parsed.path or ""

        policy = parse_no_vary_search_header(no_vary_search)
        await self.store_no_vary_search_policy(domain, path, policy)



    async def reload(self) -> None:
        """Reset the processing status of URLs that were being processed when the crawler stopped."""
        try:
            await self.conn.execute(
                f"""
                UPDATE {self.table_name}
                SET status = 'unseen'
                WHERE status = 'processing'
                """
            )
            await self.conn.commit()
            logger.info("Reloaded URLs by resetting 'processing' to 'unseen'.")
        except aiosqlite.Error as e:
            logger.error(f"Failed to reload URLs: {e}")

    async def close(self) -> None:
        """Close any resources if needed."""
        self.revisit_event.set()  # Wake up any waiting coroutines
        logger.info("Closed BackedURLQueue.")
    async def clear(self) -> None:
        """Clear the URL queue."""
        try:
            await self.conn.execute(f"DELETE FROM {self.table_name}")
            await self.conn.commit()
            logger.info("Cleared URL queue.")
        except aiosqlite.Error as e:
            logger.error(f"Failed to clear the URL queue: {e}")
