from urllib.parse import urlparse, urlunparse, quote, unquote
import aiosqlite
import asyncio
import datetime
import logging
from typing import Optional

# Initialize logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Define change frequency intervals
changefreq_intervals = {
    "always": datetime.timedelta(minutes=5),
    "hourly": datetime.timedelta(hours=1),
    "daily": datetime.timedelta(days=1),
    "weekly": datetime.timedelta(weeks=1),
    "monthly": datetime.timedelta(days=30),
    "yearly": datetime.timedelta(days=365),
    "never": None,  # Use None to indicate that the page should not be revisited
}


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
        status: str = "unseen",  # Status can be 'unseen', 'processing', 'seen', 'revisiting'
        db = None
    ):
        
        self.db = db
        
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

    def to_dict(self):
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
        }

    @staticmethod
    def from_row(row):
        return URLItem(
            url=row[0],
            url_score=row[1],
            page_score=row[2],
            page_type=row[3],
            lastmod_sitemap=row[4],
            changefreq_sitemap=row[5],
            priority_sitemap=row[6],
            time_last_visited=row[7],
            next_revisit_time=row[8],  # Adjusted index for new field
            revisit_count=row[9],
            flair=row[10],
            reddit_score=row[11],
            error=row[12],
            changefreq=row[13],
            lastmod=row[14],
            priority=row[15],
            status=row[16],
        )


class BackedURLQueue:
    def __init__(self, conn, table_name="url_metadata"):
        self.conn = conn
        self.table_name = table_name
        self.processed_urls = 0
        self.start_time = datetime.datetime.now()
        self.revisit_event = asyncio.Event()

    async def _get_connection(self):
        return self.conn

    async def initialize(self):
        """Initialize the database table and reload any necessary data."""
        await self._setup_db(self.conn)
        await self.reload(self.conn)  # Pass the connection to reload

    async def _setup_db(self, conn):
        await conn.execute(
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
                revisit_count INTEGER DEFAULT 0,
                flair TEXT,
                reddit_score REAL,
                error TEXT,
                changefreq TEXT,
                lastmod TEXT,
                priority REAL,
                status TEXT DEFAULT 'unseen',
                next_revisit_time TEXT
            );
            """
        )
        # Add indexes for performance
        await conn.execute(
            f"CREATE INDEX IF NOT EXISTS idx_status_url_score ON {self.table_name} (status, url_score DESC);"
        )
        await conn.execute(
            f"CREATE INDEX IF NOT EXISTS idx_next_revisit_time ON {self.table_name} (next_revisit_time);"
        )
        await conn.execute(
            f"CREATE INDEX IF NOT EXISTS idx_url ON {self.table_name} (url);"
        )
        
        await conn.execute(
            f"CREATE TABLE IF NOT EXISTS {self.table_name}_canonical_url (url TEXT PRIMARY KEY, canonical_url TEXT);"
        )
        await conn.commit()
    
    async def get(self,url):
        url = normalize_url(url)
        cursor = await self.conn.execute(
            f"""
            SELECT url, url_score, page_score, page_type, lastmod_sitemap, changefreq_sitemap,
                   priority_sitemap, time_last_visited, next_revisit_time, revisit_count, flair, reddit_score, error,
                   changefreq, lastmod, priority, status
            FROM {self.table_name}
            WHERE url = ?
            """,
            (url,),
        )
        row = await cursor.fetchone()
        await cursor.close()
        if row:
            return URLItem.from_row(row)
        return None

    async def push(self, url_item: URLItem):
        """Add a new URLItem to the queue if it doesn't already exist, or update it if it does."""
        try:
            # Normalize the URL to ensure consistent formatting
            url_item.url = normalize_url(url_item.url)

            # Convert URLItem instance to a dictionary to prepare for insertion or update
            data = url_item.to_dict()
            placeholders = ", ".join(["?"] * len(data))  # Placeholder for each field
            columns = ", ".join(data.keys())             # Column names for the SQL query
            values = tuple(data.values())                # Actual values to insert or update

            # Check if the URL already exists in the database
            cursor = await self.conn.execute(
                f"SELECT 1 FROM {self.table_name} WHERE url = ?", (url_item.url,)
            )
            exists = await cursor.fetchone()
            await cursor.close()

            if not exists:
                # If URL does not exist, insert a new entry
                await self.conn.execute(
                    f"""
                    INSERT INTO {self.table_name} ({columns})
                    VALUES ({placeholders})
                    """,
                    values,
                )

            await self.conn.commit()

        except aiosqlite.Error as e:
            # Log the error if insertion or update fails
            logger.error(f"Failed to push or update URL {url_item.url}: {e}")


    async def pop(self) -> Optional[URLItem]:
        """Retrieve and return the next URLItem to process."""
        try:
            cursor = await self.conn.execute(
                f"""
                    SELECT url, url_score, page_score, page_type, lastmod_sitemap, changefreq_sitemap,
                           priority_sitemap, time_last_visited, next_revisit_time, revisit_count, flair, reddit_score, error,
                           changefreq, lastmod, priority, status
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
                    f"UPDATE {self.table_name} SET status = 'processing' WHERE url = ?",
                    (url_item.url,),
                )
                await self.conn.commit()
                url_item.status = "processing"
            else:
                url_item = None
            return url_item
        except aiosqlite.Error as e:
            logger.error(f"SQLite error during pop: {e}")
            return None

    async def url_known(self,url) -> int:
        try:
            totle = 0
            url = normalize_url(url)
            cursor = await self.conn.execute(
                f"SELECT 1 FROM {self.table_name} WHERE url = ?", (url,)
            )
            exists = await cursor.fetchone()
            await cursor.close()
            totle = 1 if exists else 0
            
            cursor = await self.conn.execute(
                f"SELECT 1 FROM {self.table_name} WHERE next_revisit_time = ?", (url,)
            )
            exists = await cursor.fetchone()
            await cursor.close()
            totle = 1 if exists else 0
            return totle
        except:
            pass
        
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
            logger.error("Failed to count seen URLs: {e}")
            return 0

    async def set_page_score(self, url: str, score: float):
        """Set the page score for a given URL."""
        try:
            url = normalize_url(url)
            await self.conn.execute(
                f"""
                    UPDATE {self.table_name}
                    SET page_score = ?
                    WHERE url = ?
                    """,
                (score, url),
            )
            await self.conn.commit()
        except aiosqlite.Error as e:
            logger.error(f"Failed to set page score for {url}: {e}")

    async def mark_error(self, url: str, status: str):
        """Mark a URL as having an error."""
        try:
            url = normalize_url(url)
            await self.conn.execute(
                f"""
                    UPDATE {self.table_name}
                    SET status = 'error', error = ?
                    WHERE url = ?
                    """,
                (status, url),
            )
            await self.conn.commit()
        except aiosqlite.Error as e:
            logger.error(f"Failed to mark error for {url}: {e}")

    async def mark_seen(self, url: str):
        """Mark a URL as seen and processed, and calculate the next revisit time."""
        try:
            url = normalize_url(url)
            time_now = datetime.datetime.utcnow()
            time_last_visited_str = time_now.strftime("%Y-%m-%d %H:%M:%S")

            # Fetch the change frequency from the database
            cursor = await self.conn.execute(
                f"SELECT changefreq_sitemap, changefreq FROM {self.table_name} WHERE url = ?",
                (url,),
            )
            row = await cursor.fetchone()
            await cursor.close()
            if row:
                changefreq_sitemap, changefreq = row
            else:
                changefreq_sitemap, changefreq = None, None

            # Determine the revisit interval
            changefreq_value = (changefreq_sitemap or changefreq or "daily").lower()
            revisit_interval = changefreq_intervals.get(
                changefreq_value, datetime.timedelta(days=1)
            )

            if revisit_interval:
                next_revisit_time = time_now + revisit_interval
                next_revisit_time_str = next_revisit_time.strftime("%Y-%m-%d %H:%M:%S")
            else:
                next_revisit_time_str = None  # For 'never' changefreq

            await self.conn.execute(
                f"""
                    UPDATE {self.table_name}
                    SET status = 'seen', time_last_visited = ?, next_revisit_time = ?
                    WHERE url = ?
                    """,
                (time_last_visited_str, next_revisit_time_str, url),
            )
            await self.conn.commit()
            self.processed_urls += 1
        except aiosqlite.Error as e:
            logger.error(f"Failed to mark {url} as seen: {e}")

    # In BackedURLQueue class
    async def update_error(self, url: str, error_message: str):
        """Log an error message for a specific URL in the database."""
        url = normalize_url(url)
        await self.conn.execute(
            f"""
            UPDATE {self.table_name}
            SET status = 'error', error = ?
            WHERE url = ?
            """,
            (error_message, url),
        )
        await self.conn.commit()

    async def mark_processing(self, url: str):
        """Mark a URL as currently being processed."""
        try:
            url = normalize_url(url)
            await self.conn.execute(
                f"UPDATE {self.table_name} SET status = 'processing' WHERE url = ?",
                (url,),
            )
            await self.conn.commit()
        except aiosqlite.Error as e:
            logger.error(f"Failed to mark {url} as processing: {e}")

    async def reload(self, conn=None):
        """Reset the processing status of URLs that were being processed when the crawler stopped."""
        try:
            if conn is None:
                await self.conn.execute(
                    f"""
                        UPDATE {self.table_name}
                        SET status = 'unseen'
                        WHERE status = 'processing'
                        """
                )
                await self.conn.commit()
            else:
                await self.conn.execute(
                    f"""
                    UPDATE {self.table_name}
                    SET status = 'unseen'
                    WHERE status = 'processing'
                    """
                )
                await self.conn.commit()
        except aiosqlite.Error as e:
            logger.error(f"Failed to reload URLs: {e}")

    async def have_been_seen(self, url: str) -> bool:
        """Check if a URL has already been seen."""
        try:
            url = normalize_url(url)
            cursor = await self.conn.execute(
                f"SELECT status FROM {self.table_name} WHERE url = ?",
                (url,),
            )
            result = await cursor.fetchone()
            await cursor.close()
            if result:
                return result[0] in ["processing", "seen", "revisiting"]
            return False
        except aiosqlite.Error as e:
            logger.error(f"Failed to check if {url} has been seen: {e}")
            return False
    
    async def count_processing(self) -> int:
        """Return the number of URLs that are currently being processed."""
        try:
            cursor = await self.conn.execute(
                f"SELECT COUNT(*) FROM {self.table_name} WHERE status = 'processing'"
            )
            result = await cursor.fetchone()
            await cursor.close()
            return result[0] if result else 0
        except aiosqlite.Error as e:
            logger.error("Failed to count processing URLs: {e}")
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
            logger.error("Failed to count unseen URLs: {e}")
            return 0

    async def update_status(self, url: str, status: str):
        """Update the status of a URL in the database."""
        try:
            url = normalize_url(url)  # Ensure the URL is normalized before updating
            await self.conn.execute(
                f"""
                    UPDATE {self.table_name}
                    SET status = ?
                    WHERE url = ?
                    """,
                (status, url),
            )
            await self.conn.commit()
        except aiosqlite.Error as e:
            logger.error(f"Failed to update status for {url} to '{status}': {e}")

    async def count_all(self) -> int:
        """Return the total number of URLs in the queue."""
        try:
            cursor = await self.conn.execute(f"SELECT COUNT(*) FROM {self.table_name}")
            result = await cursor.fetchone()
            await cursor.close()
            return result[0] if result else 0
        except aiosqlite.Error as e:
            logger.error("Failed to count all URLs: {e}")
            return 0

    async def empty(self) -> bool:
        """Check if the queue is empty."""
        unseen_count = await self.count_unseen()
        return unseen_count == 0

    async def reset(self):
        """Reset the status of all URLs to 'unseen'."""
        try:
            await self.conn.execute(f"UPDATE {self.table_name} SET status = 'unseen'")
            await self.conn.commit()
        except aiosqlite.Error as e:
            logger.error(f"Failed to reset the queue: {e}")

    async def mark_revisit(self, url: str):
        """Mark a URL as needing to be revisited."""
        try:
            url = normalize_url(url)
            await self.conn.execute(
                f"""
                    UPDATE {self.table_name}
                    SET status = 'revisiting'
                    WHERE url = ?
                    """,
                (url,),
            )
            await self.conn.commit()
            self.revisit_event.set()  # Signal that a URL is ready for revisiting
        except aiosqlite.Error as e:
            logger.error(f"Failed to mark {url} for revisit: {e}")

    async def pop_revisit(self):
        """Asynchronously yield URLItems that need to be revisited."""
        while True:
            try:
                now_str = datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
                cursor = await self.conn.execute(
                    f"""
                        SELECT url, url_score, page_score, page_type, lastmod_sitemap, changefreq_sitemap,
                               priority_sitemap, time_last_visited, next_revisit_time, revisit_count, flair, reddit_score, error,
                               changefreq, lastmod, priority, status
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
                    # No URLs ready for revisiting, wait until one is marked
                    self.revisit_event.clear()
                await self.revisit_event.wait()
            except aiosqlite.Error as e:
                logger.error(f"SQLite error during pop_revisit: {e}")
                await asyncio.sleep(1)  # Wait before retrying to prevent tight loop
