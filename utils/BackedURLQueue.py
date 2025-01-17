import asyncio
import json
from urllib.parse import urlparse, urlunparse, quote, unquote
import aiosqlite
import datetime
import logging
from typing import Any, List, Optional, Dict

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


def calculate_revisit_time(
    changefreq: Optional[str],
    time_last_visited: Optional[str],
    lastmod_HTTP: Optional[str] = None
) -> Optional[datetime.datetime]:
    """
    Calculate the next revisit time (a datetime) based on the change frequency.
    We choose the earliest of: time_last_visited, lastmod_HTTP as the "anchor" point.
    Then we add the revisit interval to this earliest date.
    """

    # 1. If changefreq is missing, or "never", we can't calculate a revisit time
    if not changefreq:
        return None

    changefreq_value = changefreq.lower()
    revisit_interval = CHANGEFREQ_INTERVALS.get(changefreq_value, datetime.timedelta(days=1))
    if revisit_interval is None:  # e.g., "never"
        return None

    # 2. Helper to parse ISO8601 strings safely
    def _parse_iso(iso_str: Optional[str]) -> Optional[datetime.datetime]:
        if iso_str:
            try:
                return datetime.datetime.fromisoformat(iso_str)
            except ValueError:
                pass
        return None

    # 3. Parse the two optional date strings
    dt_last_visited = _parse_iso(time_last_visited)
    dt_http = _parse_iso(lastmod_HTTP)

    # 4. Filter out None values and pick the earliest
    candidates = [dt for dt in (dt_last_visited, dt_http) if dt is not None]
    if not candidates:
        # No valid dates to anchor from, so we can't compute next revisit
        return None

    earliest_dt = min(candidates)

    # 5. Add the interval to that earliest anchor date
    return earliest_dt + revisit_interval


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

    # Split by commas
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


def extract_domain(url: str) -> str:
    """
    Convenience function to parse out the domain from a normalized URL,
    removing 'www.' if present.
    """
    try:
        parsed = urlparse(url)
        return parsed.hostname.lower() if parsed.hostname else ""
    except Exception:
        return ""


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
        lastmod_html: Optional[str] = None,
        priority: Optional[float] = None,
        status: str = "unseen",
        etag: Optional[str] = None,
        last_modified: Optional[str] = None,
        domain: Optional[str] = None,
    ):
        self.url = url
        self.url_score = url_score
        self.page_score = page_score
        self.page_type = page_type
        self.lastmod_sitemap = lastmod_sitemap
        self.changefreq_sitemap = changefreq_sitemap
        self.priority_sitemap = priority_sitemap
        self.time_last_visited = time_last_visited
        self.next_revisit_time = calculate_revisit_time(changefreq, time_last_visited)
        self.revisit_count = revisit_count
        self.flair = flair
        self.reddit_score = reddit_score
        self.error = error
        self.changefreq = changefreq
        self.lastmod_html = lastmod_html
        self.priority = priority
        self.status = status
        self.etag = etag
        self.last_modified = last_modified
        self.domain = domain or extract_domain(url)

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
            "lastmod": self.lastmod_html,
            "priority": self.priority,
            "status": self.status,
            "etag": self.etag,
            "last_modified": self.last_modified,
            "domain": self.domain,
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
            lastmod_html=row["lastmod"],
            priority=row["priority"],
            status=row["status"],
            etag=row["etag"],
            last_modified=row["last_modified"],
            domain=row["domain"],
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
        """Create or update the necessary tables and indexes (including domain column)."""
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

            # Create the main table if it doesn't exist
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
                    last_modified TEXT,
                    domain TEXT
                );
                """
            )

            # Check if 'domain' column exists, if not, add it:
            columns_query = f"PRAGMA table_info({self.table_name});"
            cursor = await self.conn.execute(columns_query)
            columns_info = await cursor.fetchall()
            col_names = [col[1].lower() for col in columns_info]
            await cursor.close()

            if 'domain' not in col_names:
                await self.conn.execute(
                    f"ALTER TABLE {self.table_name} ADD COLUMN domain TEXT;"
                )

            # Create indexes
            await self.conn.execute(
                f"CREATE INDEX IF NOT EXISTS idx_status_url_score ON {self.table_name} (status, url_score DESC);"
            )
            await self.conn.execute(
                f"CREATE INDEX IF NOT EXISTS idx_next_revisit_time ON {self.table_name} (next_revisit_time);"
            )
            await self.conn.execute(
                f"CREATE INDEX IF NOT EXISTS idx_url ON {self.table_name} (url);"
            )
            await self.conn.execute(
                f"CREATE INDEX IF NOT EXISTS idx_domain ON {self.table_name} (domain);"
            )

            await self.conn.commit()
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
        """
        Insert or update a URLItem in the database.
        We also ensure the domain is parsed and stored.
        """
        try:
            # (Re)extract domain to be absolutely sure
            url_item.domain = extract_domain(url_item.url)

            data = url_item.to_dict()
            columns = ", ".join(data.keys())
            placeholders = ", ".join(["?"] * len(data))
            values = tuple(data.values())

            await self.conn.execute(
                f"""
                INSERT OR REPLACE INTO {self.table_name} ({columns})
                VALUES ({placeholders})
                """,
                values,
            )
            await self.conn.commit()
        except aiosqlite.Error as e:
            logger.error(f"Failed to push/update URL '{url_item.url}': {e}")

    async def super_pop(self, count: int = 1) -> Optional[List[URLItem]]:
        """
        Retrieve and mark the next batch of URLItems to process based on comprehensive priority rules.
        Combines the logic of pop and pop_revisit.

        Priority Order:
            1. Unseen URLs with highest max(url_score, page_score).
            2. Seen URLs due for revisiting based on next_revisit_time and other metrics.
            3. Any remaining URLs based on priority_sitemap or time_last_visited.

        Args:
            count (int): The number of URLs to retrieve.

        Returns:
            Optional[List[URLItem]]: A list of URLItems to process, or None if no URLs are available.
        """
        urls_to_process = []

        try:
            async with self.conn.execute("BEGIN"):
                # 1. Fetch unseen URLs
                unseen_query = f"""
                    SELECT *
                    FROM {self.table_name}
                    WHERE status = 'unseen'
                    ORDER BY MAX(url_score, page_score) DESC
                    LIMIT ?
                """
                cursor = await self.conn.execute(unseen_query, (count,))
                rows = await cursor.fetchall()
                for row in rows:
                    if len(urls_to_process) >= count:
                        break
                    url_item = URLItem.from_row(row)
                    urls_to_process.append(url_item)

                # Mark fetched unseen URLs as 'processing'
                if rows:
                    urls_normalized = [normalize_url(item.url) for item in urls_to_process]
                    await self.conn.executemany(
                        f"""
                        UPDATE {self.table_name}
                        SET status = 'processing'
                        WHERE url = ?
                        """,
                        [(url,) for url in urls_normalized]
                    )
                    await self.conn.commit()

                remaining = count - len(urls_to_process)
                if remaining <= 0:
                    return urls_to_process

                # 2. Fetch seen URLs due for revisiting
                now_str = datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
                revisit_query = f"""
                    SELECT *,
                           MAX(url_score, page_score) AS max_score,
                           (priority_sitemap * MAX(url_score, page_score)) AS priority_score
                    FROM {self.table_name}
                    WHERE status = 'seen'
                      AND next_revisit_time <= ?
                      AND (url_score > 0 OR page_score > 0)
                    ORDER BY
                        priority_score DESC,
                        time_last_visited ASC
                    LIMIT ?
                """
                cursor = await self.conn.execute(revisit_query, (now_str, remaining))
                rows = await cursor.fetchall()
                for row in rows:
                    if len(urls_to_process) >= count:
                        break
                    url_item = URLItem.from_row(row)
                    urls_to_process.append(url_item)

                # Mark fetched revisiting URLs as 'processing'
                if rows:
                    urls_normalized = [normalize_url(item.url) for item in urls_to_process[-len(rows):]]
                    await self.conn.executemany(
                        f"""
                        UPDATE {self.table_name}
                        SET status = 'processing'
                        WHERE url = ?
                        """,
                        [(url,) for url in urls_normalized]
                    )
                    await self.conn.commit()

                remaining = count - len(urls_to_process)
                if remaining <= 0:
                    return urls_to_process

                # 3. Fetch any remaining URLs based on priority_sitemap or time_last_visited
                any_query = f"""
                    SELECT *,
                           (priority_sitemap) AS priority_score
                    FROM {self.table_name}
                    ORDER BY
                        priority_score DESC,
                        CASE WHEN priority_sitemap IS NULL THEN time_last_visited END ASC
                    LIMIT ?
                """
                cursor = await self.conn.execute(any_query, (remaining,))
                rows = await cursor.fetchall()
                for row in rows:
                    if len(urls_to_process) >= count:
                        break
                    url_item = URLItem.from_row(row)
                    urls_to_process.append(url_item)

                # Mark fetched any URLs as 'processing'
                if rows:
                    urls_normalized = [normalize_url(item.url) for item in urls_to_process[-len(rows):]]
                    await self.conn.executemany(
                        f"""
                        UPDATE {self.table_name}
                        SET status = 'processing'
                        WHERE url = ?
                        """,
                        [(url,) for url in urls_normalized]
                    )
                    await self.conn.commit()

            if urls_to_process:
                return urls_to_process
            return None

        except aiosqlite.Error as e:
            logger.error(f"SQLite error during super_pop: {e}")
            return None

    async def pop(self, count: int = 1, exclude_domains: Optional[List[str]] = None) -> List[URLItem]:
        """
        Retrieve and mark the next batch of URLItems as 'processing', avoiding specified domains.
        Tries to "unite domains" by using a row_number/window function approach:
            1) Partition by domain,
            2) Order each domain group by url_score DESC,
            3) Then pick row_number=1 across all domains, then row_number=2, etc.
        This yields more domain diversity when pulling unseen items.
        """
        urls_to_process = []
        exclude_domains = tuple(exclude_domains or [])  # Convert to tuple for SQLite

        try:
            async with self.conn.execute("BEGIN"):
                # Dynamically build the SQL query to exclude domains
                domain_conditions = " AND ".join(
                    f"LOWER(domain) NOT LIKE ?" for _ in exclude_domains
                )
                exclude_clause = f" AND ({domain_conditions})" if exclude_domains else ""

                # We'll use a CTE with ROW_NUMBER() PARTITION BY domain
                # Then pick from that ordered set until we get `count`.
                # Example approach:
                #
                # WITH domain_ranked AS (
                #   SELECT
                #     t.*,
                #     ROW_NUMBER() OVER (PARTITION BY domain ORDER BY url_score DESC) AS rn
                #   FROM urls_to_visit t
                #   WHERE t.status='unseen' AND <exclude_clause>
                # )
                # SELECT *
                # FROM domain_ranked
                # ORDER BY rn, url_score DESC
                # LIMIT ?

                pop_query = f"""
                    WITH domain_ranked AS (
                        SELECT
                            t.*,
                            ROW_NUMBER() OVER (PARTITION BY domain ORDER BY url_score DESC) AS rn
                        FROM {self.table_name} t
                        WHERE t.status = 'unseen'
                        {exclude_clause}
                    )
                    SELECT *
                    FROM domain_ranked
                    ORDER BY rn, url_score DESC
                    LIMIT ?
                """

                # Build params for the exclude pattern plus 'count'
                params = [f"%{d}%" for d in exclude_domains]
                params.append(count)

                cursor = await self.conn.execute(pop_query, params)
                rows = await cursor.fetchall()
                await cursor.close()

                for row in rows:
                    url_item = URLItem.from_row(row)
                    urls_to_process.append(url_item)

                # Update status of fetched URLs to 'processing'
                if rows:
                    await self.conn.executemany(
                        f"""
                        UPDATE {self.table_name}
                        SET status = 'processing'
                        WHERE url = ?
                        """,
                        [(item.url,) for item in urls_to_process],
                    )
                    await self.conn.commit()

            return urls_to_process

        except aiosqlite.Error as e:
            logger.error(f"Error fetching URLs with pop: {e}")
            return []

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
        except aiosqlite.Error as e:
            logger.error(f"Failed to mark error for '{url}': {e}")

    async def mark_seen(self, url: str) -> None:
        """
        Mark a URL as seen, calculate the next revisit time, and increment revisit_count.
        """
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

            # Increment revisit_count
            await self.conn.execute(
                f"""
                UPDATE {self.table_name}
                SET status = 'seen',
                    time_last_visited = ?,
                    next_revisit_time = ?,
                    revisit_count = revisit_count + 1
                WHERE url = ?
                """,
                (time_last_visited_str, next_revisit_time_str, normalized_url),
            )
            await self.conn.commit()
            self.processed_urls += 1
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
        except aiosqlite.Error as e:
            logger.error(f"Failed to mark '{url}' for revisit: {e}")

    async def _mark_as_processing(self, url: str) -> None:
        """
        Helper method to mark a URL as 'processing'.
        """
        try:
            normalized_url = normalize_url(url)
            await self.conn.execute(
                f"""
                UPDATE {self.table_name}
                SET status = 'processing'
                WHERE url = ?
                """,
                (normalized_url,),
            )
        except aiosqlite.Error as e:
            logger.error(f"Failed to mark '{url}' as processing: {e}")

    async def pop_revisit(self) -> Optional[URLItem]:
        """
        Retrieve and mark the next URL to revisit based on the following priority:
          1) Unseen items with higher max(url_score, page_score)
             Order by max(url_score, page_score) DESC
          2) Seen items with url_score > 0 or page_score > 0
             - Ordered by:
                a) revisit_count ASC
                b) priority_sitemap * max(url_score, page_score) DESC
                c) If priority_sitemap is NULL, then time_last_visited ASC
          3) Any item ordered by:
             a) priority_sitemap DESC
             b) If priority_sitemap is NULL, then time_last_visited ASC

        Returns:
            The next URLItem to process, or None if no URLs are available.
        """
        try:
            async with self.conn.execute("BEGIN"):

                # 1. Attempt to fetch an unseen URL
                unseen_query = f"""
                    SELECT *
                    FROM {self.table_name}
                    WHERE status = 'unseen'
                    ORDER BY MAX(url_score, page_score) DESC
                    LIMIT 1
                """
                cursor = await self.conn.execute(unseen_query)
                row = await cursor.fetchone()
                if row:
                    url_item = URLItem.from_row(row)
                    await self._mark_as_processing(url_item.url)
                    await self.conn.commit()
                    return url_item

                await cursor.close()

                # 2. Attempt to fetch a seen URL with url_score > 0 or page_score > 0
                seen_query = f"""
                    SELECT *,
                           MAX(url_score, page_score) AS max_score,
                           (priority_sitemap * MAX(url_score, page_score)) AS priority_score
                    FROM {self.table_name}
                    WHERE status = 'seen'
                      AND (url_score > 0 OR page_score > 0)
                    ORDER BY
                        revisit_count ASC,
                        priority_score DESC,
                        CASE WHEN priority_sitemap IS NULL THEN time_last_visited END ASC
                    LIMIT 1
                """
                cursor = await self.conn.execute(seen_query)
                row = await cursor.fetchone()
                if row:
                    url_item = URLItem.from_row(row)
                    await self.mark_revisit(url_item.url)
                    await self.conn.commit()
                    return url_item

                await cursor.close()

                # 3. Fetch any URL based on priority_sitemap or time_last_visited
                any_query = f"""
                    SELECT *,
                           (priority_sitemap) AS priority_score
                    FROM {self.table_name}
                    ORDER BY
                        priority_score DESC,
                        CASE WHEN priority_sitemap IS NULL THEN time_last_visited END ASC
                    LIMIT 1
                """
                cursor = await self.conn.execute(any_query)
                row = await cursor.fetchone()
                if row:
                    url_item = URLItem.from_row(row)
                    await self.mark_revisit(url_item.url)
                    await self.conn.commit()
                    return url_item

                await cursor.close()

            # If no URLs are found
            return None

        except aiosqlite.Error as e:
            logger.error(f"SQLite error during pop_revisit: {e}")
            return None

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
        except aiosqlite.Error as e:
            logger.error(f"Failed to store No-Vary-Search policy for {domain}{path}: {e}")

    async def apply_no_vary_search_policy(self, url: str, no_vary_search: str):
        """
        Parse the No-Vary-Search policy and store it in the DB for the given domain and path.
        We'll store policies per (domain, path) basis.
        """
        parsed = urlparse(url)
        domain = parsed.hostname.lower().replace("www.", "") if parsed.hostname else ""
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
        except aiosqlite.Error as e:
            logger.error(f"Failed to reload URLs: {e}")

    async def close(self) -> None:
        """Close any resources if needed."""
        self.revisit_event.set()  # Wake up any waiting coroutines

    async def clear(self) -> None:
        """Clear the URL queue."""
        try:
            await self.conn.execute(f"DELETE FROM {self.table_name}")
            await self.conn.commit()
            logger.info("Cleared URL queue.")
        except aiosqlite.Error as e:
            logger.error(f"Failed to clear the URL queue: {e}")
