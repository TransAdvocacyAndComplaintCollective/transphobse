import asyncio
import aiosqlite
import logging
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse

logger = logging.getLogger(__name__)

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
        retries: int = 0,
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
        self.lastmod_html = lastmod_html
        self.priority = priority
        self.status = status
        self.etag = etag
        self.last_modified = last_modified
        self.domain = domain or self.extract_domain(url)
        self.retries = retries

    @staticmethod
    def extract_domain(url: str) -> str:
        parsed = urlparse(url)
        return parsed.hostname.lower() if parsed.hostname else ""

    @staticmethod
    def from_row(row: aiosqlite.Row) -> "URLItem":
        valid_fields = {field for field in URLItem.__init__.__code__.co_varnames}
        filtered_row = {key: row[key] for key in row.keys() if key in valid_fields}
        return URLItem(**filtered_row)

    def to_dict(self) -> Dict[str, Any]:
        return self.__dict__.copy()


class BackedURLQueue:
    def __init__(self, conn: aiosqlite.Connection, table_name: str = "urls_to_visit"):
        self.conn = conn
        self.table_name = table_name
        self.lock = asyncio.Lock()

    async def initialize(self) -> None:
        await self._setup_db()
        await self._setup_no_vary_search_policy_table()

    async def _setup_db(self) -> None:
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
                lastmod_html TEXT,
                priority REAL,
                status TEXT DEFAULT 'unseen',
                etag TEXT,
                last_modified TEXT,
                domain TEXT,
                retries INTEGER DEFAULT 0
            )
            """
        )
        await self.conn.execute(
            f"CREATE INDEX IF NOT EXISTS idx_status_url_score ON {self.table_name} (status, url_score DESC);"
        )
        await self.conn.execute(
            f"CREATE INDEX IF NOT EXISTS idx_next_revisit_time ON {self.table_name} (next_revisit_time);"
        )
        await self.conn.commit()

    async def _setup_no_vary_search_policy_table(self):
        await self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS no_vary_search_policies (
                domain TEXT NOT NULL,
                path TEXT NOT NULL,
                params BOOLEAN,
                except_policy TEXT,
                key_order BOOLEAN,
                PRIMARY KEY (domain, path)
            )
            """
        )
        await self.conn.commit()

    async def push(self, url_item: URLItem) -> None:
        async with self.lock:
            data = url_item.to_dict()
            placeholders = ", ".join(["?"] * len(data))
            columns = ", ".join(data.keys())
            values = tuple(data.values())
            try:
                existing = await self.conn.execute(
                    f"SELECT 1 FROM {self.table_name} WHERE url = ? LIMIT 1",
                    (url_item.url,)
                )
                if not await existing.fetchone():
                    await self.conn.execute(
                        f"INSERT INTO {self.table_name} ({columns}) VALUES ({placeholders})",
                        values,
                    )
                    await self.conn.commit()
            except aiosqlite.Error as e:
                logger.error(f"Failed to push URL '{url_item.url}': {e}")

    async def get_no_vary_search_policy(self, domain: str, path: str) -> Optional[Dict[str, Any]]:
        async with self.lock:
            query = f"""
            SELECT * 
            FROM no_vary_search_policies
            WHERE domain = ? AND path = ?
            LIMIT 1
            """
            try:
                cursor = await self.conn.execute(query, (domain, path))
                row = await cursor.fetchone()
                await cursor.close()
                if row:
                    return dict(row)
                else:
                    return None
            except aiosqlite.Error as e:
                logger.error(f"Error fetching no-vary search policy for {domain}{path}: {e}")
                return None

    async def pop(self, count: int = 1, exclude_domains: Optional[List[str]] = None) -> List[URLItem]:
        async with self.lock:
            exclude_domains = exclude_domains or []
            exclude_clause = " AND ".join(f"domain NOT LIKE ?" for _ in exclude_domains)
            where_clause = f"status = 'unseen' {f'AND {exclude_clause}' if exclude_domains else ''}"
            params = [f"%{d}%" for d in exclude_domains] + [count]

            query = f"""
                WITH ranked_urls AS (
                    SELECT *, ROW_NUMBER() OVER (PARTITION BY domain ORDER BY url_score DESC) AS rn
                    FROM {self.table_name}
                    WHERE {where_clause}
                )
                SELECT *
                FROM ranked_urls
                WHERE rn <= ?
                ORDER BY rn, url_score DESC
            """
            cursor = await self.conn.execute(query, params)
            rows = await cursor.fetchall()
            await cursor.close()

            if not rows:
                return []

            urls = [URLItem.from_row(row) for row in rows]
            url_ids = [url.url for url in urls]
            await self.conn.execute(
                f"UPDATE {self.table_name} SET status = 'processing' WHERE url IN ({','.join('?' for _ in url_ids)})",
                url_ids,
            )
            await self.conn.commit()

            return urls

    async def is_not_seen(self, url: str) -> bool:
        cursor = await self.conn.execute(
            f"SELECT COUNT(*) FROM {self.table_name} WHERE url = ? AND status = 'unseen'",
            (url,),
        )
        count = (await cursor.fetchone())[0]
        await cursor.close()
        return count > 0

    async def have_been_seen(self, urls: List[str]) -> List[str]:
        placeholders = ", ".join(["?"] * len(urls))
        query = f"""
        SELECT url 
        FROM {self.table_name} 
        WHERE url IN ({placeholders}) AND status = 'seen'
        """
        cursor = await self.conn.execute(query, urls)
        rows = await cursor.fetchall()
        await cursor.close()
        return [row[0] for row in rows]

    async def mark_seen(self, url: str) -> None:
        async with self.lock:
            await self.conn.execute(
                f"UPDATE {self.table_name} SET status = 'seen', time_last_visited = datetime('now') WHERE url = ?",
                (url,),
            )
            await self.conn.commit()

    async def update_error(self, url: str, error_message: str) -> None:
        async with self.lock:
            await self.conn.execute(
                f"UPDATE {self.table_name} SET status = 'error', error = ? WHERE url = ?",
                (error_message, url),
            )
            await self.conn.commit()

    async def set_page_score(self, url: str, page_score: float) -> None:
        async with self.lock:
            await self.conn.execute(
                f"UPDATE {self.table_name} SET page_score = ? WHERE url = ?",
                (page_score, url),
            )
            await self.conn.commit()

    async def count_seen(self) -> int:
        cursor = await self.conn.execute(
            f"SELECT COUNT(*) FROM {self.table_name} WHERE status = 'seen'"
        )
        count = (await cursor.fetchone())[0]
        await cursor.close()
        return count

    async def empty(self) -> bool:
        cursor = await self.conn.execute(
            f"SELECT COUNT(*) FROM {self.table_name} WHERE status = 'unseen'"
        )
        count = (await cursor.fetchone())[0]
        await cursor.close()
        return count == 0
