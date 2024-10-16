import aiosqlite
import datetime
import logging
from typing import Optional

# Initialize logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
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
        is_seen: bool = False,
        is_processing: bool = False,
        revisit_count: int = 0,
    ):
        self.url = url
        self.url_score = url_score
        self.page_score = page_score
        self.page_type = page_type
        self.lastmod_sitemap = lastmod_sitemap
        self.changefreq_sitemap = changefreq_sitemap
        self.priority_sitemap = priority_sitemap
        self.time_last_visited = time_last_visited
        self.is_seen = is_seen
        self.is_processing = is_processing
        self.revisit_count = revisit_count

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
            "is_seen": self.is_seen,
            "is_processing": self.is_processing,
            "revisit_count": self.revisit_count,
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
            is_seen=row[8],
            is_processing=row[9],
            revisit_count=row[10],
        )


class BackedURLQueue:
    def __init__(self, conn: aiosqlite.Connection, table_name="url_metadata"):
        self.conn = conn
        self.table_name = table_name
        self.total_urls = 0
        self.processed_urls = 0
        self.start_time = datetime.datetime.now()

    async def initialize(self):
        """Initialize the database table and reload any necessary data."""
        await self._setup_db()
        await self.reload()

    async def _setup_db(self):
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
                is_seen BOOLEAN DEFAULT 0,
                is_processing BOOLEAN DEFAULT 0,
                revisit_count INTEGER DEFAULT 0
            );
            """
        )
        await self.conn.commit()

    async def set_page_score(self, url: str, score: float):
        """Set the page score for a given URL."""
        await self.conn.execute(
            f"""
            UPDATE {self.table_name}
            SET page_score = ?
            WHERE url = ?
            """,
            (score, url),
        )
        await self.conn.commit()

    async def push(self, url_item: URLItem):
        """Add a new URLItem to the queue."""
        data = url_item.to_dict()
        placeholders = ", ".join(["?"] * len(data))
        columns = ", ".join(data.keys())
        values = tuple(data.values())

        await self.conn.execute(
            f"""
            INSERT OR REPLACE INTO {self.table_name} ({columns})
            VALUES ({placeholders})
            """,
            values,
        )
        await self.conn.commit()
        self.total_urls += 1

    async def count_all(self) -> int:
        """Return the total number of URLs in the queue."""
        async with self.conn.execute(
            f"""
            SELECT COUNT(*) FROM {self.table_name}
            """
        ) as cursor:
            result = await cursor.fetchone()
            return result[0]
    
    async def set_page_score_page(self, url: str, score: float):
        """Set the page score for a given URL."""
        await self.conn.execute(
            f"""
            UPDATE {self.table_name}
            SET page_score = ?
            WHERE url = ?
            """,
            (score, url),
        )
        await self.conn.commit()

    async def add(
        self,
        anchor_url,
        score,
        anchor_text: Optional[str] = None,
        page_type: Optional[str] = None,
        lastmod_sitemap: Optional[str] = None,
        changefreq_sitemap: Optional[str] = None,
        priority_sitemap: Optional[float] = None,
    ):
        """
        Add a URL to the queue with the given score.
        If the URL exists, update the score if the new one is higher.
        """
        async with self.conn.execute(
            f"""
            SELECT url_score FROM {self.table_name}
            WHERE url = ?
            """,
            (anchor_url,),
        ) as cursor:
            current_score_row = await cursor.fetchone()

        if current_score_row is None:
            # URL does not exist, insert it
            url_item = URLItem(
                url=anchor_url,
                url_score=score,
                page_type=page_type,
                lastmod_sitemap=lastmod_sitemap,
                changefreq_sitemap=changefreq_sitemap,
                priority_sitemap=priority_sitemap,
            )
            await self.push(url_item)
        else:
            # URL exists, check if the new score is higher and update if needed
            current_score = current_score_row[0]
            if score > current_score:
                await self.conn.execute(
                    f"""
                    UPDATE {self.table_name}
                    SET url_score = ?
                    WHERE url = ?
                    """,
                    (score, anchor_url),
                )
                await self.conn.commit()

    async def pop(self, allow_for_recruiting=False) -> Optional[URLItem]:
        """Retrieve and return the next URLItem to process."""
        try:
            async with self.conn.execute(
                f"""
                SELECT url, url_score, page_score, page_type, lastmod_sitemap, changefreq_sitemap,
                       priority_sitemap, time_last_visited, is_seen, is_processing, revisit_count
                FROM {self.table_name}
                WHERE is_seen = 0 AND is_processing = 0
                ORDER BY url_score ASC
                LIMIT 1;
                """
            ) as cursor:
                row = await cursor.fetchone()

            if row:
                url_item = URLItem.from_row(row)
                if not url_item.url:
                    logger.error("Invalid URL found during pop, skipping")
                    # Mark as seen to prevent reprocessing
                    await self.mark_seen(url_item.url)
                    return None

                await self.mark_processing(url_item.url)
                return url_item
            logger.info("No unseen URL found")
            return None
        except aiosqlite.Error as e:
            logger.error(f"SQLite error during pop: {e}")
            return None

    async def mark_seen(self, url: str):
        """Mark a URL as seen and processed."""
        await self.conn.execute(
            f"""
            UPDATE {self.table_name}
            SET is_seen = 1, is_processing = 1
            WHERE url = ?
            """,
            (url,),
        )
        await self.conn.commit()
        self.processed_urls += 1

    async def mark_processing(self, url: str):
        """Mark a URL as currently being processed."""
        await self.conn.execute(
            f"""
            UPDATE {self.table_name}
            SET is_processing = 1
            WHERE url = ?
            """,
            (url,),
        )
        await self.conn.commit()

    async def reload(self):
        """Reset the processing status of URLs that were being processed when the crawler stopped."""
        await self.conn.execute(
            f"""
            UPDATE {self.table_name}
            SET is_processing = 0
            WHERE is_processing = 1 AND is_seen = 0
            """
        )
        await self.conn.commit()

    async def have_been_seen(self, url: str) -> bool:
        """Check if a URL has already been seen."""
        async with self.conn.execute(
            f"""
            SELECT is_seen FROM {self.table_name}
            WHERE url = ?
            """,
            (url,),
        ) as cursor:
            result = await cursor.fetchone()
            if result:
                return result[0] == 1
            return False

    async def count_seen(self) -> int:
        """Return the number of URLs that have been seen."""
        async with self.conn.execute(
            f"""
            SELECT COUNT(*) FROM {self.table_name}
            WHERE is_seen = 1
            """
        ) as cursor:
            result = await cursor.fetchone()
            return result[0]

    async def count_unseen(self) -> int:
        """Return the number of URLs that have not been seen yet."""
        async with self.conn.execute(
            f"""
            SELECT COUNT(*) FROM {self.table_name}
            WHERE is_seen = 0 AND is_processing = 0
            """
        ) as cursor:
            result = await cursor.fetchone()
            return result[0]

    async def empty(self, allow_for_recruiting=False) -> bool:
        """Check if the queue is empty."""
        async with self.conn.execute(
            f"""
            SELECT 1 FROM {self.table_name}
            WHERE is_seen = 0 AND is_processing = 0
            LIMIT 1
            """
        ) as cursor:
            result = await cursor.fetchone()
            return result is None

    async def reset(self):
        """Reset the seen and processing status of all URLs."""
        await self.conn.execute(
            f"""
            UPDATE {self.table_name}
            SET is_seen = 0, is_processing = 0
            """
        )
        await self.conn.commit()
