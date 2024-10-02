import sqlite3
import datetime
from threading import Lock, RLock
from typing import List, Optional
import logging
import os
# Initialize logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

class Organisation:
    def __init__(self, name: str, type: str, url_found: Optional[str] = None):
        self.name = name
        self.type = type
        self.url_found = url_found

    def to_dict(self):
        return {
            'name': self.name,
            'type': self.type,
            'url_found': self.url_found
        }

class Author:
    def __init__(self, name: str, same_as: Optional[List[str]] = None, url_found: Optional[str] = None):
        self.name = name
        self.same_as = same_as if same_as is not None else []
        self.url_found = url_found

    def to_dict(self):
        return {
            'name': self.name,
            'same_as': ",".join(self.same_as),
            'url_found': self.url_found
        }

class Link:
    def __init__(self, url_found: str, url: str, text: str, mimetype: Optional[str] = None, rel: Optional[str] = None):
        self.url_found = url_found
        self.url = url
        self.text = text
        self.mimetype = mimetype
        self.rel = rel

    def to_dict(self):
        return {
            'url_found': self.url_found,
            'url': self.url,
            'text': self.text,
            'mimetype': self.mimetype,
            'rel': self.rel
        }

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
        revisit_count: int = 0
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
            'url': self.url,
            'url_score': self.url_score,
            'page_score': self.page_score,
            'page_type': self.page_type,
            'lastmod_sitemap': self.lastmod_sitemap,
            'changefreq_sitemap': self.changefreq_sitemap,
            'priority_sitemap': self.priority_sitemap,
            'time_last_visited': self.time_last_visited,
            'is_seen': self.is_seen,
            'is_processing': self.is_processing,
            'revisit_count': self.revisit_count
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
            revisit_count=row[10]
        )

class BackedURLQueue:
    def __init__(self, conn: sqlite3.Connection, table_name="url_metadata", min_heap=False):
        self.conn = conn
        self.table_name = table_name
        self.lock = RLock()
        self.total_urls = 0
        self.processed_urls = 0
        self.start_time = datetime.datetime.now()

        self._setup_db()
        self.reload()

    def _setup_db(self):
        with self.lock:
            self.conn.execute(
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
            self.conn.commit()
       
    
    def set_page_score_page(self, url: str, score: float):
        """Set the page score for a given URL."""
        with self.lock:
            self.conn.execute(
                f"""
                UPDATE {self.table_name}
                SET page_score = ?
                WHERE url = ?
                """, (score, url)
            )
            self.conn.commit()

    def push(self, url_item: URLItem):
        with self.lock:
            data = url_item.to_dict()
            placeholders = ", ".join(["?"] * len(data))
            columns = ", ".join(data.keys())
            values = tuple(data.values())

            self.conn.execute(
                f"""
                INSERT OR REPLACE INTO {self.table_name} ({columns}) 
                VALUES ({placeholders})
                """, values)
            self.conn.commit()
            self.total_urls += 1
    
    def set_page_score(self, url: str, score: float):
        """Set the page score for a given URL."""
        with self.lock:
            self.conn.execute(
                f"""
                UPDATE {self.table_name}
                SET page_score = ?
                WHERE url = ?
                """, (score, url)
            )
            self.conn.commit()

    def add(self, anchor_url, score, anchor_text:Optional[str] = None, page_type:Optional[str] = None, lastmod_sitemap:Optional[str] = None, changefreq_sitemap:Optional[str] = None, priority_sitemap:Optional[float] = None):
        """Add a URL to the queue with the given score. If the URL exists, update the score if the new one is higher."""
        logger.info(f"Adding URL to the queue: {anchor_url} with score {score}")
        with self.lock:
            # Check if the URL already exists in the table
            current_score_row = self.conn.execute(
                f"""
                SELECT url_score FROM {self.table_name}
                WHERE url = ?
                """, (anchor_url,)
            ).fetchone()

            if current_score_row is None:
                # URL does not exist, insert it
                url_item = URLItem(url=anchor_url, url_score=score)
                self.push(url_item)
                logger.info(f"Inserted new URL {anchor_url} with score {score}")
            else:
                # URL exists, check if the new score is higher and update if needed
                current_score = current_score_row[0]
                if score > current_score:
                    self.conn.execute(
                        f"""
                        UPDATE {self.table_name}
                        SET url_score = ?
                        WHERE url = ?
                        """, (score, anchor_url)
                    )
                    self.conn.commit()
                    logger.info(f"Updated score for {anchor_url} to {score}")
                else:
                    logger.info(f"No update needed for {anchor_url}, score {score} is lower or equal to existing {current_score}.")


    def pop(self):
        try:
            with self.lock:
                cur = self.conn.cursor()
                query = f"""
                SELECT url, url_score, page_score, page_type, lastmod_sitemap, changefreq_sitemap,
                       priority_sitemap, time_last_visited, is_seen, is_processing, revisit_count
                FROM {self.table_name}
                WHERE is_seen = 0
                ORDER BY url_score ASC
                LIMIT 1;
                """
                cur.execute(query)
                row = cur.fetchone()

                if row:
                    url_item = URLItem.from_row(row)
                    if not url_item.url:  # Add a check for invalid URLs
                        logger.error("Invalid URL found during pop, skipping")
                        return None

                    self.mark_seen(url_item.url)
                    return url_item
                logger.info("No unseen URL found")
                return None
        except sqlite3.Error as e:
            logger.error(f"SQLite error during pop: {e}")
            return None



    def pop_revisit(self) -> Optional[URLItem]:
        with self.lock:
            cursor = self.conn.cursor()
            cursor.execute(
                f"""
                SELECT url, url_score, page_score, page_type, lastmod_sitemap, changefreq_sitemap,
                       priority_sitemap, time_last_visited, is_seen, is_processing, revisit_count
                FROM {self.table_name}
                WHERE is_seen = 1 AND revisit_count > 0
                ORDER BY revisit_count DESC
                LIMIT 1;
                """
            )
            row = cursor.fetchone()

            if row:
                return URLItem.from_row(row)
            return None

    def mark_seen(self, url: str):
        with self.lock:
            self.conn.execute(
                f"""
                UPDATE {self.table_name}
                SET is_seen = 1, is_processing = 1
                WHERE url = ?
                """, (url,)
            )
            self.conn.commit()
            self.processed_urls += 1
            
    def mark_processing(self, url: str):
        with self.lock:
            self.conn.execute(
                f"""
                UPDATE {self.table_name}
                SET  is_processing = 1
                WHERE url = ?
                """, (url,)
            )
            self.conn.commit()
            self.processed_urls += 1
        
        

    def reload(self):
        with self.lock:
            self.conn.execute(
                f"""
                UPDATE {self.table_name}
                SET is_processing = 0
                WHERE is_processing = 1 AND is_seen = 0
                """
            )
            self.conn.commit()

    def have_been_seen(self, url: str) -> bool:
        with self.lock:
            cursor = self.conn.cursor()
            cursor.execute(
                f"""
                SELECT is_seen FROM {self.table_name}
                WHERE url = ?
                """, (url,)
            )
            result = cursor.fetchone()
            if result:
                return result[0] == 1
            return False

    def count_seen(self) -> int:
        with self.lock:
            cursor = self.conn.cursor()
            cursor.execute(
                f"""
                SELECT COUNT(*) FROM {self.table_name}
                WHERE is_seen = 1
                """
            )
            return cursor.fetchone()[0]

    def count_unseen(self) -> int:
        with self.lock:
            cursor = self.conn.cursor()
            cursor.execute(
                f"""
                SELECT COUNT(*) FROM {self.table_name}
                WHERE is_seen = 0 AND is_processing = 0
                """
            )
            return cursor.fetchone()[0]

    def empty(self) -> bool:
        with self.lock:
            cursor = self.conn.cursor()
            cursor.execute(
                f"""
                SELECT 1 FROM {self.table_name}
                WHERE is_seen = 0 AND is_processing = 0
                LIMIT 1
                """
            )
            return cursor.fetchone() is None

    def reset(self):
        with self.lock:
            self.conn.execute(
                f"""
                UPDATE {self.table_name}
                SET is_seen = 0, is_processing = 0
                """
            )
            self.conn.commit()
