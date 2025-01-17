import asyncio
import logging
import time
from typing import Any, Optional

import aiohttp
import aiosqlite
from aiohttp import ClientSession
from async_lru import alru_cache
from urllib.robotparser import RobotFileParser
from urllib.parse import urlparse, urljoin
import orjson
from collections import defaultdict
from asyncio.locks import Semaphore

# Configure logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)


class RobotsSql:
    """
    A class that provides SQL-based storage and retrieval for robots.txt data using aiosqlite.
    """

    def __init__(self, db_path: str, table_name: str = "robot_dict", cache_duration: int = 86400):
        """
        Initialize the RobotsSql instance.

        Args:
            db_path (str): Path to the SQLite database file.
            table_name (str, optional): Name of the robots.txt table. Defaults to "robot_dict".
            cache_duration (int, optional): Duration in seconds to cache robots.txt entries.
                Defaults to 1 day (86400 seconds).
        """
        self.db_path = db_path
        self.table_name = table_name
        self.cache_duration = cache_duration
        self.session: Optional[ClientSession] = None
        self.conn: Optional[aiosqlite.Connection] = None
        self.domain_locks = defaultdict(Semaphore)

    async def initialize(self):
        """
        Initialize the database connection, HTTP session,
        and ensure the robots.txt table exists.
        """
        self.conn = await aiosqlite.connect(self.db_path)
        self.conn.row_factory = aiosqlite.Row
        await self._create_table()
        self.session = aiohttp.ClientSession()

    async def _create_table(self):
        """Create the robots.txt storage table if it doesn't exist."""
        try:
            await self.conn.execute(f'''
                CREATE TABLE IF NOT EXISTS "{self.table_name}" (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    last_checked INTEGER,
                    url TEXT UNIQUE,
                    host TEXT,
                    path TEXT,
                    disallow_all BOOLEAN,
                    allow_all BOOLEAN,
                    robots_text TEXT
                );
            ''')
            # Create indexes for faster lookups
            await self.conn.execute(f'''
                CREATE INDEX IF NOT EXISTS idx_url ON "{self.table_name}" (url);
            ''')
            await self.conn.execute(f'''
                CREATE INDEX IF NOT EXISTS idx_host_path ON "{self.table_name}" (host, path);
            ''')
            await self.conn.commit()
        except Exception as e:
            logger.error(f"Error creating table {self.table_name}: {e}")
            raise

    @alru_cache(maxsize=1024)
    async def get_robot_parser(self, url: str) -> Optional[RobotFileParser]:
        """
        Retrieve the RobotFileParser for a given URL.
        Fetches and stores robots.txt if not present.

        Args:
            url (str): The URL for which to retrieve robots.txt.

        Returns:
            Optional[RobotFileParser]: The RobotFileParser object or None if retrieval failed.
        """
        robots_url = self._construct_robots_url(url)

        # Check if robots.txt is already in the database
        robot_data = await self._fetch_robot_from_db(robots_url)
        if robot_data:
            robot = self._reconstruct_robot(robot_data)
            return robot

        # Prevent multiple fetches for the same domain
        domain_lock = self.domain_locks[robots_url]
        async with domain_lock:
            # Double-check to avoid redundant fetches during lock
            robot_data = await self._fetch_robot_from_db(robots_url)
            if robot_data:
                robot = self._reconstruct_robot(robot_data)
                return robot

            # If not in DB, fetch from the web
            robot = await self._fetch_and_store_robot(robots_url)
            return robot

    def _construct_robots_url(self, url: str) -> str:
        """Construct the robots.txt URL based on the given URL."""
        parsed = urlparse(url)
        # If no scheme or https is used, default to https
        scheme = "https" if parsed.scheme in ("https", "") else "http"
        netloc = parsed.netloc or ""
        return f"{scheme}://{netloc}/robots.txt"

    async def _fetch_robot_from_db(self, robots_url: str) -> Optional[aiosqlite.Row]:
        """Fetch robots.txt data from the database."""
        try:
            async with self.conn.execute(f'''
                SELECT * FROM "{self.table_name}"
                WHERE url = ?
                  AND last_checked > ?
            ''', (robots_url, int(time.time()) - self.cache_duration)) as cursor:
                return await cursor.fetchone()
        except Exception as e:
            logger.error(f"Error fetching robots.txt from DB for {robots_url}: {e}")
            return None

    def _reconstruct_robot(self, row: aiosqlite.Row) -> RobotFileParser:
        """
        Reconstruct a RobotFileParser object from a database row.
        """
        robot = RobotFileParser()
        robot.set_url(row["url"])
        if row["robots_text"]:
            robot.parse(row["robots_text"].splitlines())
        else:
            # If there's no text, fallback on allow_all/disallow_all fields
            robot.disallow_all = row["disallow_all"]
            robot.allow_all = row["allow_all"]
        return robot

    async def _fetch_and_store_robot(self, robots_url: str) -> Optional[RobotFileParser]:
        """
        Fetch robots.txt from the web, store it in the database,
        and return the RobotFileParser.
        """
        try:
            async with self.session.get(robots_url, timeout=10) as response:
                robots_text = await response.text()
                status = response.status

                robot = RobotFileParser()
                robot.set_url(robots_url)
                robot.parse(robots_text.splitlines())

                # Based on the parser, figure out the boolean flags
                disallow_all = robot.disallow_all
                allow_all = robot.allow_all

                # If the server returns 404, treat it as no restrictions
                if status == 404:
                    disallow_all = False
                    allow_all = True

                # Store in the database
                await self._insert_or_update_robot(
                    robots_url=robots_url,
                    robots_text=robots_text,
                    host=urlparse(robots_url).hostname or "",
                    path=urlparse(robots_url).path or "/",
                    disallow_all=disallow_all,
                    allow_all=allow_all
                )
                logger.info(f"Fetched and stored robots.txt from {robots_url}")
                return robot

        except Exception as e:
            logger.error(f"Error fetching robots.txt from {robots_url}: {e}")
            return None

    async def _insert_or_update_robot(
        self,
        robots_url: str,
        robots_text: str,
        host: str,
        path: str,
        disallow_all: bool,
        allow_all: bool
    ) -> None:
        """
        Insert or update robots.txt data in the database.
        """
        try:
            last_checked = int(time.time())
            await self.conn.execute(f'''
                INSERT INTO "{self.table_name}"
                    (url, last_checked, host, path,
                     disallow_all, allow_all, robots_text)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(url) DO UPDATE SET
                    last_checked=excluded.last_checked,
                    host=excluded.host,
                    path=excluded.path,
                    disallow_all=excluded.disallow_all,
                    allow_all=excluded.allow_all,
                    robots_text=excluded.robots_text
            ''', (
                robots_url,
                last_checked,
                host,
                path,
                disallow_all,
                allow_all,
                robots_text
            ))
            await self.conn.commit()
        except Exception as e:
            logger.error(f"Failed to insert/update robots.txt for {robots_url}: {e}")

    async def close(self):
        """
        Close the database connection and HTTP session.
        """
        if self.session:
            await self.session.close()
            logger.info("HTTP session closed.")
        if self.conn:
            await self.conn.close()
            logger.info("Database connection closed.")
