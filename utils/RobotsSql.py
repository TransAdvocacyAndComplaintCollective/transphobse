import time
import urllib.robotparser as robotparser
import logging
from urllib.parse import urlparse, urljoin
import aiosqlite  # Use aiosqlite for asynchronous SQLite operations
from bs4 import BeautifulSoup

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


class RobotsSql:
    """A class that provides SQL-based storage and retrieval for robots.txt data using aiosqlite."""
    def __init__(self, conn: aiosqlite.Connection, table_name="robot_dict"):
        self.table_name = table_name
        self.conn = conn
        # No need for self.cursor as we'll use conn.execute directly
        # No threading locks are needed in an async context
        # Initialize the table asynchronously
        # Note: We can't call async methods in __init__, so we'll need an async initializer
        # We'll add an `initialize` method to set up the table

    async def initialize(self):
        """Asynchronous initializer to create the table."""
        await self._create_table()

    async def _create_table(self):
        # Create a table to store robots.txt data
        await self.conn.execute(f'''
            CREATE TABLE IF NOT EXISTS {self.table_name} (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                last_checked INTEGER,
                url TEXT UNIQUE,
                host TEXT,
                path TEXT,
                disallow_all BOOLEAN,
                allow_all BOOLEAN,
                text TEXT CHECK(length(text) <= 512000)  -- Limit to 500 KiB
            )
        ''')
        await self.conn.commit()

    async def insert_or_update(self, robot, text):
        """Insert or update the robots.txt data for a given URL."""
        url = robot.url
        host = robot.host
        path = robot.path
        disallow_all = robot.disallow_all
        allow_all = robot.allow_all

        last_checked = int(time.time())  # Store the current timestamp

        await self.conn.execute(f'''
            INSERT INTO {self.table_name} (last_checked, url, host, path, disallow_all, allow_all, text)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(url) DO UPDATE SET
                last_checked = excluded.last_checked,
                host = excluded.host,
                path = excluded.path,
                disallow_all = excluded.disallow_all,
                allow_all = excluded.allow_all,
                text = excluded.text
        ''', (last_checked, url, host, path, disallow_all, allow_all, text))
        await self.conn.commit()

    async def get(self, url):
        """Retrieve the robots.txt data for a given URL."""
        async with self.conn.execute(f'SELECT * FROM {self.table_name} WHERE url = ?', (url,)) as cursor:
            row = await cursor.fetchone()
            if not row:
                raise KeyError(f"Robots.txt for URL '{url}' not found.")

            # Reconstruct the RobotFileParser object
            robot = robotparser.RobotFileParser(row[2])
            robot.host = row[3]
            robot.path = row[4]
            robot.disallow_all = bool(row[5])
            robot.allow_all = bool(row[6])
            robot.last_checked = row[1]

            # Parse the robots.txt contents stored in the 'text' column
            robot.parse(row[7].splitlines())

        return robot

    async def remove(self, url):
        """Delete robots.txt data for a given URL."""
        await self.conn.execute(f'DELETE FROM {self.table_name} WHERE url = ?', (url,))
        await self.conn.commit()

    async def clear(self):
        """Clear all entries from the table."""
        await self.conn.execute(f'DELETE FROM {self.table_name}')
        await self.conn.commit()
