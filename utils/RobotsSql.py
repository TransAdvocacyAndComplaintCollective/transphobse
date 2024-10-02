
import time
import urllib.robotparser as robotparser
import logging
from urllib.parse import urlparse, urljoin
import threading
from bs4 import BeautifulSoup

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


class RobotsSql:
    """A class that provides SQL-based storage and retrieval for robots.txt data."""
    def __init__(self, conn, table_name="robot_dict"):
        self.table_name = table_name
        self.conn = conn
        self.cursor = self.conn.cursor()
        self.lock = threading.Lock()  # Thread-safe lock
        self._create_table()

    def _create_table(self):
        with self.lock:
            # Create a table to store robots.txt data
            self.cursor.execute(f'''
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
            self.conn.commit()

    def insert_or_update(self, robot, text):
        """Insert or update the robots.txt data for a given URL."""
        url = robot.url
        host = robot.host
        path = robot.path
        disallow_all = robot.disallow_all
        allow_all = robot.allow_all

        last_checked = int(time.time())  # Store the current timestamp
        with self.lock:
            self.cursor.execute(f'''
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
            self.conn.commit()

    def get(self, url):
        """Retrieve the robots.txt data for a given URL."""
        with self.lock:
            self.cursor.execute(f'SELECT * FROM {self.table_name} WHERE url = ?', (url,))
            row = self.cursor.fetchone()
            if not row:
                raise KeyError(f"Robots.txt for URL '{url}' not found.")

            # Reconstruct the RobotFileParser object
            robot = robotparser.RobotFileParser(row[2])
            robot.host = row[3]
            robot.path = row[4]
            robot.disallow_all = row[5]
            robot.allow_all = row[6]
            robot.last_checked = row[1]
            
            # Parse the robots.txt contents stored in the 'text' column
            robot.parse(row[7].splitlines())

            return robot

    def remove(self, url):
        """Delete robots.txt data for a given URL."""
        with self.lock:
            self.cursor.execute(f'DELETE FROM {self.table_name} WHERE url = ?', (url,))
            self.conn.commit()

    def clear(self):
        """Clear all entries from the table."""
        with self.lock:
            self.cursor.execute(f'DELETE FROM {self.table_name}')
            self.conn.commit()
