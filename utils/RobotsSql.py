import asyncio
import time
import urllib.robotparser as robotparser
import logging
from urllib.parse import urlparse, urljoin
import aiosqlite
import aiohttp
import traceback

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

class RobotsSql:
    """A class that provides SQL-based storage and retrieval for robots.txt data using aiosqlite."""

    def __init__(self, conn: aiosqlite.Connection, table_name="robot_dict"):
        self.table_name = table_name
        self.conn = conn

    async def initialize(self):
        """Asynchronous initializer to create the table."""
        await self._create_table()

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
                )
            ''')
            await self.conn.commit()
        except Exception as e:
            logger.error(f"Error creating table {self.table_name}: {e}")
            logger.error(traceback.format_exc())

    async def insert_or_update(self, url: str, robot: robotparser.RobotFileParser, text: str):
        """Insert or update the robots.txt data for a given URL."""
        parsed_url = urlparse(url)
        host = parsed_url.hostname or ''
        path = parsed_url.path or '/'
        disallow_all = robot.disallow_all
        allow_all = robot.allow_all
        last_checked = int(time.time())  # Store the current timestamp

        try:
            await self.conn.execute(f'''
                INSERT INTO "{self.table_name}" (url, last_checked, host, path, disallow_all, allow_all, robots_text)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(url) DO UPDATE SET
                    last_checked=excluded.last_checked,
                    host=excluded.host,
                    path=excluded.path,
                    disallow_all=excluded.disallow_all,
                    allow_all=excluded.allow_all,
                    robots_text=excluded.robots_text
            ''', (url, last_checked, host, path, disallow_all, allow_all, text))
            await self.conn.commit()
            logger.info(f"Successfully updated robots.txt entry for {url}")
        except Exception as e:
            logger.error(f"Failed to insert/update robots.txt for {url}: {e}")
            logger.error(traceback.format_exc())

    async def fetch_and_store_robot(self, url: str) -> robotparser.RobotFileParser:
        """Fetch the robots.txt file from the domain and store it in the database, handling HTTP errors."""
        parsed_url = urlparse(url)
        robots_url = urljoin(f"{parsed_url.scheme}://{parsed_url.hostname}", "robots.txt")
        retry_attempts = 3  # Number of retries for specific errors
        retry_delay = 2     # Delay in seconds between retries

        for attempt in range(retry_attempts):
            try:
                async with aiohttp.ClientSession() as session:
                    robot = robotparser.RobotFileParser()
                    async with session.get(robots_url) as response:
                        if response.status == 200:
                            # If robots.txt is fetched successfully, parse and store it
                            robots_text = await response.text()
                            robot.set_url(robots_url)
                            robot.parse(robots_text.splitlines())

                            # Store in database
                            await self.insert_or_update(robots_url, robot, robots_text)
                            logger.info(f"Fetched and stored robots.txt from {robots_url}")
                            return robot
                        else:
                            # Handle specific HTTP error cases
                            disallow_all = False
                            allow_all = False

                            if response.status in (401, 403):
                                robot.disallow_all = True  # Access restricted
                                logger.warning(f"Access forbidden to robots.txt at {robots_url}: HTTP {response.status}")
                            elif 400 <= response.status < 500:
                                robot.allow_all = True  # Not found or client error, assume allowed
                                logger.warning(f"Client error for robots.txt at {robots_url}: HTTP {response.status}")
                            
                            # Store the robots.txt status with disallow_all/allow_all set
                            await self.insert_or_update(
                                robots_url,
                                robot,
                                text="",  # No robots.txt content on error
                            )
                            return None

            except aiohttp.ClientError as e:
                logger.error(f"Client error while fetching robots.txt for {robots_url}: {e}")
                if attempt < retry_attempts - 1:
                    logger.info(f"Retrying in {retry_delay} seconds...")
                    await asyncio.sleep(retry_delay)
            except Exception as e:
                logger.error(f"Unexpected error while fetching robots.txt for {robots_url}: {e}")
                break

        # Final fallback if all retries fail
        await self.insert_or_update(
            robots_url,
            robotparser.RobotFileParser(),
            text="",
            disallow_all=True,
            allow_all=False
        )
        logger.info(f"Set disallow_all=True for {robots_url} due to repeated errors.")
        return None


    async def get(self, url: str) -> robotparser.RobotFileParser:
        parsed_url = urlparse(url)
        robots_url = urljoin(f"{parsed_url.scheme}://{parsed_url.hostname}", "robots.txt")
        try:
            async with self.conn.execute(f'SELECT * FROM "{self.table_name}" WHERE url = ?', (robots_url,)) as cursor:
                row = await cursor.fetchone()
                if row:
                    # Reconstruct the RobotFileParser object from the DB data
                    robot = robotparser.RobotFileParser()
                    robot.set_url(row[2])
                    robot.host = row[3]
                    robot.path = row[4]
                    robot.disallow_all = bool(row[5])
                    robot.allow_all = bool(row[6])

                    if row[7]:
                        robot.parse(row[7].splitlines())

                    return robot
                else:
                    return await self.fetch_and_store_robot(robots_url)
        except Exception as e:
            logger.error(f"Failed to retrieve robots.txt for {url}: {e}")
            logger.error(traceback.format_exc())
            return None

    async def remove(self, url: str):
        """Delete robots.txt data for a given URL."""
        try:
            await self.conn.execute(f'DELETE FROM "{self.table_name}" WHERE url = ?', (url,))
            await self.conn.commit()
            logger.info(f"Removed robots.txt entry for {url}")
        except Exception as e:
            logger.error(f"Failed to remove robots.txt for {url}: {e}")
            logger.error(traceback.format_exc())

    async def clear(self):
        """Clear all entries from the table."""
        try:
            await self.conn.execute(f'DELETE FROM "{self.table_name}"')
            await self.conn.commit()
            logger.info("Cleared all robots.txt entries from the database.")
        except Exception as e:
            logger.error(f"Failed to clear robots.txt entries: {e}")
            logger.error(traceback.format_exc())
