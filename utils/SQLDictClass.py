import time
from urllib.robotparser import RobotFileParser
import aiosqlite

class SQLDictClass:
    def __init__(self, conn, table_name="robot_dict"):
        self.table_name = table_name
        self.conn = conn

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

    async def insert_or_update(self, robot: RobotFileParser, text: str):
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
            robot = RobotFileParser(row[2])
            robot.host = row[3]
            robot.path = row[4]
            robot.disallow_all = bool(row[5])
            robot.allow_all = bool(row[6])
            robot.last_checked = row[1]
            robot.parse(row[7].splitlines())

            return robot

    async def remove(self, url):
        """Remove the entry for a given URL."""
        await self.conn.execute(f'DELETE FROM {self.table_name} WHERE url = ?', (url,))
        await self.conn.commit()

    async def clear(self):
        """Clear all entries in the robots.txt dictionary."""
        await self.conn.execute(f'DELETE FROM {self.table_name}')
        await self.conn.commit()

    async def keys(self):
        """Return a list of all keys (URLs)."""
        async with self.conn.execute(f'SELECT url FROM {self.table_name}') as cursor:
            rows = await cursor.fetchall()
            return [row[0] for row in rows]

    async def values(self):
        """Return a list of all values (robots.txt texts)."""
        async with self.conn.execute(f'SELECT text FROM {self.table_name}') as cursor:
            rows = await cursor.fetchall()
            return [row[0] for row in rows]

    async def items(self):
        """Return a list of all items (URL, text) pairs."""
        async with self.conn.execute(f'SELECT url, text FROM {self.table_name}') as cursor:
            rows = await cursor.fetchall()
            return [(row[0], row[1]) for row in rows]

    async def pop(self, url):
        """Remove and return the robots.txt data for a given URL."""
        data = await self.get(url)
        await self.remove(url)
        return data

    async def popitem(self):
        """Remove and return an arbitrary (URL, text) pair."""
        async with self.conn.execute(f'SELECT url, text FROM {self.table_name} LIMIT 1') as cursor:
            result = await cursor.fetchone()
            if result:
                url, text = result
                await self.remove(url)
                return url, text
            raise KeyError("popitem from an empty dictionary")

    async def setdefault(self, url, default=None):
        """Return the value for a URL, and if not present, insert it with the default value."""
        try:
            item = await self.get(url)
        except KeyError:
            # If the item does not exist, insert it with the default value
            await self.insert_or_update(RobotFileParser(url), default)
            return default
        return item.text

    async def update(self, other):
        """Update the dictionary with key/value pairs from another dictionary."""
        for url, data in other.items():
            robot = RobotFileParser(url)
            robot.parse(data[1].splitlines())
            await self.insert_or_update(robot, data[1])

    async def copy(self):
        """Return a shallow copy of the dictionary."""
        async with self.conn.execute(f'SELECT * FROM {self.table_name}') as cursor:
            rows = await cursor.fetchall()
            return {row[2]: row[7] for row in rows}  # url: text

    async def __contains__(self, url):
        """Check if a URL is in the dictionary."""
        async with self.conn.execute(f'SELECT url FROM {self.table_name} WHERE url = ?', (url,)) as cursor:
            result = await cursor.fetchone()
            return result is not None

    async def __len__(self):
        """Return the number of items in the dictionary."""
        async with self.conn.execute(f'SELECT COUNT(*) FROM {self.table_name}') as cursor:
            result = await cursor.fetchone()
            return result[0]

    async def __iter__(self):
        """Return an iterator for the dictionary's keys (URLs)."""
        keys = await self.keys()
        return iter(keys)

    async def __getitem__(self, url):
        """Retrieve the robots.txt data for a given URL."""
        return await self.get(url)

    async def __setitem__(self, url, value):
        """Insert or update the robots.txt data for a given URL."""
        await self.insert_or_update(url, value)

    async def __delitem__(self, url):
        """Remove the entry for a given URL."""
        await self.remove(url)

    def __repr__(self):
        """Return a string representation of the dictionary."""
        return str(asyncio.run(self.items()))

    async def close(self):
        """Close the database connection."""
        await self.conn.close()

# Example usage
async def main():
    conn = await aiosqlite.connect(":memory:")
    sql_dict = SQLDictClass(conn)
    await sql_dict.initialize()

    # Insert example data
    robot_parser = RobotFileParser("https://example.com/robots.txt")
    robot_parser.parse("User-agent: *\nDisallow: /private/".splitlines())
    await sql_dict.insert_or_update(robot_parser, "User-agent: *\nDisallow: /private/")

    # Retrieve the data
    robot = await sql_dict.get("https://example.com/robots.txt")
    print(robot)

    # Use dictionary methods
    print("Keys:", await sql_dict.keys())
    print("Values:", await sql_dict.values())
    print("Items:", await sql_dict.items())

    # Pop an item
    print("Popped item:", await sql_dict.pop("https://example.com/robots.txt"))

    # Check the current state
    print("Current state:", await sql_dict.items())

    # Clear the dictionary
    await sql_dict.clear()

    # Close the connection
    await sql_dict.close()
