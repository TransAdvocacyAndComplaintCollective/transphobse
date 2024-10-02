import sqlite3
import threading
import time
from urllib.robotparser import RobotFileParser

class SQLDictClass:
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

    def insert_or_update(self,robot:RobotFileParser, text:str):
        url = robot.url
        host = robot.host
        path = robot.path
        disallow_all = robot.disallow_all
        allow_all = robot.allow_all
        
        """Insert or update the robots.txt data for a given URL."""
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
            robot = self.cursor.fetchone()
            robots =RobotFileParser(robot[2])
            robots.parse(robot[7].split('\n'))
            robots.disallow_all = robot[5]
            robots.allow_all = robot[6]
            robots.last_checked = robot[1]
            robots.path = robot[4]
            robots.host = robot[3]
            robots.url = robot[2]
            return robots
            

    def remove(self, url):
        """Remove the entry for a given URL."""
        with self.lock:
            self.cursor.execute(f'DELETE FROM {self.table_name} WHERE url = ?', (url,))
            self.conn.commit()

    def clear(self):
        """Clear all entries in the robots.txt dictionary."""
        with self.lock:
            self.cursor.execute(f'DELETE FROM {self.table_name}')
            self.conn.commit()

    def keys(self):
        """Return a list of all keys (URLs)."""
        with self.lock:
            self.cursor.execute(f'SELECT url FROM {self.table_name}')
            return [row[0] for row in self.cursor.fetchall()]

    def values(self):
        """Return a list of all values (robots.txt texts)."""
        with self.lock:
            self.cursor.execute(f'SELECT text FROM {self.table_name}')
            return [row[0] for row in self.cursor.fetchall()]

    def items(self):
        """Return a list of all items (URL, text) pairs."""
        with self.lock:
            self.cursor.execute(f'SELECT url, text FROM {self.table_name}')
            return [(row[0], row[1]) for row in self.cursor.fetchall()]

    def pop(self, url):
        """Remove and return the robots.txt data for a given URL."""
        with self.lock:
            data = self.get(url)
            if data is None:
                raise KeyError(f"{url} not found in dictionary")
            self.remove(url)
            return data

    def popitem(self):
        """Remove and return an arbitrary (URL, text) pair."""
        with self.lock:
            self.cursor.execute(f'SELECT url, text FROM {self.table_name} LIMIT 1')
            result = self.cursor.fetchone()
            if result:
                url, text = result
                self.remove(url)
                return url, text
            raise KeyError("popitem from an empty dictionary")

    def setdefault(self, url, default=None):
        """Return the value for a URL, and if not present, insert it with the default value."""
        with self.lock:
            item = self.get(url)
            if item is None:
                self.insert_or_update(url, "", "", False, True, default)
                return default
            return item[7]  # Return the text

    def update(self, other):
        """Update the dictionary with key/value pairs from other."""
        with self.lock:
            for url, data in other.items():
                self.insert_or_update(url, *data)

    def copy(self):
        """Return a shallow copy of the dictionary."""
        with self.lock:
            self.cursor.execute(f'SELECT * FROM {self.table_name}')
            return {row[2]: row[7] for row in self.cursor.fetchall()}  # url: text

    def __contains__(self, url):
        """Check if a URL is in the dictionary."""
        with self.lock:
            self.cursor.execute(f'SELECT url FROM {self.table_name} WHERE url = ?', (url,))
            return self.cursor.fetchone() is not None

    def __len__(self):
        """Return the number of items in the dictionary."""
        with self.lock:
            self.cursor.execute(f'SELECT COUNT(*) FROM {self.table_name}')
            return self.cursor.fetchone()[0]

    def __iter__(self):
        """Return an iterator for the dictionary's keys (URLs)."""
        return iter(self.keys())

    def __getitem__(self, url):
        """Retrieve the robots.txt data for a given URL."""
        data = self.get(url)
        if data is None:
            raise KeyError(f"{url} not found in dictionary")
        return data

    def __setitem__(self, url, value):
        """Insert or update the robots.txt data for a given URL."""
        self.insert_or_update(url, *value)

    def __delitem__(self, url):
        """Remove the entry for a given URL."""
        self.remove(url)

    def __repr__(self):
        """Return a string representation of the dictionary."""
        return str(self.items())

    def __del__(self):
        """Close the database connection."""
        with self.lock:
            self.conn.close()

# Example usage
if __name__ == "__main__":
    conn = sqlite3.connect(":memory:", check_same_thread=False)
    sql_dict = SQLDictClass(conn)

    # Insert example data
    sql_dict.insert_or_update(
        url="https://example.com/robots.txt",
        host="example.com",
        path="/",
        disallow_all=False,
        allow_all=True,
        text="User-agent: *\nDisallow: /private/"
    )

    # Retrieve the data
    print(sql_dict.get("https://example.com/robots.txt"))

    # Use dictionary methods
    print("Keys:", sql_dict.keys())
    print("Values:", sql_dict.values())
    print("Items:", sql_dict.items())

    # Pop an item
    print("Popped item:", sql_dict.pop("https://example.com/robots.txt"))

    # Check the current state
    print("Current state:", sql_dict)

    # Clear the dictionary
    sql_dict.clear()

    # Close the connection
    conn.close()
