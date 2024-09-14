import os
import gzip
import pickle
import threading
import logging
import time
import requests
import ssl
import re
import csv
import concurrent.futures
from bs4 import BeautifulSoup
from io import BytesIO
from urllib.parse import urljoin, urlparse, urlunparse
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from requests.packages.urllib3.exceptions import InsecureRequestWarning
from cachetools import TTLCache, cached
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
from typing import List, Tuple, Generator, Optional
import networkx as nx
from typing import List

# Import custom keywords (assuming you have a module named `keywords.py`)
from utils.keywords import  Little_List

# Suppress InsecureRequestWarnings
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

# Checkpoint settings
CHECKPOINT_DIR = 'checkpoints'  # Define a directory for checkpoints
CHECKPOINT_FULL_FILE = os.path.join(CHECKPOINT_DIR, 'checkpoint_full.pkl.gz')  # Full checkpoint file path
CHECKPOINT_INCREMENTAL_FILE = os.path.join(CHECKPOINT_DIR, 'checkpoint_incremental.pkl.gz')  # Incremental checkpoint file path

# Create the checkpoint directory if it doesn't exist
if not os.path.exists(CHECKPOINT_DIR):
    os.makedirs(CHECKPOINT_DIR)

# Lock for thread safety when writing to shared resources
csv_lock = threading.Lock()

class SSLAdapter(HTTPAdapter):
    """An HTTP adapter that uses a client SSL context."""
    def __init__(self, ssl_context=None, **kwargs):
        self.ssl_context = ssl_context
        super().__init__(**kwargs)

    def init_poolmanager(self, *args, **kwargs):
        kwargs['ssl_context'] = self.ssl_context
        super().init_poolmanager(*args, **kwargs)

def create_ssl_session(verify_ssl: bool = True) -> requests.Session:
    """Create a session that uses a client SSL/TLS context with retry strategy."""
    session = requests.Session()
    retries = Retry(
        total=5,
        backoff_factor=1,
        status_forcelist=[403, 500, 502, 503, 504],
        raise_on_status=False
    )
    context = ssl.create_default_context(ssl.Purpose.SERVER_AUTH)
    context.minimum_version = ssl.TLSVersion.TLSv1_2

    if not verify_ssl:
        context.check_hostname = False
        context.verify_mode = ssl.CERT_NONE

    adapter = SSLAdapter(ssl_context=context, max_retries=retries)
    session.mount('https://', adapter)
    return session

class SitemapCrawler:
    def __init__(self, main_sitemap: str, target_paths: List[str], anti_target_paths: List[str], 
                 allowed_domains: List[str], feeds: Optional[List[str]] = None, 
                 page_like_sitemap: Optional[List[str]] = None):
        self.main_sitemap = main_sitemap
        self.target_paths = sorted(target_paths, key=len, reverse=True)
        self.anti_target_paths = sorted(anti_target_paths, key=len, reverse=True)
        self.allowed_domains = set(domain.lower() for domain in allowed_domains)
        self.feeds = feeds or []
        self.page_like_sitemap = page_like_sitemap or []
        self.session = create_ssl_session(verify_ssl=False)  # Reusable session
        self.context_graph = nx.DiGraph()  # Initialize context graph
        print("Crawler initialized.")
        
        
    # Updated XML parser with 'features' parameter
    @staticmethod
    def parse_xml_sitemap(xml_content: bytes) -> Tuple[List[str], List[str]]:
        """Parse XML sitemap and return URLs and sitemaps."""
        soup = BeautifulSoup(xml_content, 'xml', features='xml')
        urls = [loc.text for loc in soup.find_all('loc')]
        sitemaps = [loc.text for loc in soup.find_all('sitemap loc')]
        return urls, sitemaps

    @staticmethod
    def parse_html_sitemap(html_content: str, base_url: str) -> Tuple[List[str], List[str]]:
        """Parse HTML sitemap and return URLs."""
        soup = BeautifulSoup(html_content, 'html.parser')
        urls = [urljoin(base_url, link['href']) for link in soup.find_all('a', href=True) if SitemapCrawler.is_valid_url(urljoin(base_url, link['href']))]
        return urls, []

    def get_checkpoint_files(self) -> list:
        """Retrieve a list of possible checkpoint files to attempt loading from, ordered by recency."""
        checkpoint_files = []
        # Add full checkpoint first
        if os.path.exists(CHECKPOINT_FULL_FILE):
            checkpoint_files.append(CHECKPOINT_FULL_FILE)

        # Get the directory of the incremental checkpoint file
        checkpoint_dir = os.path.dirname(CHECKPOINT_INCREMENTAL_FILE)

        # Ensure checkpoint_dir is valid and exists
        if checkpoint_dir and os.path.exists(checkpoint_dir):
            incremental_files = sorted(
                [os.path.join(checkpoint_dir, f) for f in os.listdir(checkpoint_dir) if f.startswith(os.path.basename(CHECKPOINT_INCREMENTAL_FILE))],
                key=os.path.getmtime,
                reverse=True
            )
            checkpoint_files.extend(incremental_files)
        else:
            logging.error(f"Checkpoint directory '{checkpoint_dir}' does not exist.")  # Corrected error message here

        return checkpoint_files

    def parse_robots_txt(content: str) -> List[str]:
        """
        Parse the content of a robots.txt file to extract sitemap URLs.
    
        Args:
        - content (str): The content of the robots.txt file as a string.
    
        Returns:
        - List[str]: A list of valid sitemap URLs found in the robots.txt content.
        """
        sitemaps = []
    
        # Split the content into lines and iterate through each line
        for line in content.splitlines():
            line = line.strip()  # Remove leading and trailing whitespace
    
            # Skip empty lines and comments
            if not line or line.startswith('#'):
                continue
            
            # Check if the line starts with 'Sitemap:' (case insensitive)
            if line.lower().startswith("sitemap:"):
                # Split the line on the first ':' to extract the URL
                parts = line.split(":", 1)
                if len(parts) == 2:
                    sitemap_url = parts[1].strip()
    
                    # Validate the extracted URL
                    if is_valid_url(sitemap_url):
                        sitemaps.append(sitemap_url)
    
        return sitemaps
    
    def is_valid_url(url: str) -> bool:
        """
        Validate a URL to ensure it is well-formed.
    
        Args:
        - url (str): The URL to validate.
    
        Returns:
        - bool: True if the URL is valid, False otherwise.
        """
        parsed = urlparse(url)
        return bool(parsed.scheme) and bool(parsed.netloc)

    @cached(cache=TTLCache(maxsize=1000, ttl=86400))
    def fetch_robots_txt(self, url: str) -> List[str]:
        """
        Fetch and parse the robots.txt file to find sitemap URLs.

        Args:
        - url (str): The base URL of the website (e.g., "https://www.example.com").

        Returns:
        - List[str]: A list of sitemap URLs extracted from the robots.txt file.
        """
        robots_url = urljoin(url, "/robots.txt")  # Construct the URL for robots.txt
        try:
            # Make a GET request to fetch the robots.txt file
            response = self.session.get(robots_url, headers={'User-Agent': 'Mozilla/5.0'}, timeout=10, verify=False)
            response.raise_for_status()  # Raise an exception for HTTP errors

            # Parse the content to extract sitemap URLs
            sitemap_urls = self.parse_robots_txt(response.text)
            return sitemap_urls

        except requests.exceptions.RequestException as e:
            # Log an error message if there's an issue fetching robots.txt
            logging.error(f"Error fetching robots.txt from {robots_url}: {e}")
            return []
    def walk_sitemap_generator(self) -> Generator[Tuple[str, str], None, None]:
        """
        Generator to walk through the main sitemap and its sub-sitemaps recursively, yielding URLs.

        Yields:
            Tuple[str, str]: A tuple containing the current sitemap URL and a discovered URL.
        """
        # Initialize the set of URLs to visit with the main sitemap and any additional feeds
        urls_to_visit = set(self.fetch_robots_txt(self.main_sitemap) or [urljoin(self.main_sitemap, "/sitemap.xml")])
        urls_to_visit.update(self.feeds)

        visited_sitemaps = set()
        visited_urls = set()

        # Load checkpoint if available
        urls_to_visit, visited_sitemaps, visited_urls = self.load_checkpoint(urls_to_visit, visited_sitemaps, visited_urls)

        # Initialize progress bar for sitemap processing
        with tqdm(total=len(urls_to_visit), desc="Processing sitemaps", unit="sitemap") as pbar:
            while urls_to_visit:
                current_url = urls_to_visit.pop()
                current_url = self.normalize_url(current_url)

                # Skip already visited sitemaps or URLs not in allowed domains
                if current_url in visited_sitemaps or not self.is_allowed_domain(current_url):
                    continue

                visited_sitemaps.add(current_url)
                # Fetch URLs and sub-sitemaps from the current sitemap
                urls, sitemaps = self.get_sitemap_urls(current_url)
                self.update_context_graph(current_url, urls)

                # Normalize and filter new sitemaps
                new_sitemaps = set(self.normalize_url(url) for url in sitemaps) - visited_sitemaps
                urls_to_visit.update(new_sitemaps)

                # Filter and score URLs to prioritize more relevant links
                filtered_urls = self.filter_and_score_urls(urls)
                for url in filtered_urls:
                    url = self.normalize_url(url)
                    # Yield URLs that have not been visited and belong to allowed domains
                    if url not in visited_urls and self.is_allowed_domain(url):
                        visited_urls.add(url)
                        yield (current_url, url)

                # If a URL matches the page-like sitemap pattern, add it to be visited
                for url in urls:
                    normalized_url = self.normalize_url(url)
                    if self.matches_path(url, self.page_like_sitemap) and normalized_url not in visited_sitemaps:
                        urls_to_visit.add(normalized_url)

                # Update the progress bar and sleep briefly to avoid overwhelming the server
                pbar.total = len(urls_to_visit) + len(visited_sitemaps)
                time.sleep(0.1)
                pbar.update(1)

                # Periodically save incremental checkpoints
                if len(visited_sitemaps) % self.dynamic_checkpoint_interval(len(visited_sitemaps), len(urls_to_visit) + len(visited_sitemaps)) == 0:
                    self.async_save_checkpoint(urls_to_visit, visited_sitemaps, visited_urls, incremental=True)

                # Save full checkpoint less frequently
                if len(visited_sitemaps) % 500 == 0:
                    self.async_save_checkpoint(urls_to_visit, visited_sitemaps, visited_urls, incremental=False)
    
@cached(cache=TTLCache(maxsize=1000, ttl=86400))
def search_keywords_in_url(url: str, keywords: List[str]) -> Optional[str]:
    """
    Fetch a webpage and search for specific keywords in its text content.

    Args:
    - url (str): The URL of the webpage to fetch.
    - keywords (List[str]): A list of keywords to search for in the webpage content.

    Returns:
    - Optional[str]: The first keyword found in the content, or None if no keyword is found.
    """
    try:
        # Make a GET request to fetch the webpage content
        with requests.get(url, headers={'User-Agent': 'Mozilla/5.0'}, timeout=30) as response:
            response.raise_for_status()  # Ensure the request was successful

            # Parse the webpage using BeautifulSoup
            soup = BeautifulSoup(response.text, "html.parser")
            
            # Remove script, style, and other non-content tags
            for script in soup(["script", "style", "noscript", "header", "footer", "aside"]):
                script.extract()

            # Extract and lowercase the page's text content
            page_text = soup.get_text(separator=' ', strip=True).lower()

            # Search for keywords in the page text
            for keyword in keywords:
                if keyword.lower() in page_text:
                    return keyword  # Return the first matching keyword found
        
        # Return None if no keyword is found
        return None
    except requests.RequestException as e:
        logging.error(f"Error fetching page {url}: {e}")
        return None

def main():
    # Define the input configuration
    main_sitemap = "https://www.bbc.co.uk"
    target_paths = ["bbc.co.uk/news/", "bbc.co.uk/sport/"]
    anti_target_paths = ["bbc.co.uk/sport/topics/", "bbc.co.uk/news/topics/", "bbc.co.uk/news/business/topics/"]
    allowed_domains = ["bbc.co.uk", "bbci.co.uk", "feeds.bbci.co.uk"]
    feeds = [
        "http://feeds.bbci.co.uk/news/rss.xml", 
        "http://feeds.bbci.co.uk/news/world/rss.xml", 
        "http://feeds.bbci.co.uk/news/business/rss.xml", 
        "http://feeds.bbci.co.uk/news/politics/rss.xml", 
        "http://feeds.bbci.co.uk/news/education/rss.xml", 
        "http://feeds.bbci.co.uk/news/science_and_environment/rss.xml", 
        "http://feeds.bbci.co.uk/news/technology/rss.xml", 
        "http://feeds.bbci.co.uk/news/entertainment_and_arts/rss.xml"
    ]
    page_like_sitemap = ["bbc.co.uk/sport/", "bbc.co.uk/news/"]

    # Initialize SitemapCrawler with provided arguments
    crawler = SitemapCrawler(main_sitemap, target_paths, anti_target_paths, allowed_domains, feeds, page_like_sitemap)

    # Create the generator for URLs from the sitemap
    url_generator = crawler.walk_sitemap_generator()

    # Open the CSV file to write results
    with open('bbc_news_keywords_sitemap.csv', mode='a', newline='', encoding='utf-8') as csv_file:
        csv_writer = csv.writer(csv_file)
        if csv_file.tell() == 0:  # Check if file is empty to write the header
            csv_writer.writerow(['URL', 'Keyword'])

        # Use ThreadPoolExecutor to concurrently search for keywords in URLs
        with ThreadPoolExecutor(max_workers=10) as executor:
            # Submit tasks for all URLs to search for keywords
            future_to_url = {executor.submit(search_keywords_in_url, url, Little_List): url for _, url in url_generator}

            # Process each completed future
            for future in as_completed(future_to_url):
                url = future_to_url[future]
                try:
                    keyword_found = future.result()
                    if keyword_found:
                        with csv_lock:  # Ensure thread-safe access
                            csv_writer.writerow([url, keyword_found])
                            logging.info(f"Keyword found in {url}: {keyword_found}")
                except Exception as e:
                    logging.error(f"Error processing URL {url}: {e}")

if __name__ == "__main__":
    # Configure logging
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    main()
