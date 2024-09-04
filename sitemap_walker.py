import csv
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import ssl
from requests.packages.urllib3.exceptions import InsecureRequestWarning
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
import gzip
from io import BytesIO
from tqdm import tqdm
import re
import numpy as np
import time

from bbc_transphobia_pdf_scraper import KEYWORDS  # Added for request delay handling

requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

class SSLAdapter(HTTPAdapter):
    """An HTTP adapter that uses a client SSL context."""

    def __init__(self, ssl_context=None, **kwargs):
        self.ssl_context = ssl_context
        super().__init__(**kwargs)

    def init_poolmanager(self, *args, **kwargs):
        kwargs['ssl_context'] = self.ssl_context
        super().init_poolmanager(*args, **kwargs)

class SitemapCrawler:
    def __init__(self, main_sitemap, target_paths, anti_target_paths, allowed_domains, feeds=None,page_like_sitemap=None):
        self.main_sitemap = main_sitemap
        self.target_paths = sorted(target_paths, key=len, reverse=True)  # Sort by length for precedence
        self.anti_target_paths = sorted(anti_target_paths, key=len, reverse=True)  # Sort by length for precedence
        self.allowed_domains = [domain.lower() for domain in allowed_domains]  # Ensure domains are in lowercase
        self.feeds = feeds or []  # List of RSS and Atom feeds
        self.page_like_sitemap = page_like_sitemap or []


    def create_ssl_session(self, verify_ssl=True):
        """Create a session that uses a client SSL/TLS context with retry strategy."""
        session = requests.Session()
        retries = Retry(
            total=5,
            backoff_factor=1,
            status_forcelist=[403, 500, 502, 503, 504],
            raise_on_status=False  # Do not raise exceptions on retryable statuses
        )
        context = ssl.create_default_context(ssl.Purpose.SERVER_AUTH)
        context.minimum_version = ssl.TLSVersion.TLSv1_2

        if not verify_ssl:
            context.check_hostname = False
            context.verify_mode = ssl.CERT_NONE

        adapter = SSLAdapter(ssl_context=context, max_retries=retries)
        session.mount('https://', adapter)
        return session

    def fetch_robots_txt(self, url, session):
        """Fetch and parse the robots.txt file to find sitemap URLs."""
        robots_url = urljoin(url, "/robots.txt")
        try:
            response = session.get(robots_url, headers={'User-Agent': 'Mozilla/5.0'}, timeout=10, verify=False)
            response.raise_for_status()
            sitemap_urls = self.parse_robots_txt(response.text)
            return sitemap_urls
        except requests.exceptions.RequestException as e:
            return []

    def parse_robots_txt(self, content):
        """Parse robots.txt content to extract sitemap URLs."""
        return [line.split(":", 1)[1].strip() for line in content.splitlines() if line.lower().startswith("sitemap:")]

    def is_valid_url(self, url):
        """Check if the URL is valid and resolvable."""
        parsed = urlparse(url)
        return bool(parsed.scheme) and bool(parsed.netloc) and not (parsed.netloc.endswith('.local') or parsed.netloc.startswith('localhost'))

    def path_matches(self, url, path_pattern):
        return (path_pattern.lower() in url.lower())

    def matches_path(self, url, paths):
        """Check if the URL matches any of the provided paths, giving precedence to longer paths."""
        for path in paths:
            if self.path_matches(url, path):
                return True
        return False

    def get_sitemap_urls(self, url, session):
        """Retrieve URLs from a sitemap without filtering them out."""
        try:
            if not self.is_valid_url(url):
                return [], []

            # Perform a HEAD request to check the URL quickly
            head_response = session.head(url, headers={'User-Agent': 'Mozilla/5.0'}, timeout=5, verify=False)
            if head_response.status_code != 200:
                return [], []

            # Perform a GET request to retrieve the sitemap content
            response = session.get(url, headers={'User-Agent': 'Mozilla/5.0'}, timeout=10, verify=False)
            response.raise_for_status()
            content_type = response.headers.get('Content-Type', '')
            # Determine the content type and parse accordingly
            if 'application/rss+xml' in content_type or 'application/atom+xml' in content_type or ".rss" in url:
                return self.parse_rss_feed(response.content, url)
            elif 'application/xml' in content_type or 'text/xml' in content_type:
                # Check if the XML content is actually an RSS or Atom feed
                if b'<rss' in response.content or b'<feed' in response.content:  # Simple check for RSS or Atom feed
                    return self.parse_rss_feed(response.content, url)
                else:
                    return self.parse_xml_sitemap(response.content, url)
            elif 'text/plain' in content_type:
                return self.parse_text_sitemap(response.text, url)
            elif 'text/html' in content_type:
                return self.parse_html_sitemap(response.text, url)
            elif url.endswith('.gz'):
                return self.parse_gzip_sitemap(response.content, url)
            else:
                print("oh shit! we dont know what to do with",url,content_type)
                return [], []  # Ensure consistent return format
        except requests.exceptions.RequestException as e:
            return [], []  # Ensure consistent return format

    def parse_xml_sitemap(self, xml_content, base_url):
        """Parse XML sitemap and return URLs and sitemaps."""
        soup = BeautifulSoup(xml_content, 'xml')
        x_urls=[]
        # Extract URLs within <url> elements
        urls = [loc for loc in soup.find_all('url')]
        for url in urls:
            x_urls.append(url.find('loc').text)
        # Extract sitemaps within <sitemap> elements
        sitemaps = [loc.text for loc in soup.find_all('sitemap')]

        return x_urls, sitemaps

    def parse_text_sitemap(self, text_content, base_url):
        """Parse plain text sitemap and return URLs."""
        urls = [line.strip() for line in text_content.splitlines() if line.strip()]
        return urls,[]

    def parse_html_sitemap(self, html_content, base_url):
        """Parse HTML sitemap and return URLs."""
        soup = BeautifulSoup(html_content, 'html.parser')
        urls = []
        for link in soup.find_all('a', href=True):
            href = link['href']
            full_url = urljoin(base_url, href)
            if self.is_valid_url(full_url):
                urls.append(full_url)
        return urls , []

    def parse_gzip_sitemap(self, gz_content, base_url):
        """Parse a .gz (Gzip) compressed sitemap and return URLs."""
        try:
            with gzip.GzipFile(fileobj=BytesIO(gz_content)) as gz:
                decompressed_content = gz.read()
            return self.parse_xml_sitemap(decompressed_content, base_url)
        except Exception as e:
            print(f"Error parsing Gzip sitemap from {base_url}: {e}")
            return [],[]

    def parse_rss_feed(self, rss_content, base_url):
        """Parse RSS feed and return URLs."""
        try:
            soup = BeautifulSoup(rss_content, 'xml')
            urls = [item.find('link').text for item in soup.find_all('item') if item.find('link')]
            return urls,[]
        except Exception as e:
            print(f"Error parsing RSS feed from {base_url}: {e}")
            return [],[]

    def filter_and_score_urls(self, urls):
        """Filter URLs by target paths, exclude anti-target paths, and score them using a model."""
        relevant_urls = []
        for url in urls:
            if self.matches_path(url, self.anti_target_paths):
                continue
            if self.matches_path(url, self.target_paths) or len(self.target_paths) == 0:
                relevant_urls.append(url)
        return relevant_urls

    def is_allowed_domain(self, url):
        """Check if a URL belongs to any of the allowed domains or their subdomains."""
        parsed_url = urlparse(url)
        domain = parsed_url.hostname.lower()
        for  allowed_domain in self.allowed_domains :
            if domain in allowed_domain.lower() or domain.endswith("." + allowed_domain.lower()):
                return True
        return False

    def walk_sitemap_generator(self):
        """Generator to walk through sitemap and sub-sitemaps recursively and yield URLs."""
        session = self.create_ssl_session(verify_ssl=False)
        # Initialize URLs to visit with sitemaps and feeds
        urls_to_visit = set(self.fetch_robots_txt(self.main_sitemap, session) or [urljoin(self.main_sitemap, "/sitemap.xml")])
        urls_to_visit.update(self.feeds)

        visited_sitemaps = set()  # Set of visited sitemap URLs
        visited_urls = set()  # Set of visited URLs

        with tqdm(total=len(urls_to_visit), desc="Processing sitemaps", unit="sitemap") as pbar:
            while urls_to_visit:
                current_url = urls_to_visit.pop()
                if current_url in visited_sitemaps or not self.is_allowed_domain(current_url):
                    continue

                visited_sitemaps.add(current_url)
                urls, sitemaps = self.get_sitemap_urls(current_url, session)

                # Update URLs and sitemaps to visit
                new_sitemaps = set(sitemaps) - visited_sitemaps
                urls_to_visit.update(new_sitemaps)

                # Process and yield relevant URLs
                filtered_urls = self.filter_and_score_urls(urls)
                for url in filtered_urls:
                    if url not in visited_urls and self.is_allowed_domain(url):
                        visited_urls.add(url)
                        yield (current_url, url)  # Yield current sitemap URL and the discovered URL

                # Handle additional pages that look like sitemaps
                for url in urls:
                    if self.matches_path(url, self.page_like_sitemap) and url not in visited_sitemaps:
                        urls_to_visit.add(url)

                # Update progress bar and manage rate limiting
                pbar.total = len(urls_to_visit) + len(visited_sitemaps)
                time.sleep(1)  # Delay to avoid rate limiting; adjust or make conditional
                pbar.update(1)

def search_keywords_in_url(url, KEYWORDS):
    """Fetch a webpage and search for specific keywords in its text content."""
    try:
        response = requests.get(url, timeout=5)
        if response.status_code != 200:
            return None
        
        soup = BeautifulSoup(response.text, "html.parser")
        for script in soup(["script", "noscript"]):
            script.extract()
        page_text = soup.get_text().lower()
        
        for keyword in KEYWORDS:
            if keyword.lower() in page_text:
                return keyword
        return None
    except requests.RequestException as e:
        logging.error(f"Error fetching page {url}: {e}")
        return None


import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def main():
    main_sitemap = "https://www.bbc.co.uk"
    target_paths = [
        "bbc.co.uk/news/", 
        "bbc.co.uk/sport/",
    ]
    anti_target_paths = [
        "bbc.co.uk/sport/topics/",
        "bbc.co.uk/news/topics/",
        "bbc.co.uk/news/business/topics/"
    ]
    allowed_domains = ["bbc.co.uk", "bbci.co.uk", "feeds.bbci.co.uk"]
    feeds = [
        "http://feeds.bbci.co.uk/news/rss.xml", 
        "http://feeds.bbci.co.uk/news/world/rss.xml", 
        "http://feeds.bbci.co.uk/news/business/rss.xml", 
        "http://feeds.bbci.co.uk/news/politics/rss.xml", 
        "http://feeds.bbci.co.uk/news/education/rss.xml", 
        "http://feeds.bbci.co.uk/news/science_and_environment/rss.xml", 
        "http://feeds.bbci.co.uk/news/technology/rss.xml", 
        "http://feeds.bbci.co.uk/news/entertainment_and_arts/rss.xml",
        
    ]
    page_like_sitemap = [
        "bbc.co.uk/sport/",
        "bbc.co.uk/news/"
    ]
    crawler = SitemapCrawler(main_sitemap, target_paths, anti_target_paths, allowed_domains, feeds, page_like_sitemap)
    url_generator = crawler.walk_sitemap_generator()

    try:
        with open('bbc_news_keywords_sitemap.csv', mode='w', newline='', encoding='utf-8') as csv_file:
            csv_writer = csv.writer(csv_file)
            csv_writer.writerow(['URL', 'Keyword'])
            for sitemap, url in url_generator:
                if not any(char.isdigit() for char in url):
                    continue
                try:
                    keyword_found = search_keywords_in_url(url, KEYWORDS)
                    if keyword_found:
                        csv_writer.writerow([url, keyword_found])
                        logging.info(f"Keyword found in {url}: {keyword_found}")
                except requests.exceptions.RequestException as e:
                    logging.error(f"Failed to fetch {url}: {e}")
                except Exception as e:
                    logging.error(f"An unexpected error occurred with {url}: {e}")
    except KeyboardInterrupt:
        logging.warning("Process interrupted by user. Exiting gracefully...")
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")

if __name__ == "__main__":
    main()

