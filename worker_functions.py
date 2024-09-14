import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
import logging
import re
import csv
import xml.etree.ElementTree as ET
from redis import Redis

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def scraper_worker(current_url, keywords, anti_keywords, allowed_subdirs, output_csv="crawler_results.csv"):
    """Worker function to process a URL for relevance and follow links."""
    
    # Initialize Redis connection
    redis_conn = Redis()
    
    # Initialize requests session for fetching pages
    session = requests.Session()
    
    def fetch_page(url):
        """Fetches the content of a webpage with appropriate headers and error handling."""
        headers = {'User-Agent': 'Mozilla/5.0 (compatible; KeyPhraseFocusCrawler/1.0)'}
        try:
            response = session.get(url, headers=headers, timeout=10)
            response.raise_for_status()  # Raise HTTPError for bad responses
            return response.text
        except requests.RequestException as e:
            logger.error(f"Error fetching {url}: {e}")
        return None

    def parse_html(html):
        """Parses HTML content and extracts relevant text."""
        soup = BeautifulSoup(html, 'html.parser')
        texts = soup.stripped_strings
        return " ".join(texts)

    def keyword_relevance(text):
        """Calculates the relevance score of a page or anchor text based on keywords."""
        score = sum(text.lower().count(keyword.lower()) for keyword in keywords)
        anti_score = sum(text.lower().count(anti_keyword.lower()) for anti_keyword in anti_keywords)
        found_keywords = [keyword for keyword in keywords if keyword.lower() in text.lower()]
        return score - anti_score, found_keywords

    def is_allowed_subdirectory(url):
        """Check if a URL belongs to the allowed subdirectories."""
        parsed_url = urlparse(url)
        for subdir in allowed_subdirs:
            if parsed_url.path.startswith(subdir):
                return True
        return False

    def save_to_csv(url, relevance, keywords):
        """Save URL and its relevance score to CSV."""
        with open(output_csv, mode='a', newline='', buffering=1) as csv_file:
            csv_writer = csv.writer(csv_file)
            csv_writer.writerow([url, relevance, ", ".join(keywords)])
    
    def mark_as_visited(url):
        """Marks a URL as visited using Redis."""
        redis_conn.sadd('visited_urls', url)
    
    def has_been_visited(url):
        """Check if a URL has already been visited using Redis."""
        return redis_conn.sismember('visited_urls', url)
    
    # Start processing the current URL
    if has_been_visited(current_url) or not is_allowed_subdirectory(current_url):
        return  # Skip if already visited or not allowed

    # Mark the URL as visited
    mark_as_visited(current_url)

    logger.info(f"Visiting {current_url}")
    content = fetch_page(current_url)
    if content:
        text = parse_html(content)
        relevance, found_keywords = keyword_relevance(text)
        logger.info(f"Relevance Score for {current_url}: {relevance}")

        # Save URL and its score to the CSV file
        save_to_csv(current_url, relevance, found_keywords)

        # Extract links to follow and enqueue
        soup = BeautifulSoup(content, 'html.parser')
        links = [(a['href'], a.text.strip()) for a in soup.find_all('a', href=True)]  # Extract href and anchor text
        for link, anchor_text in links:
            full_url = urljoin(current_url, link)
            if is_allowed_subdirectory(full_url) and not has_been_visited(full_url):  # Check if link is allowed and not visited
                anchor_score, _ = keyword_relevance(anchor_text)  # Score based on anchor text
                
                # Here, you'd normally enqueue the link for further processing. This is left out 
                # as it depends on your queuing system setup.
                # Example: enqueue_link(full_url, anchor_score)
                logger.info(f"Found new URL to visit: {full_url} with relevance {anchor_score}")
