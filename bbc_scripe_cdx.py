import csv
import random
from typing import Optional, List, Dict, Tuple
from cachetools import TTLCache, cached
import requests
import time
import logging
import json
import os
import ahocorasick
from requests.exceptions import RequestException
from bs4 import BeautifulSoup
import progressbar
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
from utils.keywords import KEYWORDS

# Set up logging for debugging and monitoring
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

CHECKPOINT_FILE = 'checkpoint_cdx.json'

# Preprocessing functions
def preprocess_text(text: str) -> str:
    """Preprocess the text by removing punctuation, stop words, and normalizing whitespace."""
    tokens = word_tokenize(text)
    stop_words = set(stopwords.words('english'))
    filtered_tokens = [word for word in tokens if word.isalnum() and word.lower() not in stop_words]
    return ' '.join(filtered_tokens)

def extract_text_content(soup: BeautifulSoup) -> str:
    """Extract and clean text content from a BeautifulSoup object, removing non-content elements."""
    for tag in soup(['script', 'noscript', 'style', 'header', 'footer', 'aside', 'nav']):
        tag.decompose()
    return soup.get_text(separator=' ', strip=True)

# Aho-Corasick Automaton
def build_automaton(keywords: List[str]) -> ahocorasick.Automaton:
    """Build an Aho-Corasick Automaton for fast multi-keyword search."""
    A = ahocorasick.Automaton()
    for idx, keyword in enumerate(keywords):
        A.add_word(keyword.lower(), (idx, keyword))
    A.make_automaton()
    return A

# Checkpoint management
def save_checkpoint(checkpoint_data: Dict) -> None:
    """Save the checkpoint data to a file."""
    try:
        with open(CHECKPOINT_FILE, 'w') as f:
            json.dump(checkpoint_data, f)
    except IOError as e:
        logging.error(f"Failed to save checkpoint: {e}")

def load_checkpoint() -> Dict:
    """Load the checkpoint data from a file."""
    if os.path.exists(CHECKPOINT_FILE):
        try:
            with open(CHECKPOINT_FILE, 'r') as f:
                return json.load(f)
        except (IOError, json.JSONDecodeError) as e:
            logging.error(f"Failed to load checkpoint: {e}")
    return create_default_checkpoint()

def create_default_checkpoint() -> Dict:
    """Create a default checkpoint structure."""
    return {'cdx_index': 0, 'offset': 0, 'processed_urls': [], 'cdx_processed': False}

# Data fetching
def fetch_with_retry(
    url: str,
    max_retries: int,
    backoff_factor: int,
    params: Optional[Dict] = None,
    do_json: bool = True
) -> Tuple[bool, List[Dict]]:
    """Fetch data from a URL with retries and exponential backoff."""
    for attempt in range(max_retries):
        try:
            response = requests.get(url, params=params)
            response.raise_for_status()
            
            if response.status_code == 429:  # Rate limit error
                wait_time = int(response.headers.get("Retry-After", backoff_factor * (2 ** attempt)))
                logging.warning(f"Rate limit hit. Retrying after {wait_time} seconds...")
                time.sleep(wait_time)
                continue

            if do_json:
                logging.info(f"Fetching URL: {url} with params: {params}")
                data = [json.loads(line) for line in response.text.splitlines()]
                return True, data
            else:
                output = []
                for line in response.iter_lines():
                    output.append(line.decode('utf-8'))
                return True, output
        
        except (RequestException, json.JSONDecodeError) as e:
            logging.error(f"Error fetching data from {url}: {e}. Retrying...")

            # Randomized exponential backoff
            backoff_time = backoff_factor * (2 ** attempt) + random.uniform(0, 1)
            logging.info(f"Waiting for {backoff_time:.2f} seconds before retrying...")
            time.sleep(backoff_time)
    
    logging.error(f"Failed to fetch data from {url} after {max_retries} attempts.")
    return False, []

def get_total_items(base_url: str, url_pattern: str, max_retries: int, backoff_factor: int) -> int:
    """Calculate the total number of items to fetch from the Common Crawl index."""
    query_count_url = f"{base_url}?url={url_pattern}&output=json&showNumPages=true"
    success, data = fetch_with_retry(query_count_url, max_retries, backoff_factor)
    
    if success and data:
        try:
            if data and isinstance(data[0], dict):
                page_size = data[0].get('pageSize', 0)
                pages = data[0].get('pages', 0)
                return page_size * pages
        except (TypeError, AttributeError) as e:
            logging.error(f"Error processing total items data: {e}")
    return 0

def fetch_wayback_bbc_news(url_pattern: str, max_retries: int = 2, backoff_factor: int = 2) -> List[str]:
    """Fetch BBC news URLs from the Wayback Machine using a URL pattern."""
    from datetime import datetime
    urls_output = set()
    this_year = datetime.now().year
    for year in range(1996, this_year+1):
        wayback_page_count_url = f"http://web.archive.org/cdx/search/cdx?url={url_pattern}&filter=statuscode:200&collapse=urlkey&fl=original&from={year}&to={year}"
        success, urls = fetch_with_retry(wayback_page_count_url, max_retries, backoff_factor,do_json=False)
        if success:
            logging.info(f"Successfully fetched {len(urls)} URLs from Wayback Machine.")
            urls_output.update(urls)
    logging.error(f"Failed to fetch data from Wayback Machine after {max_retries} attempts.")
    return urls_output

def fetch_commoncrawl_bbc_news(url_pattern: str, max_retries: int = 5, backoff_factor: int = 2):
    """Fetch BBC news URLs from Common Crawl using a URL pattern."""
    checkpoint_data = load_checkpoint()
    urls = set(checkpoint_data['processed_urls'])
    
    try:
        response = requests.get("https://index.commoncrawl.org/collinfo.json")
        response.raise_for_status()
        cc_cdxs = response.json()
    except requests.RequestException as e:
        logging.error(f"Error fetching Common Crawl indexes: {e}")
        return []

    all_entries = set()

    for index, cdx in enumerate(cc_cdxs):
        if index < checkpoint_data['cdx_index']:
            continue
        
        base_url = cdx['cdx-api']
        total_items = get_total_items(base_url, url_pattern, max_retries, backoff_factor)
        if total_items <= 0:
            continue

        offset = checkpoint_data['offset'] if index == checkpoint_data['cdx_index'] else 0

        while offset < total_items:
            query_url = f"{base_url}?url={url_pattern}&output=json&fields=url&collapse=digest&collapse=original&offset={offset}"
            success, new_entries = fetch_with_retry(query_url, max_retries, backoff_factor)
            if not success: 
                break

            new_entries_set = set(entry["url"] for entry in new_entries) - urls
            if new_entries_set:
                all_entries.update(new_entries_set)
                urls.update(new_entries_set)
                for entry in new_entries_set:
                    yield entry 
                offset += len(new_entries)
                checkpoint_data.update({'cdx_index': index, 'offset': offset, 'processed_urls': list(urls)})
                save_checkpoint(checkpoint_data)
            else:
                break

        checkpoint_data.update({'cdx_index': index + 1, 'offset': 0})
        save_checkpoint(checkpoint_data)



# Keyword search
@cached(cache=TTLCache(maxsize=1000, ttl=86400))
def search_keywords_in_url(url: str, keywords: Tuple[str, ...], automaton: ahocorasick.Automaton) -> Optional[str]:
    """Fetch a webpage, preprocess its content, and search for specific keywords using Aho-Corasick."""
    try:
        with requests.get(url, headers={'User-Agent': 'Mozilla/5.0'}, timeout=30) as response:
            response.raise_for_status()
            soup = BeautifulSoup(response.text, "html.parser")
            page_text = extract_text_content(soup)
            preprocessed_text = preprocess_text(page_text)

            for end_index, (idx, keyword) in automaton.iter(preprocessed_text):
                return keyword  # Return the first matched keyword
    except RequestException as e:
        logging.error(f"Error fetching page {url}: {e}")
    return None

def get_all_urls(url_pattern):
    urls = set()
    for url in fetch_commoncrawl_bbc_news(url_pattern+"*"):
        urls.add(url)
    for url in fetch_wayback_bbc_news(url_pattern+"*"):
        urls.add(url)
    return urls


# Main function
def main():
    url_pattern = "bbc.co.uk/news/*"
    automaton = build_automaton(KEYWORDS)
    with open('bbc_news_keywords_cdx.csv', mode='w', newline='', encoding='utf-8') as csv_file:
        csv_writer = csv.writer(csv_file)
        csv_writer.writerow(['URL', 'Keyword'])

        # for url in fetch_commoncrawl_bbc_news(url_pattern):
        #     pass
        #     print(url)
        #     # # Convert the keywords list to a tuple to ensure it's hashable
        #     # keyword_found = search_keywords_in_url(url, tuple(KEYWORDS), automaton)
        #     # if keyword_found:
        #     #     csv_writer.writerow([url, keyword_found])
        #     # else:
        #     #     logging.info(f"Keyword not found in: {url}")
        
        for url in fetch_wayback_bbc_news(url_pattern):
            pass
            # Convert the keywords list to a tuple to ensure it's hashable
            # keyword_found = search_keywords_in_url(url, tuple(KEYWORDS), automaton)
            # if keyword_found:
            #     csv_writer.writerow([url, keyword_found])
            # else:
            #     logging.info(f"Keyword not found in: {url}")

if __name__ == "__main__":
    main()
