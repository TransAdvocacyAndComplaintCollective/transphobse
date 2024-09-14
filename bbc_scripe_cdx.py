import csv
from typing import Optional
from cachetools import TTLCache, cached
import requests
import time
import logging
import json
import os
import concurrent.futures
from requests.exceptions import ConnectionError, HTTPError, Timeout, RequestException
from bs4 import BeautifulSoup
import progressbar
import ahocorasick  # Aho-Corasick library for efficient multi-keyword search
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
from utils.keywords import KEYWORDS

# Set up logging for debugging and monitoring
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

CHECKPOINT_FILE = 'checkpoint.json'

def preprocess_text(text: str) -> str:
    """Preprocess the text by removing punctuation, stop words, and normalizing whitespace."""
    # Tokenize and remove punctuation
    tokens = word_tokenize(text)
    # Remove stop words
    stop_words = set(stopwords.words('english'))
    filtered_tokens = [word for word in tokens if word.isalnum() and word.lower() not in stop_words]
    # Rejoin tokens into a single string
    return ' '.join(filtered_tokens)



def build_automaton(keywords) -> ahocorasick.Automaton:
    """Build an Aho-Corasick Automaton for fast multi-keyword search."""
    A = ahocorasick.Automaton()
    for idx, keyword in enumerate(keywords):
        A.add_word(keyword.lower(), (idx, keyword))
    A.make_automaton()
    return A

def extract_text_content(soup: BeautifulSoup) -> str:
    """Extract and clean text content from a BeautifulSoup object, removing non-content elements."""
    # Remove non-content elements
    for tag in soup(['script', 'noscript', 'style', 'header', 'footer', 'aside', 'nav']):
        tag.decompose()
    
    # Get cleaned text
    return soup.get_text(separator=' ', strip=True)


def save_checkpoint(checkpoint_data):
    """Save the checkpoint data to a file."""
    try:
        with open(CHECKPOINT_FILE, 'w') as f:
            json.dump(checkpoint_data, f)
    except IOError as e:
        logging.error(f"Failed to save checkpoint: {e}")

def load_checkpoint():
    """Load the checkpoint data from a file."""
    if os.path.exists(CHECKPOINT_FILE):
        try:
            with open(CHECKPOINT_FILE, 'r') as f:
                return json.load(f)
        except (IOError, json.JSONDecodeError) as e:
            logging.error(f"Failed to load checkpoint: {e}")
            return create_default_checkpoint()
    return create_default_checkpoint()

def create_default_checkpoint():
    """Create a default checkpoint structure."""
    return {
        'cdx_index': 0,
        'offset': 0,
        'processed_urls': [],
        'cdx_processed': False
    }

def fetch_with_retry(url, max_retries, backoff_factor, params=None):
    """Fetch data from a URL with retries and exponential backoff."""
    try:
        logging.info(f"Fetching URL: {url} with params: {params}")
        response = requests.get(url, params=params, timeout=20)
        response.raise_for_status()
        data = [json.loads(line) for line in response.text.splitlines()]
        return True, data
    except (ConnectionError, HTTPError, Timeout) as e:
        pass
    except (json.JSONDecodeError, ValueError) as e:
        logging.error(f"JSON decode error for {url}: {e}")
        return False, []
    except RequestException as e:
        logging.error(f"An error occurred: {e}")
        return False, []
    
    logging.error(f"Failed to fetch data from {url} after {max_retries} attempts.")
    return False, []

def get_total_items(base_url, url_pattern, max_retries, backoff_factor):
    """Calculate the total number of items to fetch from the Common Crawl index."""
    query_count_url = f"{base_url}?url={url_pattern}&output=json&showNumPages=true"
    success, data = fetch_with_retry(query_count_url, max_retries, backoff_factor)
    
    if success and data:
        try:
            if isinstance(data, list) and data and isinstance(data[0], dict):
                page_size = data[0].get('pageSize', 0)
                pages = data[0].get('pages', 0)
                return page_size * pages
        except (TypeError, AttributeError) as e:
            logging.error(f"Error processing total items data: {e}")
    return 0



def fetch_wayback_bbc_news(url_pattern, max_retries=5, backoff_factor=2):
    """
    Fetch BBC news URLs from the Wayback Machine using a URL pattern.

    Parameters:
    - url_pattern (str): The URL pattern to search for in the Wayback Machine.
    - max_retries (int): Maximum number of retries in case of errors.
    - backoff_factor (int): Factor by which the backoff time increases after each retry.

    Returns:
    - urls (list): A list of URLs fetched from the Wayback Machine.
    """
    from datetime import datetime

    urls = []
    this_year = datetime.now().year
    for year in range(1996, this_year):
        wayback_page_count_url = f"http://web.archive.org/cdx/search/cdx?url={url_pattern}&filter=statuscode:200&collapse=urlkey&matchType=prefix&fl=original&from={year}&to={year}"
        for attempt in range(max_retries):
            try:
                logging.info(f"Attempt {attempt + 1}: Fetching Wayback Machine data for pattern: {url_pattern}")
                response = requests.get(wayback_page_count_url, timeout=20)
                response.raise_for_status()

                # Parse response and extract URLs
                urls = response.text.splitlines()
    
                logging.info(f"Successfully fetched {len(urls)} URLs from Wayback Machine.")
                return urls
            
            except (ConnectionError, HTTPError, Timeout) as e:
                wait_time = backoff_factor * (2 ** attempt)  # Exponential backoff
                logging.error(f"Error fetching Wayback Machine indexes: {e}. Retrying in {wait_time} seconds...")
                time.sleep(wait_time)
            except requests.RequestException as e:
                logging.error(f"An error occurred while fetching Wayback Machine indexes: {e}")
                break
    
    logging.error(f"Failed to fetch data from Wayback Machine after {max_retries} attempts.")
    return urls

    


def fetch_commoncrawl_bbc_news(url_pattern, max_retries=5, backoff_factor=2):
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

    with progressbar.ProgressBar(max_value=len(cc_cdxs), redirect_stdout=True, widgets=[
        progressbar.Bar('=', '[', ']'), ' ', progressbar.Percentage(), ' ', progressbar.ETA()
    ]) as outer_bar:
        for index, cdx in enumerate(cc_cdxs):
            if index < checkpoint_data['cdx_index']:
                outer_bar.update(index + 1)
                continue
            
            base_url = cdx['cdx-api']
            total_items = get_total_items(base_url, url_pattern, max_retries, backoff_factor)
            if total_items <= 0:
                outer_bar.update(index + 1)
                continue

            offset = checkpoint_data['offset'] if index == checkpoint_data['cdx_index'] else 0

            with progressbar.ProgressBar(max_value=total_items, redirect_stdout=True, widgets=[
                progressbar.Bar('=', '[', ']'), ' ', progressbar.Percentage(), ' ', progressbar.ETA()
            ]) as inner_bar:
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
                        if offset > total_items:
                            offset = total_items
                        checkpoint_data.update({'cdx_index': index, 'offset': offset, 'processed_urls': list(urls)})
                        save_checkpoint(checkpoint_data)
                        inner_bar.update(offset)
                    else:
                        break

                inner_bar.finish()
            outer_bar.update(index + 1)

            checkpoint_data.update({'cdx_index': index + 1, 'offset': 0})
            save_checkpoint(checkpoint_data)

@cached(cache=TTLCache(maxsize=1000, ttl=86400))
def search_keywords_in_url(url: str, keywords, automaton: ahocorasick.Automaton) -> Optional[str]:
    """Fetch a webpage, preprocess its content, and search for specific keywords using Aho-Corasick."""
    try:
        with requests.get(url, headers={'User-Agent': 'Mozilla/5.0'}, timeout=30) as response:
            response.raise_for_status()

            soup = BeautifulSoup(response.text, "html.parser")
            page_text = extract_text_content(soup)
            preprocessed_text = preprocess_text(page_text)

            # Search for keywords using Aho-Corasick automaton
            for end_index, (idx, keyword) in automaton.iter(preprocessed_text):
                return keyword  # Return the first matched keyword

        return None
    except requests.RequestException as e:
        logging.error(f"Error fetching page {url}: {e}")
        return None

def main():
    url_pattern = "bbc.co.uk/news/*"

    with open('bbc_news_keywords.csv', mode='w', newline='', encoding='utf-8') as csv_file:
        csv_writer = csv.writer(csv_file)
        csv_writer.writerow(['URL', 'Keyword'])

        for url in fetch_commoncrawl_bbc_news(url_pattern):
            keyword_found = search_keywords_in_url(url, KEYWORDS)
            if keyword_found:
                csv_writer.writerow([url, keyword_found])
            else:
                logging.info(f"Keyword not found in: {url}")
        
        for url in fetch_wayback_bbc_news(url_pattern):
            keyword_found = search_keywords_in_url(url, KEYWORDS)
            if keyword_found:
                csv_writer.writerow([url, keyword_found])
            else:
                logging.info(f"Keyword not found in: {url}")

if __name__ == "__main__":
    main()
