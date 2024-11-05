import csv
import datetime
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
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
from utils.keywords import KEYWORDS
import urllib.parse


# Set up logging for debugging and monitoring
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

CHECKPOINT_FILE = "checkpoint_cdx.json"

# Preprocessing functions
def preprocess_text(text: str) -> str:
    """Preprocess the text by removing punctuation, stop words, and normalizing whitespace."""
    tokens = word_tokenize(text)
    stop_words = set(stopwords.words("english"))
    filtered_tokens = [
        word for word in tokens if word.isalnum() and word.lower() not in stop_words
    ]
    return " ".join(filtered_tokens)


def extract_text_content(soup: BeautifulSoup) -> str:
    """Extract and clean text content from a BeautifulSoup object, removing non-content elements."""
    for tag in soup(["script", "noscript", "style", "header", "footer", "aside", "nav"]):
        tag.decompose()
    return soup.get_text(separator=" ", strip=True)


# Aho-Corasick Automaton
def build_automaton(keywords: List[str]) -> ahocorasick.Automaton:
    """Build an Aho-Corasick Automaton for fast multi-keyword search."""
    A = ahocorasick.Automaton()
    for idx, keyword in enumerate(keywords):
        A.add_word(keyword.lower(), (idx, keyword))
    A.make_automaton()
    return A


# Checkpoint management
def create_default_checkpoint() -> Dict:
    """Create a default checkpoint structure for both Wayback and Common Crawl."""
    return {
        "wayback": {
            "year": 1996,                     # Tracks which year we're processing
            "mimetype": None,                 # Tracks the MIME type being processed
            "offset": 0,                      # Offset for the Wayback pagination
            "processed_urls": [],             # Successfully processed URLs
            "cached_cdx_entries": [],         # Cached CDX entries for processing
            "failed_cdx_requests": [],         # Failed CDX requests (e.g., due to 404)
            "cached_entries": []              # Cached CDX entries for processing
        },
        "commoncrawl": {
            "base_url": None,                 # Current base URL of Common Crawl index
            "mimetype": None,                 # Tracks the MIME type being processed
            "offset": 0,                      # Offset for the Common Crawl pagination
            "cdx_index": 0,                   # Current CDX index being processed
            "processed_urls": [],             # Successfully processed URLs
            "cached_cdx_entries": [],         # Cached CDX entries for processing
            "failed_cdx_requests": [],         # Failed CDX requests (e.g., due to 404)
            "cached_entries": []              # Cached CDX entries for processing
        }
    }


def save_checkpoint(checkpoint_data: Dict) -> None:
    """Save the checkpoint data to a file."""
    try:
        with open(CHECKPOINT_FILE, "w") as f:
            json.dump(checkpoint_data, f, indent=4)
    except IOError as e:
        logging.error(f"Failed to save checkpoint: {e}")



def load_checkpoint() -> Dict:
    """Load the checkpoint data from a file or create a default checkpoint if not present."""
    if os.path.exists(CHECKPOINT_FILE):
        try:
            with open(CHECKPOINT_FILE, "r") as f:
                checkpoint_data = json.load(f)

                # Ensure 'commoncrawl' and 'wayback' keys are present
                for key in ['commoncrawl', 'wayback']:
                    if key not in checkpoint_data:
                        checkpoint_data[key] = {
                            "base_url": None,
                            "mimetype": None,
                            "offset": 0,
                            "cdx_index": 0,
                            "processed_urls": [],
                            "cached_cdx_entries": [],
                            "failed_cdx_requests": []
                        }
                    else:
                        # Ensure all expected keys are present in 'commoncrawl' and 'wayback'
                        checkpoint_data[key].setdefault("failed_cdx_requests", [])
                        checkpoint_data[key].setdefault("processed_urls", [])
                        checkpoint_data[key].setdefault("cached_cdx_entries", [])
                return checkpoint_data
        except (IOError, json.JSONDecodeError) as e:
            logging.error(f"Failed to load checkpoint: {e}")

    # If the file does not exist or an error occurs, create a new default checkpoint
    logging.info("Checkpoint file does not exist or failed to load. Creating a new one.")
    return create_default_checkpoint()


def fetch_with_retry(url, max_retries, backoff_factor, timeout=200, json_lines=False):
    """Fetch data from a URL with retries and exponential backoff, handling both JSON and NDJSON formats."""
    custom_headers = {
        "User-Agent": "YourScriptName/1.0 (http://yourwebsite.com; your_email@example.com)"
    }

    response = None
    for attempt in range(max_retries):
        try:
            response = requests.get(url, headers=custom_headers, timeout=timeout)
            if response.status_code == 404:
                return False, None, 404
            elif response.status_code == 200:
                content_type = response.headers.get('Content-Type', '')

                # Standard JSON response
                if 'application/json' in content_type:
                    text = response.text
                    output = json.loads(text)
                    return True, output, 200

                # NDJSON response
                elif 'text/x-ndjson' in content_type:
                    lines = response.text.strip().split("\n")
                    output = [json.loads(line) for line in lines if line.strip()]
                    return True, output, 200

                # Unexpected content type
                else:
                    return False, None, response.status_code
            else:
                response.raise_for_status()
        except (requests.exceptions.HTTPError, requests.exceptions.RequestException, json.JSONDecodeError) as e:
            if attempt < max_retries - 1:
                sleep_time = backoff_factor * (2 ** attempt)
                logging.info(f"Waiting for {sleep_time:.2f} seconds before retrying...")
                time.sleep(sleep_time)
            else:
                return False, None, response.status_code if response else None



# Total item calculation for Common Crawl
def get_total_items(base_url: str, url_pattern: str, mime_filter: str, max_retries: int, backoff_factor: int) -> int:
    """Calculate the total number of items to fetch from the Common Crawl index."""
    query_count_url = f"{base_url}?url={url_pattern}&filter=mimetype:{mime_filter}&output=json&showNumPages=true"
    success, data, status_code = fetch_with_retry(query_count_url, max_retries, backoff_factor, json_lines=False)
    
    if status_code == 404:
        return 0

    if success and data:
        try:
            if isinstance(data, list):
                # Handle the case where the response is a list
                for item in data:
                    total_pages = item.get("pages", 0)
                    page_size = item.get("pageSize", 0)
                    if total_pages and page_size:
                        total_items = total_pages * page_size
                        logging.info(f"Total items calculated: {total_items}")
                        return total_items
            elif isinstance(data, dict):
                # Original case where the response is a dictionary
                total_pages = data.get("pages", 0)
                page_size = data.get("pageSize", 0)
                total_items = total_pages * page_size
                logging.info(f"Total items calculated: {total_items}")
                return total_items
        except (TypeError, AttributeError) as e:
            logging.error(f"Error parsing total items from data: {e}")
    return 0



# Fetching from Wayback Machine

# Progress bars added to fetch_wayback
def fetch_wayback(url_pattern: str, max_retries: int = 2, backoff_factor: int = 2) -> list:
    """Fetch BBC news URLs from the Wayback Machine using a URL pattern, with year-sensitive checkpointing."""
    checkpoint_data = load_checkpoint()
    wayback_checkpoint = checkpoint_data.get("wayback", {})
    urls_output = set()
    this_year = datetime.datetime.now().year

    # Initialize processed URLs and failed requests sets
    processed_urls = set(wayback_checkpoint.get("processed_urls", []))
    failed_requests = set(wayback_checkpoint.get("failed_cdx_requests", []))
    cached_cdx_entries = set(wayback_checkpoint.get("cached_cdx_entries", []))

    cached_entries = wayback_checkpoint.get("cached_cdx_entries", [])
    if cached_entries:
        logging.info("Processing cached Wayback CDX entries.")
        for entry in cached_entries:
            url = entry.get("url")
            if url is None:
                continue
            mime = entry["mime"]
            if url not in processed_urls:  # Skip if already processed
                yield (url, mime)
                processed_urls.add(url)
        wayback_checkpoint["cached_cdx_entries"] = []
        wayback_checkpoint["processed_urls"] = list(processed_urls)
        save_checkpoint(checkpoint_data)

    for mime in [
        ("application/xhtml+xml", "html"),
        ("text/html", "html"),
        ("application/xml", "feed"),
        ("text/xml", "feed"),
        ("application/rss+xml", "feed"),
        ("application/atom+xml", "feed"),
    ]:
        start_year = wayback_checkpoint.get("year", 1996) if wayback_checkpoint.get("mimetype") == mime[0] else 1996
        wayback_checkpoint["offset"] = 0

        for year in range(start_year, this_year + 1):
            offset = wayback_checkpoint.get("offset", 0)

            while True:
                encoded_url_pattern = urllib.parse.quote(url_pattern, safe='')
                encoded_mime_type = urllib.parse.quote(mime[0], safe='')

                wayback_page_count_url = (
                    f"http://web.archive.org/cdx/search/cdx?url={encoded_url_pattern}&filter=statuscode:200&collapse=urlkey"
                    f"&fl=original&from={year}&to={year}&filter=mimetype:{encoded_mime_type}&offset={offset}&limit=100"
                )
                if wayback_page_count_url in wayback_page_count_url:
                    break
                
                # Skip failed requests
                if wayback_page_count_url in failed_requests:
                    offset += 100
                    continue

                success, urls, status_code = fetch_with_retry(
                    wayback_page_count_url, max_retries, backoff_factor, timeout=100, json_lines=False
                )

                if status_code == 404 or not success:
                    logging.info(f"No more data for year {year} with mimetype {mime[0]}.")
                    failed_requests.add(wayback_page_count_url)
                    wayback_checkpoint["failed_cdx_requests"] = list(failed_requests)
                    save_checkpoint(checkpoint_data)
                    break

                if success and urls:
                    for url in urls:
                        if url not in processed_urls:  # Skip if already processed
                            wayback_checkpoint.setdefault("cached_cdx_entries", []).append({"url": url, "mime": mime})
                            processed_urls.add(url)  # Mark as processed
                    save_checkpoint(checkpoint_data)

                    for entry in wayback_checkpoint["cached_cdx_entries"]:
                        print("entry",entry)
                        url = entry.get("url")
                        if url is None:
                            continue
                        mime = entry["mime"]
                        if url not in processed_urls:  # Skip if already processed
                            yield (url, mime)
                            processed_urls.add(url)
                    wayback_checkpoint["cached_cdx_entries"].append(wayback_page_count_url)
                    save_checkpoint(checkpoint_data)

                    offset += len(urls)
                    wayback_checkpoint["offset"] = offset
                    wayback_checkpoint["year"] = year
                    wayback_checkpoint["mimetype"] = mime[0]
                    wayback_checkpoint["processed_urls"] = list(processed_urls)
                    save_checkpoint(checkpoint_data)
                else:
                    logging.error(f"Failed to fetch data for {year} with mimetype {mime[0]}")
                    failed_requests.add(wayback_page_count_url)
                    wayback_checkpoint["failed_cdx_requests"] = list(failed_requests)
                    save_checkpoint(checkpoint_data)
                    break
                time.sleep(1)

            wayback_checkpoint["offset"] = 0
            save_checkpoint(checkpoint_data)

    logging.info("Completed fetching Wayback Machine URLs.")

try:
    response = requests.get("https://index.commoncrawl.org/collinfo.json", timeout=100)
    response.raise_for_status()
    cc_cdxs = response.json()
except requests.RequestException as e:
    logging.error(f"Error fetching Common Crawl indexes: {e}")

# Progress bars added to fetch_commoncrawl
def fetch_commoncrawl(
    url_pattern: str,
    max_retries: int = 3,
    backoff_factor: int = 1,
    mime_types: Optional[List[Tuple[str, str]]] = None,
    min_limit: int = 5,
    timeout: int = 5
):
    
    """Fetch BBC news URLs from Common Crawl using a URL pattern, with caching and replay of CDX entries."""
    if mime_types is None:
        mime_types = [
            ("text/html", "html"),
            ("text/xml", "feed"),
            ("application/rss+xml", "feed")
        ]

    checkpoint_data = load_checkpoint()
    commoncrawl_checkpoint = checkpoint_data.get("commoncrawl", {})
    urls = set(commoncrawl_checkpoint.get("processed_urls", []))
    failed_requests = set(commoncrawl_checkpoint.get("failed_cdx_requests", []))

    cached_entries = commoncrawl_checkpoint.get("cached_cdx_entries", [])
    if cached_entries:
        logging.info("Processing cached Common Crawl CDX entries.")
        for entry in cached_entries:
            if isinstance(entry, dict):  # Ensure entry is a dictionary
                url = entry.get("url")
                mime = entry.get("mime")
                if url not in urls and url not in failed_requests:  # Skip if already processed
                    yield (url, mime)
                    urls.add(url)
            else:
                logging.warning(f"Skipping non-dictionary entry: {entry}")

        commoncrawl_checkpoint["cached_cdx_entries"] = []
        commoncrawl_checkpoint["processed_urls"] = list(urls)
        save_checkpoint(checkpoint_data)


    cdx_index = commoncrawl_checkpoint.get("cdx_index", 0)

    for index in range(cdx_index, len(cc_cdxs)):
        cdx = cc_cdxs[index]
        base_url = cdx["cdx-api"]
        if base_url != commoncrawl_checkpoint.get("base_url"):
            commoncrawl_checkpoint["base_url"] = base_url
            commoncrawl_checkpoint["offset"] = 0
            save_checkpoint(checkpoint_data)

        for mime in mime_types:
            if mime[0] != commoncrawl_checkpoint.get("mimetype"):
                commoncrawl_checkpoint["mimetype"] = mime[0]
                commoncrawl_checkpoint["offset"] = 0
                save_checkpoint(checkpoint_data)

            total_items = get_total_items(
                base_url, url_pattern, mime[0], max_retries, backoff_factor
            )
            if total_items <= 0:
                continue

            offset = commoncrawl_checkpoint.get("offset", 0)
            while offset < total_items:
                    limit = min(min_limit, total_items - offset)
                    query_url = (
                        f"{base_url}?url={url_pattern}&output=json&fields=url&collapse=digest"
                        f"&collapse=original&filter=mimetype:{mime[0]}&offset={offset}&limit={limit}"
                    )
                    
                    if query_url in commoncrawl_checkpoint["cached_cdx_entries"]:
                        break
                    # Skip failed requests
                    if query_url in failed_requests:
                        logging.info(f"Skipping known failed request: {query_url}")
                        offset += limit
                        continue

                    success, new_entries, status_code = fetch_with_retry(
                        query_url, max_retries, backoff_factor, timeout=timeout, json_lines=True
                    )

                    if status_code == 404:
                        logging.info(f"URL {query_url} returned 404. Skipping.")
                        failed_requests.add(query_url)
                        commoncrawl_checkpoint["failed_cdx_requests"] = list(failed_requests)
                        save_checkpoint(checkpoint_data)
                        break

                    if not success or not new_entries:
                        logging.error(f"Failed to fetch {query_url} or no data returned. Response: {new_entries}")
                        failed_requests.add(query_url)
                        commoncrawl_checkpoint["failed_cdx_requests"] = list(failed_requests)
                        save_checkpoint(checkpoint_data)
                        break

                    for entry in new_entries:
                        if isinstance(entry, dict):  # Ensure that entry is a dictionary
                            url = entry.get("url")
                            if url and url not in urls:  # Skip if already processed
                                commoncrawl_checkpoint.setdefault("cached_cdx_entries", []).append({"url": url, "mime": mime})
                                urls.add(url)  # Mark as processed
                        else:
                            logging.warning(f"Skipping unexpected entry format: {entry}")

                    save_checkpoint(checkpoint_data)

                    # Process the cached entries again to ensure they are updated
                    for entry in commoncrawl_checkpoint["cached_cdx_entries"]:
                        if isinstance(entry, dict):  # Ensure entry is a dictionary
                            url = entry.get("url")
                            mime = entry.get("mime")
                            if url and url not in urls:
                                commoncrawl_checkpoint.setdefault("cached_cdx_entries", []).append({"url": url, "mime": mime})
                                urls.add(url)  # Mark as processed

                    commoncrawl_checkpoint["cached_cdx_entries"].append(query_url)
                    save_checkpoint(checkpoint_data)

                    offset += limit
                    commoncrawl_checkpoint["offset"] = offset
                    commoncrawl_checkpoint["processed_urls"] = list(urls)
                    save_checkpoint(checkpoint_data)

            commoncrawl_checkpoint["offset"] = 0
            save_checkpoint(checkpoint_data)

        commoncrawl_checkpoint["cdx_index"] = index + 1
        commoncrawl_checkpoint["offset"] = 0
        save_checkpoint(checkpoint_data)

    logging.info("Completed fetching Common Crawl URLs.")





# Keyword search using Aho-Corasick
@cached(cache=TTLCache(maxsize=1000, ttl=86400))
def search_keywords_in_url(
    url: str, keywords: Tuple[str, ...], automaton: ahocorasick.Automaton
) -> Optional[str]:
    """Fetch a webpage, preprocess its content, and search for specific keywords using Aho-Corasick."""
    try:
        with requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=30) as response:
            response.raise_for_status()
            soup = BeautifulSoup(response.text, "html.parser")
            page_text = extract_text_content(soup)
            preprocessed_text = preprocess_text(page_text)

            for end_index, (idx, keyword) in automaton.iter(preprocessed_text):
                return keyword  # Return the first matched keyword
    except RequestException as e:
        logging.error(f"Error fetching page {url}: {e}")
    return None


def get_all_urls_cdx(url_pattern):
    for url in fetch_commoncrawl(url_pattern + "*"):
        yield url[0]
    for url in fetch_wayback(url_pattern + "*"):
        yield url[0]


# Main function
def main():
    url_pattern = "bbc.co.uk/news/*"
    automaton = build_automaton(KEYWORDS)
    with open("bbc_news_keywords_cdx.csv", mode="w", newline="", encoding="utf-8") as csv_file:
        csv_writer = csv.writer(csv_file)
        csv_writer.writerow(["URL", "Keyword"])

        for url in fetch_commoncrawl(url_pattern):
            keyword_found = search_keywords_in_url(url, tuple(KEYWORDS), automaton)
            if keyword_found:
                csv_writer.writerow([url, keyword_found])
            else:
                logging.info(f"Keyword not found in: {url}")

        for url in fetch_wayback(url_pattern):
            keyword_found = search_keywords_in_url(url, tuple(KEYWORDS), automaton)
            if keyword_found:
                csv_writer.writerow([url, keyword_found])
            else:
                logging.info(f"Keyword not found in: {url}")


if __name__ == "__main__":
    main()
