import csv
import asyncio
import logging
from urllib.parse import urlparse
from typing import Tuple, Dict, Set
import aiohttp
from tqdm.asyncio import tqdm
from utils.BackedURLQueue import URLItem

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Constants
INPUT_CSV_PATH = "/home/lucy/Desktop/transphobse/dataset/postcodes.csv"  # Update with your actual file path
BASE_URL = "https://production.inyourarea.co.uk/facade/feed/"
CONCURRENT_REQUESTS = 10  # Adjust based on your system and API rate limits

# Global set to keep track of processed location IDs
location_skips: Set[str] = set()

async def get_inyourarea_locations(session: aiohttp.ClientSession, postcode: str) -> Dict:
    """
    Fetches location data for a given postcode.
    """
    url = f"{BASE_URL}{postcode}"
    try:
        async with session.get(url) as response:
            if response.status == 200:
                data = await response.json()
                logger.debug(f"Fetched locations for postcode: {postcode}")
                return data
            else:
                logger.warning(f"Failed to fetch locations for postcode: {postcode}, Status Code: {response.status}")
                return {}
    except Exception as e:
        logger.error(f"Exception fetching locations for postcode: {postcode}, Error: {e}")
        return {}

async def get_inyourarea_feed(session: aiohttp.ClientSession, postcode: str, location_id: str) -> Dict:
    """
    Fetches feed data for a specific location within a postcode.
    """
    url = f"{BASE_URL}{postcode}/{location_id}"
    if location_id in location_skips:
        print(f"Skipping already processed location_id: {location_id}")
        return {}
    try:
        async with session.get(url) as response:
            if response.status == 200:
                data = await response.json()
                logger.debug(f"Fetched feed for postcode: {postcode}, Location: {location_id}")
                location_skips.add(location_id)
                return data
            else:
                logger.warning(f"Failed to fetch feed for postcode: {postcode}, Location: {location_id}, Status Code: {response.status}")
                return {}
    except Exception as e:
        logger.error(f"Exception fetching feed for postcode: {postcode}, Location: {location_id}, Error: {e}")
        return {}

async def process_postcode(
    session: aiohttp.ClientSession, 
    postcode: str, 
    semaphore: asyncio.Semaphore
) -> Tuple[Dict[str, URLItem], Set[str], Dict[str, Set[str]]]:
    """
    Processes a single postcode by fetching its locations and their corresponding feeds.
    """
    urls_top: Dict[str, URLItem] = {}
    domains: Set[str] = set()
    url_items: Dict[str, Set[str]] = {}

    async with semaphore:
        data = await get_inyourarea_locations(session, postcode)
    
    if not data:
        logger.info(f"No data found for postcode: {postcode}")
        return urls_top, domains, url_items

    locations = data.get("parms", {}).get("props", {}).get("locations", [])
    tasks = []
    for location in locations:
        location_id = location.get("name", "").lower()
        if not location_id:
            continue
        tasks.append(get_inyourarea_feed(session, postcode, location_id))
    
    # Gather feeds concurrently
    feeds = await asyncio.gather(*tasks, return_exceptions=True)

    for location, feed in zip(locations, feeds):
        if isinstance(feed, Exception) or not feed:
            continue
        feed_items = feed.get("parms", {}).get("asyncProps", {}).get("feed", [])
        for item in feed_items:
            if item.get("type") != "articles":
                continue
            article = item.get("item", {})
            url = article.get("url")
            if not url:
                continue
            if url not in urls_top:
                urls_top[url] = URLItem(url=url)
            parsed_url = urlparse(url)
            domain = parsed_url.netloc
            domains.add(domain)
            if domain in url_items:
                url_items[domain].add(location.get("name", "").lower())
            else:
                url_items[domain] = {location.get("name", "").lower()}
    
    logger.debug(f"Processed postcode: {postcode}")
    return urls_top, domains, url_items

async def get_inyourarea() -> Tuple[Dict[str, URLItem], Set[str], Dict[str, Set[str]]]:
    """
    Main function to read postcodes from a CSV file and process them asynchronously.
    """
    # Read postcodes from CSV
    postcodes = []
    try:
        with open(INPUT_CSV_PATH, mode="r", newline='', encoding='utf-8') as csvfile:
            reader = csv.reader(csvfile)
            header = next(reader, None)  # Skip header if present
            for row in reader:
                if row:
                    postcode = row[0].strip().replace(" ", "")
                    if postcode:
                        postcodes.append(postcode)
    except Exception as e:
        logger.error(f"Failed to read CSV file: {e}")
        return {}, set(), {}

    if not postcodes:
        logger.warning("No postcodes found in the CSV file.")
        return {}, set(), {}

    semaphore = asyncio.Semaphore(CONCURRENT_REQUESTS)
    connector = aiohttp.TCPConnector(limit_per_host=CONCURRENT_REQUESTS)
    all_urls_top: Dict[str, URLItem] = {}
    all_domains: Set[str] = set()
    all_url_items: Dict[str, Set[str]] = {}

    async with aiohttp.ClientSession(connector=connector) as session:
        tasks = [
            asyncio.create_task(process_postcode(session, postcode, semaphore)) 
            for postcode in postcodes
        ]
        
        # Initialize tqdm progress bar
        for coro in tqdm(asyncio.as_completed(tasks), total=len(tasks), desc="Processing postcodes"):
            try:
                urls_top, domains, url_items = await coro
                all_urls_top.update(urls_top)
                all_domains.update(domains)
                for domain, locations in url_items.items():
                    if domain in all_url_items:
                        all_url_items[domain].update(locations)
                    else:
                        all_url_items[domain] = locations
            except Exception as e:
                logger.error(f"Error processing a postcode: {e}")
    
    # Example: Print summary
    logger.info(f"Total unique URLs: {len(all_urls_top)}")
    logger.info(f"Total unique domains: {len(all_domains)}")
    # You can further process or save `all_urls_top`, `all_domains`, and `all_url_items` as needed

    return all_urls_top, all_domains, all_url_items

def main():
    """
    Entry point of the script.
    """
    try:
        all_urls_top, all_domains, all_url_items = asyncio.run(get_inyourarea())
        # Further processing can be done here
    except Exception as e:
        logger.critical(f"An unexpected error occurred: {e}")

if __name__ == "__main__":
    main()
