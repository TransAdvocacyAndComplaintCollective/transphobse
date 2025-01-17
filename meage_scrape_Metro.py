import asyncio
import os
import logging

import uvloop

# Ensure that the Crawler class is correctly imported from its module.
# Adjust the import path based on your project structure.
from meage_scrape import Crawler  # Replace 'meage_scrape' with the actual module name if different

# Configure logging to display informational messages
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

async def main():
    """
    The main asynchronous function to initialize and start the crawler.
    """
    # Define necessary directories
    databases_dir = "databases"
    data_dir = "data"
    
    # Ensure the 'databases' and 'data' directories exist
    os.makedirs(databases_dir, exist_ok=True)
    os.makedirs(data_dir, exist_ok=True)
    
    # Define the start URLs and RSS feed URLs
    start_urls = ["https://metro.co.uk/"]
    feeds = [

    ]
    
    # Optional parameters (set to None or desired values)
    start_date = None  # e.g., "2023-01-01"
    end_date = None    # e.g., "2023-12-31"
    
    # Define subdirectories to allow and exclude
    allowed_subdirs_cruel = ["https://metro.co.uk/"]
    exclude_subdirs_cruel = [
    ]
    exclude_scrape_subdirs = [
    ]
    
    # Initialize the Crawler with optimized parameters
    crawler = Crawler(
        start_urls=start_urls,
        feeds=feeds,
        name="BBC_news_mage_scrape",
        allowed_subdirs=allowed_subdirs_cruel,
        start_date=start_date,
        end_date=end_date,
        exclude_subdirs=exclude_subdirs_cruel,
        exclude_scrape_subdirs=exclude_scrape_subdirs,
        find_new_site=True,    # Enable finding new sites based on keywords
        show_bar=True,         # Enable progress bar for monitoring
    )
    
    # Start the crawling process
    await crawler.crawl_start()

if __name__ == "__main__":
    try:
        uvloop.run(main())
    except KeyboardInterrupt:
        logger.info("Crawling interrupted by user.")
    except Exception as e:
        logger.error(f"An unexpected error occurred: {e}")
