import asyncio
import os
import logging

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
    start_urls = ["https://www.bbc.co.uk"]
    feeds = [
        "https://feeds.bbci.co.uk/news/rss.xml",
        "https://feeds.bbci.co.uk/news/uk/rss.xml",
        "https://feeds.bbci.co.uk/news/business/rss.xml",
        "https://feeds.bbci.co.uk/news/politics/rss.xml",
        "https://feeds.bbci.co.uk/news/health/rss.xml",
        "https://feeds.bbci.co.uk/news/education/rss.xml",
        "https://feeds.bbci.co.uk/news/science_and_environment/rss.xml",
        "https://feeds.bbci.co.uk/news/technology/rss.xml",
        "https://feeds.bbci.co.uk/news/entertainment_and_arts/rss.xml",
        "https://feeds.bbci.co.uk/news/england/rss.xml",
    ]
    
    # Optional parameters (set to None or desired values)
    start_date = None  # e.g., "2023-01-01"
    end_date = None    # e.g., "2023-12-31"
    
    # Define subdirectories to allow and exclude
    allowed_subdirs_cruel = ["https://bbc.co.uk/news/", "https://feeds.bbci.co.uk/news"]
    exclude_subdirs_cruel = [
        "www.bbc.co.uk/news/world-",
        "www.bbc.co.uk/news/election-",
    ]
    exclude_scrape_subdirs = [
        "www.bbc.co.uk/news/resources/",
        "www.bbc.co.uk/news/topics/",
        "www.bbc.co.uk/news/world-",
        "www.bbc.co.uk/news/election-",
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
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Crawling interrupted by user.")
    except Exception as e:
        logger.error(f"An unexpected error occurred: {e}")
