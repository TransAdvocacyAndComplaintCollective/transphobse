import asyncio
import os
from meage_scrape import Crawler
import utils.keywords as kw

import signal, os


async def main():
    if not os.path.exists("databaces"):
        os.mkdir("databaces")

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
    start_data = None
    end_data = None
    allow_for_recruiting = False
    exclude_lag = ["cy"]
    exclude_subdirs_cruel = [
        "www.bbc.co.uk/news/world-",
        "www.bbc.co.uk/news/election-",
    ]
    exclude_subdirs_scrape = [
        "www.bbc.co.uk/news/resources/",
        "www.bbc.co.uk/news/topics/",
        "www.bbc.co.uk/news/world-",
        "www.bbc.co.uk/news/election-",

    ]
    allowed_subdirs_cruel = ["https://bbc.co.uk/news/","https://feeds.bbci.co.uk/news"]
    crawler = Crawler(
        start_urls,
        feeds,
        "BBC_news_mage_scrape",
        allowed_subdirs=allowed_subdirs_cruel,
        start_date=start_data,
        end_date=end_data,
        exclude_lag=exclude_lag,
        exclude_subdirs=exclude_subdirs_cruel,
        exclude_scrape_subdirs=exclude_subdirs_scrape
    )
    # crawler.crawl_init()

    await crawler.crawl_start() 
    

if __name__ == "__main__":
    asyncio.run(main())
