import asyncio
import os
from meage_scrape import KeyPhraseFocusCrawler
import utils.keywords as kw

async def main():
    if not os.path.exists("databaces"):
        os.mkdir("databaces")

    start_urls = ["https://www.bbc.co.uk", "https://feeds.bbci.co.uk/"]
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
    start_data = None
    end_data = None
    allow_for_recruiting = False
    exclude_lag = ["cy"]
    exclude_subdirs_cruel = [
        "www.bbc.co.uk/news/world/",
        "www.bbc.co.uk/news/world-*",
    ]
    exclude_subdirs_scrape = [
        "www.bbc.co.uk/news/resources/",
        "www.bbc.co.uk/news/topics/",
        "www.bbc.co.uk/news/world/",
        "www.bbc.co.uk/news/world-*",

    ]
    allowed_subdirs_cruel = ["bbc.co.uk/news","feeds.bbci.co.uk/news"]
    keywords = kw.KEYWORDS

    crawler = KeyPhraseFocusCrawler(
        start_urls,
        feeds,
        "BBC_news_mage_scrape",
        allowed_subdirs_cruel=allowed_subdirs_cruel,
        keywords=keywords,
        irrelevant_for_keywords=kw.irrelevant_for_keywords,
        anti_keywords=kw.ANTI_KEYWORDS,
        start_data=start_data,
        end_data=end_data,
        exclude_lag=exclude_lag,
        exclude_subdirs_cruel=exclude_subdirs_cruel,
        exclude_subdirs_scrape=exclude_subdirs_scrape,
        allow_for_recruiting=allow_for_recruiting,
    )

    await crawler.crawl_start() 
    


if __name__ == "__main__":
    asyncio.run(main())
