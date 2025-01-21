import asyncio

from meage_scrape import Crawler


async def main():
    crawler = Crawler(
        start_urls=["https://www.bbc.co.uk/news/", "https://www.bbc.com/news/",
                    "https://www.bbc.co.uk/newsround/", "https://www.bbc.com/newsround/",
                    "https://www.bbc.co.uk/future/", "https://www.bbc.com/future/"
                    "https://www.bbc.co.uk/sport/", "https://www.bbc.com/sport/"
                ],
        feeds=[
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
        ],
        name="BBC_crawler",
        allowed_url=[
            "bbc.co.uk/sport",
            "bbc.com/sport",
            "bbc.co.uk/news",
            "bbc.com/news",
            "bbc.co.uk/future",
            "bbc.com/future",
            "bbc.co.uk/newsround",
            "bbc.com/newsround",
            "feeds.bbci.co.uk",
        ],  # Optional domain restrictions
        find_new_site=False,
    )
    await crawler.crawl_start()


asyncio.run(main())
