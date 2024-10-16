import asyncio
from utils.duckduckgo import DDGS, ConnectionErrorException, DuckDuckGoSearchException, RatelimitException
import utils.keywords as kw
from tqdm.asyncio import tqdm_asyncio
async def main():
    try:
        # Replace KEYWORDS with a sample list
        total_keywords = len(kw.KEYWORDS)
        with tqdm_asyncio(total=total_keywords, desc="Processing Keywords") as pbar:
            for key in kw.KEYWORDS:
                try:
                    ddgs = DDGS()
                    await ddgs.init()  # Initialize the aiohttp session
                    news_results = await ddgs.news(key, max_results=70,region="en-GB")
                    for result in news_results:
                        pass
                        # Convert date to readable format if needed
                except RatelimitException as e:
                    print(f"Rate limit hit: {e}")
                except DuckDuckGoSearchException as e:
                    print(f"Search exception: {e}")
                # Sleep between keyword searches to avoid rate limiting
                await asyncio.sleep(2)
                pbar.update(1)
    except ConnectionErrorException as e:
        print(f"Connection error: {e}")
    finally:
        await ddgs.close()  # Ensure that the session is closed

# Run the async main function
if __name__ == "__main__":
    asyncio.run(main())