import asyncio
from utils.duckduckgo import DDGS, RatelimitException
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
                    # Initialize the aiohttp session
                    news_results = ddgs.news(f"\"{key}\" site:bbc.co.uk", max_results=70,region="en-GB")
                    for result in news_results:
                        pass
                        # Convert date to readable format if needed
                except RatelimitException as e:
                    print(f"Rate limit hit: {e}")
                    await asyncio.sleep(20)
                except e:
                    print(f"Search exception: {e}")
                    await asyncio.sleep(20)
                # Sleep between keyword searches to avoid rate limiting
                
                pbar.update(1)
    except ConnectionErrorException as e:
        print(f"Connection error: {e}")
    finally:
        await ddgs.close()  # Ensure that the session is closed

# Run the async main function
if __name__ == "__main__":
    asyncio.run(main())