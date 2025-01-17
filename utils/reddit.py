import httpx
from bs4 import BeautifulSoup
import asyncio
import random
from urllib.parse import urlparse, urljoin


async def reddit_domain_scrape(domain):
    """
    Scrapes the 'old Reddit' domain listings for links to the given domain.
    Yields (full_url, title_text) tuples.
    """
    base_url = f"https://old.reddit.com/domain/{domain}/"
    headers = {"User-Agent": "Mozilla/5.0"}
    seen_urls = set()

    async with httpx.AsyncClient(headers=headers, timeout=60) as client:
        while base_url:
            try:
                response = await client.get(base_url)

                # Handle common status codes
                if response.status_code == 200:
                    text = response.text
                elif response.status_code == 429:
                    # Rate-limited; see if Reddit returned "Retry-After"
                    retry_after = response.headers.get("Retry-After")
                    wait_time = int(retry_after) if retry_after and retry_after.isdigit() else 60
                    await asyncio.sleep(wait_time)
                    continue  # Retry after sleeping
                else:
                    break  # Stop on other error codes
            except httpx.RequestError:
                # Network or request error; stop scraping
                break

            soup = BeautifulSoup(text, "html.parser")

            # In old Reddit, posts are typically div.thing elements
            posts = soup.find_all("div", class_="thing")

            for post in posts:
                # Post title link
                link_tag = post.find("a", class_="title")
                if not link_tag:
                    continue
                href = link_tag["href"]

                # Convert relative to absolute URL
                full_url = urljoin("https://old.reddit.com", href)
                parsed_url = urlparse(full_url)

                # Check if the link is for the specified domain
                # (or you can compare parsed_url.netloc == domain if you want an exact match)
                if domain in parsed_url.netloc:
                    if full_url not in seen_urls:
                        seen_urls.add(full_url)
                        title_text = link_tag.get_text(strip=True)
                        # Yield each post link + title
                        yield full_url, title_text

            # Pagination: find the <span class="next-button"> if it exists
            next_button = soup.find("span", class_="next-button")
            if next_button and next_button.a:
                base_url = next_button.a["href"]  # e.g. old.reddit.com/?count=25&after=...
            else:
                break  # No further pages

            # Random delay to help avoid rate limiting
            await asyncio.sleep(random.uniform(2, 5))
