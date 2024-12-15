import httpx
from bs4 import BeautifulSoup
import asyncio
import random
from urllib.parse import urlparse, urljoin


async def reddit_domain_scrape(domain):
    base_url = f"https://www.reddit.com/domain/{domain}/"
    headers = {"User-Agent": "Mozilla/5.0"}
    seen_urls = set()

    async with httpx.AsyncClient(headers=headers, timeout=60) as client:
        while base_url:
            try:
                response = await client.get(base_url)

                if response.status_code == 200:
                    text = response.text
                elif response.status_code == 429:
                    # Handle rate limiting
                    retry_after = response.headers.get("Retry-After")
                    wait_time = int(retry_after) if retry_after and retry_after.isdigit() else 60
                    print(f"Rate limited. Waiting for {wait_time} seconds.")
                    await asyncio.sleep(wait_time)
                    continue  # Retry the same URL after waiting
                else:
                    print(f"Failed to retrieve {base_url} with status code {response.status_code}")
                    break  # Stop if other HTTP errors occur
            except httpx.RequestError as e:
                print(f"Request failed for {base_url}: {e}")
                break  # Stop on network errors

            soup = BeautifulSoup(text, "html.parser")
            posts = soup.find_all("div", {"data-testid": "post-container"})

            for post in posts:
                link_tag = post.find("a", href=True)
                if not link_tag:
                    continue
                href = link_tag['href']
                full_url = urljoin("https://www.reddit.com", href)
                parsed_url = urlparse(full_url)

                # Check if the URL is for the specified domain
                if domain in parsed_url.netloc:
                    if full_url in seen_urls:
                        continue
                    seen_urls.add(full_url)
                    title = post.find("h3")
                    title_text = title.get_text(strip=True) if title else "No Title"
                    yield full_url, title_text

            # Find the next page URL
            next_button = soup.find("span", class_="next-button")
            if next_button and next_button.a:
                base_url = next_button.a['href']
            else:
                break  # No further pages available

            # Random delay to reduce chances of being rate-limited
            await asyncio.sleep(random.uniform(2, 5))
