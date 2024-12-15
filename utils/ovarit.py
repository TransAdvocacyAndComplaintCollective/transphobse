import httpx
from bs4 import BeautifulSoup
import asyncio
import random
from urllib.parse import urljoin, urlparse

async def ovarit_domain_scrape(domain):
    base_url = f"https://www.ovarit.com/domain/{domain}"
    seen_pages = set()  # To keep track of processed pages
    seen_urls = set()   # To keep track of extracted URLs
    headers = {"User-Agent": "Mozilla/5.0"}

    async with httpx.AsyncClient(headers=headers, timeout=httpx.Timeout(60)) as client:
        while base_url:
            if base_url in seen_pages:
                print(f"Skipping already seen page: {base_url}")
                break  # Avoid infinite loops

            seen_pages.add(base_url)

            try:
                response = await client.get(base_url)
                if response.status_code == 200:
                    text = response.text
                else:
                    print(f"Failed to retrieve {base_url} with status code {response.status_code}")
                    break  # Stop processing if there's an HTTP error
            except httpx.RequestError as e:
                print(f"Failed to retrieve {base_url}: {e}")
                break  # Stop processing on network errors

            soup = BeautifulSoup(text, "html.parser")

            # Extract posts or links containing the domain
            posts = soup.find_all("div", class_="post")  # Adjust the selector based on the actual site structure

            for post in posts:
                link = post.find("a", href=True)
                if not link:
                    continue
                href = link['href']
                full_url = urljoin("https://www.ovarit.com", href)
                parsed_url = urlparse(full_url)

                # Check if the URL contains the domain and hasn't been seen before
                if domain in parsed_url.netloc and full_url not in seen_urls:
                    seen_urls.add(full_url)
                    title = link.get_text(strip=True)
                    yield full_url, title

            # Find the "next page" link and update the base_url if it exists
            next_page_link = soup.find("a", text="Next")
            if next_page_link and next_page_link.get("href"):
                base_url = urljoin("https://www.ovarit.com", next_page_link["href"])
            else:
                break  # Exit the loop if no next page is found

            # Random delay to avoid overwhelming the server
            await asyncio.sleep(random.uniform(2, 5))
