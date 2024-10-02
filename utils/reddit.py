from urllib.parse import urlparse
import requests
from bs4 import BeautifulSoup
import time  # For sleep
import random  # For randomizing sleep duration

def reddit_domain_scrape(domain):
    base_url = f"https://www.reddit.com/domain/{domain}"
    seen_urls = set()  # Set to keep track of seen URLs for quick lookup
    
    while base_url:
        if base_url in seen_urls:
            # Skip if this URL has already been processed
            print(f"Skipping already seen URL: {base_url}")
            break  # Avoid infinite loop
        
        # Add the current base URL to seen URLs
        seen_urls.add(base_url)
        
        # Retry mechanism with support for Retry-After header
        retries = 5  # Number of retries

        while retries > 0:
            try:
                res = requests.get(base_url, headers={"User-Agent": "Mozilla/5.0"})  # Add User-Agent to mimic a browser
                print(f"Status code: {res.status_code}")
                
                if res.status_code == 200:
                    break  # If successful, break out of retry loop
                
                elif res.status_code == 429:
                    # Handle rate limiting using Retry-After header
                    retry_after = res.headers.get("Retry-After")
                    if retry_after:
                        # Retry-After header could be in seconds or as a date
                        try:
                            wait_time = int(retry_after)
                        except ValueError:
                            # If it's not an integer, it might be a date, and we can handle it differently if needed
                            wait_time = 60  # Default wait time
                    else:
                        wait_time = 60  # Default wait time if header is not present

                    print(f"Rate limit hit. Waiting for {wait_time} seconds as per Retry-After header...")
                    time.sleep(wait_time)
                    retries -= 1  # Reduce the number of retries
                
                else:
                    print(f"Failed to retrieve {base_url} with status code {res.status_code}")
                    return  # Exit if any other status code is received

            except requests.RequestException as e:
                print(f"Failed to retrieve {base_url}: {e}")
                return  # Exit on a request exception
        
        if retries == 0:
            print("Max retries reached. Exiting.")
            break  # Exit if max retries have been reached
        
        text = res.text
        bs = BeautifulSoup(text, "html.parser")
        
        # Extract and print links containing the domain
        for link in bs.find_all("a", href=True):
            href = link.get("href")
            o = urlparse(href)
            if (href.startswith("http") or href.startswith("https")) and domain in o.hostname:
                text = link.text.strip()
                if "\n\n\n\n" == text:
                    continue
                yield href, link.text.strip()
        
        # Find the "next page" link and update the base_url if it exists
        next_page_links = bs.find_all("a", href=True)
        base_url = None  # Reset base_url to None initially
        for next_page_link in next_page_links:
            if f"www.reddit.com/domain/{domain}/?count=" in next_page_link['href'] and next_page_link['href'] not in seen_urls:
                base_url = next_page_link['href']
                break
        
        # Random delay to reduce chances of getting rate-limited
        time.sleep(random.uniform(2, 5))


