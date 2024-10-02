import requests
from bs4 import BeautifulSoup

def ovarit_domain_scrape(domain):
    base_url = f"https://www.ovarit.com/domain/{domain}"
    seen_urls = set()  # Set to keep track of seen URLs for quick lookup
    
    while base_url:
        if base_url in seen_urls:
            # Skip if this URL has already been processed
            print(f"Skipping already seen URL: {base_url}")
            break  # Avoid infinite loop
        
        # Add the current base URL to seen URLs
        seen_urls.add(base_url)
        
        try:
            res = requests.get(base_url)
            print(f"Status code: {res.status_code}")
        except requests.RequestException as e:
            print(f"Failed to retrieve {base_url}: {e}")
            break
        
        if res.status_code == 200:
            text = res.text
            bs = BeautifulSoup(text, "html.parser")
            
            # Extract and print links containing the domain
            for link in bs.find_all("a", href=True):
                href = link.get("href")
                if (href.startswith("http") or href.startswith("https")) and domain in href:
                    text= link.text
                    if "\n\n\n\n" == text:
                        continue
                    yield href, link.text
            
            # Find the "next page" link and update the base_url if it exists
            next_page_links = bs.find_all("a", href=True)
            for next_page_link in next_page_links:
                if f"/www.bbc.co.uk?after=" in next_page_link['href']:
                    base_url = f"https://www.ovarit.com{next_page_link['href']}"
                    break
        else:
            print(f"Failed to retrieve {base_url}")
            break
