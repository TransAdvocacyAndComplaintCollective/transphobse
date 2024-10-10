import requests
from bs4 import BeautifulSoup
import json
from concurrent.futures import ThreadPoolExecutor, as_completed

# Define headers to mimic a browser request
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:129.0) Gecko/20100101 Firefox/129.0',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/png,image/svg+xml,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.5',
    'Upgrade-Insecure-Requests': '1',
    'Sec-Fetch-Dest': 'document',
    'Sec-Fetch-Mode': 'navigate',
    'Sec-Fetch-Site': 'none',
    'Sec-Fetch-User': '?1',
    'Priority': 'u=0, i'
}

# Step 1: Scrape the Main A-Z Pages with Pagination
def fetch_programme_urls(char):
    base_url = "https://www.bbc.co.uk/programmes/a-z/by/{}/all"
    all_programme_urls = set()
    page_num = 1
    seen_urls = set()
    consecutive_duplicate_pages = 0

    for _ in range(100):
        url = f"{base_url.format(char)}?page={page_num}"
        try:
            response = requests.get(url, headers=HEADERS)
            if response.status_code != 200:
                break
            text = response.text
            soup = BeautifulSoup(text, 'html.parser')

            links = soup.find_all('a', href=True)
            current_page_urls = set()
            found_links = False

            for link in links:
                href = link['href']
                if (href.startswith('/programmes/') or href.startswith('https://www.bbc.co.uk/programmes/')) and "player" not in href:
                    full_url = "https://www.bbc.co.uk" + href if href.startswith('/programmes/') else href
                    if full_url.endswith('/all'):
                        continue  # Skip any invalid or incorrect URLs like '/all'
                    current_page_urls.add(full_url)
                    if full_url not in all_programme_urls:
                        all_programme_urls.add(full_url)
                        found_links = True

            if current_page_urls == seen_urls:
                consecutive_duplicate_pages += 1
                if consecutive_duplicate_pages >= 3:
                    break
            else:
                consecutive_duplicate_pages = 0

            if not found_links:
                break

            seen_urls = current_page_urls
            page_num += 1
        except Exception as e:
            print(f"Error fetching {url}: {e}")
            break

    return list(all_programme_urls)

def get_programme_urls():
    all_programme_urls = set()
    characters = list("abcdefghijklmnopqrstuvwxyz@")

    # Use ThreadPoolExecutor for concurrent fetching
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = {executor.submit(fetch_programme_urls, char): char for char in characters}

        # Adding a progress bar for URL fetching
        for future in as_completed(futures):
            result = future.result()
            all_programme_urls.update(result)

    return list(all_programme_urls)

# Step 2: Fetch Program Details in JSON Format
def get_programme_details(programme_id, max_retries=3):
    url = f"https://www.bbc.co.uk/programmes/{programme_id}.json"
    retries = 0

    while retries < max_retries:
        try:
            response = requests.get(url, headers=HEADERS)
            if response.status_code == 404:
                print(f"404 Error: {url} not found.")
                return None  # Skip 404 errors
            elif response.status_code >= 500:
                print(f"Server error {response.status_code} for {url}. Retrying...")
                retries += 1
                continue

            response.raise_for_status()  # Raise an exception for other HTTP errors
            return response.json()

        except requests.RequestException as e:
            print(f"Error fetching details for {programme_id}: {e}")
            break
        except Exception as e:
            print(f"Unexpected error fetching details for {programme_id}: {e}")
            break

    print(f"Failed to fetch details for {programme_id} after {max_retries} retries.")
    return None

# Step 3: Scrape Episodes Guide Pages
def get_episodes(programme_id):
    base_url = f"https://www.bbc.co.uk/programmes/{programme_id}/episodes/guide"
    episodes = []
    page_num = 1
    stop = False

    for _ in range(90):
        try:
            response = requests.get(f"{base_url}?page={page_num}", headers=HEADERS)
            if response.status_code != 200:
                break
            text = response.text
            soup = BeautifulSoup(text, 'html.parser')
            episode_links = soup.find_all('a', href=True)

            if not episode_links:
                break

            for link in episode_links:
                href = link['href']
                if href.startswith('/programmes/') and href.count('/') == 2:
                    if href in episodes:
                        stop = True
                        break
                    episodes.append("https://www.bbc.co.uk" + href)
            if stop:
                break
            page_num += 1
        except Exception as e:
            print(f"Error fetching episodes for {programme_id} on page {page_num}: {e}")
            break

    return episodes

# Step 4: Combine and Save Data
def scrape_bbc_programmes():
    all_programme_urls = get_programme_urls()
    programme_data = []

    # Use ThreadPoolExecutor for concurrent fetching of programme details
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = {executor.submit(get_programme_details, programme_url.split('/')[-1]): programme_url for programme_url in all_programme_urls}

        # Adding a progress bar for fetching programme details
        for future in as_completed(futures):
            details = future.result()
            if details:
                episodes = get_episodes(details['programme']['pid'])
                details['episodes'] = episodes
                programme_data.append(details)

    # Save data to a JSON file
    with open('bbc_programmes.json', 'w') as f:
        json.dump(programme_data, f, indent=4)

    print("Scraping completed and data saved to bbc_programmes.json")

# Execute the scraping function
scrape_bbc_programmes()
