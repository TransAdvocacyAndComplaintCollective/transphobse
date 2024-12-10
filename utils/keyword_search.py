import asyncio
import csv
import random
import ssl
import logging
from typing import List, Dict, Optional
from urllib.parse import urlencode, urljoin

import aiohttp
import aiofiles
from bs4 import BeautifulSoup

# import utils.keywords as keywords  # Ensure this module exists and contains KEYWORDS


# User-Agent list for headers
USER_AGENTS = [
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Ubuntu Chromium/37.0.2062.94 Safari/537.36",
    "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/45.0.2454.85 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_5) AppleWebKit/600.8.9 (KHTML, like Gecko) Version/8.0.8 Safari/600.8.9",
    "Mozilla/5.0 (iPad; CPU OS 8_4_1 like Mac OS X) AppleWebKit/600.1.4 (KHTML, like Gecko) Version/8.0 Mobile/12H321 Safari/600.1.4",
]

async def get_searx_domains() -> List[str]:
    """
    Fetches a list of Searx instance domains with 'normal' network type.
    """
    domains = []
    url = "https://searx.space/data/instances.json"
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, ssl=False) as response:
                if response.status == 200:
                    data = await response.json()
                    for instance, details in data.get("instances", {}).items():
                        if details.get("network_type") == "normal":
                            domains.append(f"{instance}search")
                    logging.debug(f"Fetched {len(domains)} Searx domains.")
                else:
                    logging.error(f"Failed to fetch Searx domains, Status Code: {response.status}")
    except Exception as e:
        logging.error(f"Exception while fetching Searx domains: {e}")
    return domains

async def fetch_page(
    url: str,
    session: aiohttp.ClientSession,
    headers: Dict[str, str],
    search_params: Optional[Dict[str, str]] = None,
    mode: str = "get",
    timeout: int = 30,
    json_mode: bool = False
) -> Dict[str, Optional[str]]:
    """
    Fetches a web page and returns its content, status code, and final URL.
    """
    if search_params:
        logging.debug(f"Search Params: {search_params}")

    try:
        request_kwargs = {
            "headers": headers,
            "ssl": False,
            "timeout": aiohttp.ClientTimeout(total=timeout)
        }

        if mode == "get":
            async with session.get(url, **request_kwargs) as response:
                logging.info(f"GET {response.status} - {response.url}")
                return await process_response(response, json_mode)
        elif mode == "post":
            async with session.post(url, data=search_params, **request_kwargs) as response:
                logging.info(f"POST {response.status} - {response.url}")
                return await process_response(response, json_mode)
        else:
            logging.error(f"Unsupported HTTP method: {mode}")
    except aiohttp.ClientError as e:
        logging.error(f"Client error while fetching {url}: {e}")
    except asyncio.TimeoutError:
        logging.error(f"Timeout while fetching {url}")
    except ssl.SSLError as e:
        logging.error(f"SSL error while fetching {url}: {e}")
    except Exception as e:
        logging.error(f"Unexpected error while fetching {url}: {e}")

    return {"content": None, "status": "error", "url": url}

async def process_response(response: aiohttp.ClientResponse, json_mode: bool) -> Dict[str, Optional[str]]:
    """
    Processes the HTTP response and extracts content based on the mode.
    """
    if response.status in {200, 202}:
        if json_mode:
            try:
                json_content = await response.json()
                return {"content": json_content, "status": response.status, "url": str(response.url)}
            except Exception as e:
                logging.error(f"Error parsing JSON: {e}")
        try:
            text_content = await response.text()
            return {"content": text_content, "status": response.status, "url": str(response.url)}
        except Exception as e:
            logging.error(f"Error reading text content: {e}")
    else:
        logging.warning(f"Unexpected status code: {response.status}")
    return {"content": None, "status": response.status, "url": str(response.url)}

async def search_engine(
    keyword: str,
    site: Optional[str],
    base_urls: List[str],
    engine_name: str = "generic",
    search_params: Optional[Dict[str, str]] = None,
    space: str = " ",
    page_param: Optional[str] = None,
    mode: str = "get",
    query_param: str = "q",
    min_wait: int = 10,
    rate_limit: int = 10,
    index_param_mode: str = "page",
    items_per_page: int = 10,
    starting_index: Optional[int] = None,
    json_mode: bool = False,
    retries: int = 3
) -> List[Dict[str, str]]:
    """
    Generic search engine function to perform searches and extract links.
    """
    links = []
    current_index = starting_index if starting_index is not None else (1 if index_param_mode == "page" else 0)
    formatted_keyword = keyword
    search_attempts = 0


    async with aiohttp.ClientSession() as session:
        while search_attempts < retries:

            # Prepare search parameters
            params = search_params.copy() if search_params else {}
            query = f'"{formatted_keyword}" site:{site}' if site else f'"{formatted_keyword}"'
            params[query_param] = query
            if page_param:
                params[page_param] = str(current_index)

            headers = {
                "User-Agent": random.choice(USER_AGENTS),
                "Content-Type": "application/x-www-form-urlencoded",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/png,image/svg+xml,*/*;q=0.8",
                "Accept-Language": "en-GB,en;q=0.5",
                "Upgrade-Insecure-Requests": "1",
                "Sec-Fetch-Dest": "document",
                "Sec-Fetch-Mode": "navigate",
                "Sec-Fetch-Site": "same-origin"
            }

            base_url = random.choice(base_urls)
            url = f"{base_url}?{urlencode(params)}" if mode == "get" else base_url

            # Rate limiting
            await asyncio.sleep(rate_limit)

            result = await fetch_page(url, session, headers, params, mode, json_mode=json_mode)
            content, status, final_url = result["content"], result["status"], result["url"]

            if status in {429, 403}:  # Rate limited or forbidden
                await exponential_backoff(search_attempts, min_wait)
                search_attempts += 1
                continue
            elif status in {200, 202} and content:
                try:
                    if json_mode:
                        new_links = extract_links_json(content, keyword, engine_name)
                    else:
                        soup = BeautifulSoup(content, "html.parser")
                        new_links = extract_links(soup, site, base_url, engine_name, keyword)
                    links.extend(new_links)

                    if len(new_links) < items_per_page:
                        break

                    current_index += 1
                    search_attempts = 0  # Reset retries after a successful fetch
                except Exception as e:
                    search_attempts += 1
                    continue
            else:
                logging.error(f"Unexpected response status: {status} | URL: {final_url}")
                search_attempts += 1

        if search_attempts >= retries:
            logging.info("Maximum retries reached, ending search.")

    return links

def extract_links_json(data: Dict, keyword: str, engine_name: str) -> List[Dict[str, str]]:
    """
    Extracts links from JSON responses.
    """
    links = []
    for result in data.get("results", []):
        link = result.get("url", "")
        title = result.get("title", link)
        snippet = result.get("content", "")
        links.append({
            "link": link,
            "link_text": title,
            "snippet": snippet,
            "keyword": keyword,
            "engine": engine_name,
        })
    return links

def extract_links(
    soup: BeautifulSoup,
    site: Optional[str],
    base_url: str,
    engine_name: str,
    keyword: str
) -> List[Dict[str, str]]:
    """
    Extracts and returns cleaned links from a BeautifulSoup object based on the engine.
    """
    links = []
    unwanted_texts = [
        "privacy policy", "terms of service", "public instances", "source code",
        "issue tracker", "contact instance maintainer", "donate", "about"
    ]
    unwanted_domains = [
        "github.com", "searx.space", "iubenda.com", "indst.eu", "canine.tools",
        "fairkom.eu", "vojk.au", "buechter.org"
    ]

    engine_lower = engine_name.lower()

    if engine_lower == "searx":
        for link in soup.find_all('a', href=True):
            href = link['href']
            if any(domain in href for domain in unwanted_domains):
                continue
            if site is None or site in href:
                if href.startswith(("http", "https", "www")):
                    link_text = link.get_text(strip=True) or href
                    if any(unwanted in link_text.lower() for unwanted in unwanted_texts):
                        continue
                    links.append({
                        "link": href,
                        "link_text": link_text,
                        "keyword": keyword,
                        "engine": engine_name,
                    })
    elif engine_lower == "bbc":
        results_section = soup.find_all("div", class_="ssrcss-1mhwnz8-Promo")
        if not results_section:
            return links

        for result in results_section:
            try:
                title_tag = result.find("a", class_="ssrcss-its5xf-PromoLink")
                title = title_tag.text.strip() if title_tag else "No title"
                url = title_tag["href"] if title_tag and "href" in title_tag.attrs else "No URL"
                date_tag = result.find("span", class_="ssrcss-1if1g9v-MetadataText")
                date_published = date_tag.text.strip() if date_tag else "No date"
                section_tag = result.find("span", class_="ssrcss-ar8sc2-IconContainer")
                section = section_tag.text.strip() if section_tag else "No section"
                summary_tag = result.find("p", class_="ssrcss-1q0x1qg-Paragraph")
                summary = summary_tag.text.strip() if summary_tag else "No summary"

                if url.startswith("/"):
                    url = urljoin("https://www.bbc.co.uk", url)

                links.append({
                    "link": url,
                    "link_text": title,
                    "date_published": date_published,
                    "section": section,
                    "summary": summary,
                    "keyword": keyword,
                    "engine": engine_name,
                })
            except Exception as e:
                pass

    elif engine_lower == "duckduckgo":
        logging.debug("Processing DuckDuckGo results.")
        for result in soup.find_all('div', class_='links_main'):
            for link in result.find_all('a', href=True):
                href = link['href']
                if site is None or site in href:
                    link_text = link.get_text(strip=True) or href
                    links.append({
                        "link": href,
                        "link_text": link_text,
                        "keyword": keyword,
                        "engine": engine_name,
                    })

    else:
        for link in soup.find_all("a", href=True):
            href = urljoin(base_url, link["href"])
            link_text = link.get_text(strip=True) or href
            if site is None or site in href:
                links.append({
                    "link": href,
                    "link_text": link_text,
                    "keyword": keyword,
                    "engine": engine_name,
                })
    return links

async def wait_between_requests(min_wait: int = 5, max_wait: int = 15):
    """
    Waits for a random duration between min_wait and max_wait seconds.
    """
    delay = random.uniform(min_wait, max_wait)
    logging.debug(f"Waiting for {delay:.2f} seconds between requests.")
    await asyncio.sleep(delay)

async def exponential_backoff(error_count: int, min_wait: int = 5):
    """
    Implements exponential backoff based on the number of errors encountered.
    """
    delay = min(min_wait * (2 ** error_count), 300)  # Max 5 minutes
    await asyncio.sleep(delay)

async def duckduckgo_search(keyword: str, site: Optional[str] = None) -> List[Dict[str, str]]:
    """
    Performs a DuckDuckGo search.
    """
    base_url = "https://html.duckduckgo.com/html"
    search_query = f'"{keyword}" site:{site}' if site else f'"{keyword}"'
    search_params = {"q": search_query}
    return await search_engine(
        keyword=keyword,
        site=site,
        base_urls=[base_url],
        engine_name="DuckDuckGo",
        search_params=search_params,
        mode="get",
    )

async def duckduckgo_news_search(keyword: str, site: Optional[str] = None) -> List[Dict[str, str]]:
    """
    Performs a DuckDuckGo news search.
    """
    base_url = "https://html.duckduckgo.com/html"
    search_query = f'"{keyword}" site:{site}' if site else f'"{keyword}"'
    search_params = {
        "q": search_query,
        "iar": "news",
        "ia": "news"
    }
    return await search_engine(
        keyword=keyword,
        site=site,
        base_urls=[base_url],
        engine_name="DuckDuckGo",
        search_params=search_params,
        mode="get",
    )

async def bbc_search(keyword: str) -> List[Dict[str, str]]:
    """
    Performs a BBC search.
    """
    base_url = "https://www.bbc.co.uk/search"
    search_params = {"q": keyword, "d": "NEWS_PS"}
    return await search_engine(
        keyword=keyword,
        site=None,
        base_urls=[base_url],
        engine_name="BBC",
        search_params=search_params,
        mode="get",
        query_param="q",
        page_param="page",
        min_wait=30,
    )

async def searx_search(keyword: str, site: Optional[str] = None) -> List[Dict[str, str]]:
    """
    Performs a Searx search.
    """
    search_params = {
        "category_general": "1",
        "language": "en-GB",
        "time_range": "",
        "safesearch": "0",
        "theme": "simple",
    }
    domain_list = await get_searx_domains()
    return await search_engine(
        keyword=keyword,
        site=site,
        base_urls=domain_list,
        engine_name="Searx",
        search_params=search_params,
        mode="post",
        min_wait=5,
        page_param="pageno",
        # Removed 'do=40' as it's not supported
        retries=3
    )

async def searx_search_news(keyword: str, site: Optional[str] = None) -> List[Dict[str, str]]:
    """
    Performs a Searx news search.
    """
    search_params = {
        "language": "en-GB",
        "time_range": "",
        "safesearch": "0",
        "theme": "simple",
        "category_news": "1"
    }
    domain_list = await get_searx_domains()
    return await search_engine(
        keyword=keyword,
        site=site,
        base_urls=domain_list,
        engine_name="Searx",
        search_params=search_params,
        mode="post",
        min_wait=100,
        page_param="pageno",
        # Removed 'do=40' as it's not supported
        retries=3
    )

async def lookup_keyword(keyword: str, domain: Optional[str] = None) -> List[Dict[str, str]]:
    """
    Looks up a keyword across multiple search engines and aggregates the results.
    """
    duckduckgo_results = await duckduckgo_search(keyword, domain)
    searx_results = await searx_search(keyword, domain)
    all_results = duckduckgo_results + searx_results
    return all_results

async def main():
    """
    Main function to perform searches and save results to a CSV file.
    """
    filtered_keywords = [kw for kw in keywords.KEYWORDS if len(kw) >= 6]
    output_file = "data/search_results.csv"

    try:
        async with aiofiles.open(output_file, mode="a", newline="", encoding="utf-8") as file:
            writer = csv.DictWriter(file, fieldnames=["link", "link_text", "keyword", "engine", "snippet", "date_published", "section", "summary"])
            
            # Write header if the file is empty
            await file.seek(0, 2)  # Move to the end of the file
            if await file.tell() == 0:
                await file.write(','.join(writer.fieldnames) + '\n')

            for keyword in filtered_keywords:
                logging.info(f"Processing keyword: {keyword}")

                # Perform searches
                searx_news_results = await searx_search_news(keyword)
                duckduckgo_results = await duckduckgo_search(keyword)
                bbc_results = await bbc_search(keyword)

                # Combine all results
                all_results = searx_news_results + duckduckgo_results + bbc_results

                # Write results to CSV
                for result in all_results:
                    await file.write(','.join([
                        f"\"{result.get('link', '').replace('\"', '\"\"')}\"",
                        f"\"{result.get('link_text', '').replace('\"', '\"\"')}\"",
                        f"\"{result.get('keyword', '').replace('\"', '\"\"')}\"",
                        f"\"{result.get('engine', '').replace('\"', '\"\"')}\"",
                        f"\"{result.get('snippet', '').replace('\"', '\"\"')}\"",
                        f"\"{result.get('date_published', '').replace('\"', '\"\"')}\"",
                        f"\"{result.get('section', '').replace('\"', '\"\"')}\"",
                        f"\"{result.get('summary', '').replace('\"', '\"\"')}\""
                    ]) + '\n')

                logging.info(f"Completed processing for keyword: {keyword}")

        logging.info(f"CSV file saved as '{output_file}'")
    except Exception as e:
        logging.error(f"Error during CSV writing: {e}")

if __name__ == "__main__":
    asyncio.run(main())
