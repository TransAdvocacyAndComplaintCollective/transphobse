import asyncio
import random
import logging
from typing import List, Dict, Optional
from urllib.parse import urlencode, urljoin
from bs4 import BeautifulSoup
import httpx

# import utils.keywords as keywords  # Ensure this module exists and contains KEYWORDS

# Configure logging
logging.basicConfig(level=logging.INFO)

# User-Agent list for headers
USER_AGENTS = [
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Ubuntu Chromium/37.0.2062.94 Safari/537.36",
    "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/45.0.2454.85 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_5) AppleWebKit/600.8.9 (KHTML, like Gecko) Version/8.0.8 Safari/600.8.9",
    "Mozilla/5.0 (iPad; CPU OS 8_4_1 like Mac OS X) AppleWebKit/600.1.4 (KHTML, like Gecko) Version/8.0 Mobile/12H321 Safari/600.1.4",
]


async def get_searx_domains() -> List[str]:
    """
    Fetches a list of Searx instance domains with 'normal' network type,
    including both '/searxng/search' and '/search' paths.
    """
    domains = []
    url = "https://searx.space/data/instances.json"
    try:
        async with httpx.AsyncClient() as client:
            response = await client.get(url, timeout=30)
            if response.status_code == 200:
                data = response.json()
                for instance, details in data.get("instances", {}).items():
                    if details.get("network_type") == "normal":
                        # Ensure proper URL formatting
                        base_instance = (
                            instance if instance.endswith("/") else f"{instance}/"
                        )
                        # Append both search paths as separate entries
                        domains.append(urljoin(base_instance, "search"))
                        domains.append(urljoin(base_instance, "searxng/search"))
                random.shuffle(domains)  # Shuffle in place
                logging.debug(
                    f"Fetched {len(domains)} Searx domains with multiple search paths."
                )
            else:
                logging.error(
                    f"Failed to fetch Searx domains, Status Code: {response.status_code}"
                )
    except httpx.ConnectTimeout:
        logging.error("Connection timed out while fetching Searx domains.")
    except httpx.RequestError as e:
        logging.error(f"Request error while fetching Searx domains: {e}")
    except Exception as e:
        logging.error(f"Unexpected exception while fetching Searx domains: {e}")

    return domains

async def wait_between_requests(min_wait: int = 5, max_wait: int = 15):
    delay = random.uniform(min_wait, max_wait)
    await asyncio.sleep(delay)



async def fetch_page(
    url: str,
    client: httpx.AsyncClient,
    headers: Dict[str, str],
    search_params: Optional[Dict[str, str]] = None,
    mode: str = "get",
    timeout: int = 30,
    json_mode: bool = False,
) -> Dict[str, Optional[str]]:
    """
    Fetches a web page and returns its content, status code, and final URL.
    """
    if search_params:
        logging.debug(f"Search Params: {search_params}")

    try:
        if mode == "get":
            response = await client.get(
                url, headers=headers, timeout=timeout, params=search_params
            )
        elif mode == "post":
            response = await client.post(
                url, headers=headers, timeout=timeout, data=search_params
            )
        else:
            logging.error(f"Unsupported HTTP method: {mode}")
            return {"content": None, "status": "error", "url": url}

        logging.info(f"{mode.upper()} {response.status_code} - {response.url}")
        return await process_response(response, json_mode)

    except httpx.RequestError as e:
        logging.error(f"Request error while fetching {url}: {e}")
    except httpx.TimeoutException:
        logging.error(f"Timeout while fetching {url}")
    except Exception as e:
        logging.error(f"Unexpected error while fetching {url}: {e}")

    return {"content": None, "status": "error", "url": url}


async def process_response(
    response: httpx.Response, json_mode: bool
) -> Dict[str, Optional[str]]:
    """
    Processes the HTTP response and extracts content based on the mode.
    """
    if response.status_code in {200, 202}:
        if json_mode:
            try:
                json_content = response.json()
                return {
                    "content": json_content,
                    "status": response.status_code,
                    "url": str(response.url),
                }
            except Exception as e:
                logging.error(f"Error parsing JSON: {e}")
        try:
            text_content = response.text
            return {
                "content": text_content,
                "status": response.status_code,
                "url": str(response.url),
            }
        except Exception as e:
            logging.error(f"Error reading text content: {e}")
    else:
        logging.warning(f"Unexpected status code: {response.status_code}")
    return {"content": None, "status": response.status_code, "url": str(response.url)}


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
    rate_limit: int = 60,
    index_param_mode: str = "page",
    items_per_page: int = 10,
    starting_index: Optional[int] = None,
    json_mode: bool = False,
    retries: int = 2,
    max_pages: int = 20,  # Added max_pages parameter with default value of 20
):
    """
    Generic search engine function to perform searches and yield extracted links.
    It tries different base URLs if some fail.

    Yields:
        A dictionary representing a single search result.
    """
    current_index = (
        starting_index
        if starting_index is not None
        else (1 if index_param_mode == "page" else 0)
    )
    formatted_keyword = keyword
    total_urls = len(base_urls)
    url_attempt = 0  # Index to track current base_url

    headers = {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-GB,en;q=0.5",
        "Content-Type": "application/x-www-form-urlencoded",
        "Upgrade-Insecure-Requests": "1",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "same-origin",
        "Sec-Fetch-User": "?1",
        "Sec-GPC": "1",
        "Priority": "u=0, i",
        "Pragma": "no-cache",
        "Cache-Control": "no-cache",
    }
    async with httpx.AsyncClient(headers=headers) as client:
        while url_attempt < total_urls:
            base_url = base_urls[url_attempt]
            params = search_params.copy() if search_params else {}
            query = (
                f'"{formatted_keyword}" site:{site}'
                if site
                else f'"{formatted_keyword}"'
            )
            params[query_param] = query
            if page_param:
                params[page_param] = str(current_index)

            url = base_url

            # Rate limiting
            await wait_between_requests()

            # Check if current_index exceeds max_pages
            if max_pages and current_index > max_pages:
                logging.info(
                    f"Reached max_pages ({max_pages}) for engine {engine_name}. Stopping search."
                )
                break
            headers = {
                "User-Agent": random.choice(USER_AGENTS),
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "en-GB,en;q=0.5",
                "Content-Type": "application/x-www-form-urlencoded",
                "Upgrade-Insecure-Requests": "1",
                "Sec-Fetch-Dest": "document",
                "Sec-Fetch-Mode": "navigate",
                "Sec-Fetch-Site": "same-origin",
                "Sec-Fetch-User": "?1",
                "Sec-GPC": "1",
                "Priority": "u=0, i",
                "Pragma": "no-cache",
                "Cache-Control": "no-cache",
            }
            result = await fetch_page(
                url,
                client,
                headers=headers,
                search_params=params,
                mode=mode,
                json_mode=json_mode,
            )
            content, status, final_url = (
                result["content"],
                result["status"],
                result["url"],
            )

            if status == 404:
                if current_index == (
                    starting_index
                    if starting_index is not None
                    else (1 if index_param_mode == "page" else 0)
                ):
                    logging.warning(
                        f"404 on first page for URL: {base_url}. Moving to next URL."
                    )
                    url_attempt += 1
                    current_index = (
                        starting_index
                        if starting_index is not None
                        else (1 if index_param_mode == "page" else 0)
                    )
                    continue
                else:
                    logging.warning(
                        f"404 on page {current_index} for URL: {base_url}. Ending search for this URL."
                    )
                    break  # Stop searching this URL
            elif status == 429:
                logging.warning(
                    f"429 Too Many Requests for URL: {base_url}. Initiating backoff."
                )
                await exponential_backoff(url_attempt, min_wait)
                url_attempt += 1
                current_index = (
                    starting_index
                    if starting_index is not None
                    else (1 if index_param_mode == "page" else 0)
                )
                continue
            elif status in {200, 202} and content:
                try:
                    if json_mode:
                        new_links = extract_links_json(content, keyword, engine_name)
                    else:
                        soup = BeautifulSoup(content, "html.parser")
                        new_links = extract_links(
                            soup, site, base_url, engine_name, keyword
                        )

                    # Yield each link instead of returning a list
                    for link in new_links:
                        yield link

                    # If fewer results than expected, likely no more pages
                    if len(new_links) < items_per_page:
                        break

                    current_index += 1

                    # Check again after incrementing
                    if max_pages and current_index > max_pages:
                        logging.info(
                            f"Reached max_pages ({max_pages}) for engine {engine_name}. Stopping search."
                        )
                        break

                except Exception as e:
                    logging.error(f"Error parsing links: {e}")
                    url_attempt += 1
                    current_index = (
                        starting_index
                        if starting_index is not None
                        else (1 if index_param_mode == "page" else 0)
                    )
                    continue
            else:
                logging.error(
                    f"Unexpected response status: {status} | URL: {final_url}"
                )
                url_attempt += 1
                current_index = (
                    starting_index
                    if starting_index is not None
                    else (1 if index_param_mode == "page" else 0)
                )
                continue

        if url_attempt >= total_urls:
            logging.info(
                f"Maximum URLs attempted, ending search for keyword: {keyword} with site: {site}"
            )


def extract_links_json(
    data: Dict, keyword: str, engine_name: str
) -> List[Dict[str, str]]:
    """
    Extracts links from JSON responses.
    """
    links = []
    for result in data.get("results", []):
        link = result.get("url", "")
        title = result.get("title", link)
        snippet = result.get("content", "")
        links.append(
            {
                "link": link,
                "link_text": title,
                "snippet": snippet,
                "keyword": keyword,
                "engine": engine_name,
            }
        )
    return links


def extract_links(
    soup: BeautifulSoup,
    site: Optional[str],
    base_url: str,
    engine_name: str,
    keyword: str,
) -> List[Dict[str, str]]:
    """
    Extracts and returns cleaned links from a BeautifulSoup object based on the engine.
    """
    links = []
    unwanted_texts = [
        "privacy policy",
        "terms of service",
        "public instances",
        "source code",
        "issue tracker",
        "contact instance maintainer",
        "donate",
        "about",
    ]
    unwanted_domains = [
        "github.com",
        "searx.space",
        "iubenda.com",
        "indst.eu",
        "canine.tools",
        "fairkom.eu",
        "vojk.au",
        "buechter.org",
    ]

    engine_lower = engine_name.lower()

    if engine_lower == "searx":
        for link in soup.find_all("a", href=True):
            href = link["href"]
            if any(domain in href for domain in unwanted_domains):
                continue
            if site is None or site in href:
                if href.startswith(("http", "https", "www")):
                    link_text = link.get_text(strip=True) or href
                    if any(
                        unwanted in link_text.lower() for unwanted in unwanted_texts
                    ):
                        continue
                    links.append(
                        {
                            "link": href,
                            "link_text": link_text,
                            "keyword": keyword,
                            "engine": engine_name,
                        }
                    )
    elif engine_lower == "bbc":
        results_section = soup.find_all("div", class_="ssrcss-1mhwnz8-Promo")
        if not results_section:
            return links

        for result in results_section:
            try:
                title_tag = result.find("a", class_="ssrcss-its5xf-PromoLink")
                title = title_tag.text.strip() if title_tag else "No title"
                url = (
                    title_tag["href"]
                    if title_tag and "href" in title_tag.attrs
                    else "No URL"
                )
                date_tag = result.find("span", class_="ssrcss-1if1g9v-MetadataText")
                date_published = date_tag.text.strip() if date_tag else "No date"
                section_tag = result.find("span", class_="ssrcss-ar8sc2-IconContainer")
                section = section_tag.text.strip() if section_tag else "No section"
                summary_tag = result.find("p", class_="ssrcss-1q0x1qg-Paragraph")
                summary = summary_tag.text.strip() if summary_tag else "No summary"

                if url.startswith("/"):
                    url = urljoin("https://www.bbc.co.uk", url)

                links.append(
                    {
                        "link": url,
                        "link_text": title,
                        "date_published": date_published,
                        "section": section,
                        "summary": summary,
                        "keyword": keyword,
                        "engine": engine_name,
                    }
                )
            except Exception as e:
                logging.error(f"Error extracting BBC link: {e}")
                continue

    elif engine_lower == "duckduckgo":
        logging.debug("Processing DuckDuckGo results.")
        for result in soup.find_all("div", class_="links_main"):
            for link in result.find_all("a", href=True):
                href = link["href"]
                if site is None or site in href:
                    link_text = link.get_text(strip=True) or href
                    links.append(
                        {
                            "link": href,
                            "link_text": link_text,
                            "keyword": keyword,
                            "engine": engine_name,
                        }
                    )

    else:
        for link in soup.find_all("a", href=True):
            href = urljoin(base_url, link["href"])
            link_text = link.get_text(strip=True) or href
            if site is None or site in href:
                links.append(
                    {
                        "link": href,
                        "link_text": link_text,
                        "keyword": keyword,
                        "engine": engine_name,
                    }
                )
    return links





async def exponential_backoff(error_count: int, min_wait: int = 5):
    delay = min(min_wait * (2**error_count), 300)  # Max 5 minutes
    await asyncio.sleep(delay)


async def duckduckgo_search(keyword: str, site: Optional[str] = None):
    """
    Performs a DuckDuckGo search and yields search results as they are found.

    Yields:
        A dictionary representing a single search result.
    """
    base_url = "https://html.duckduckgo.com/html"
    search_query = f'"{keyword}" site:{site}' if site else f'"{keyword}"'
    search_params = {"q": search_query}

    async for result in search_engine(
        keyword=keyword,
        site=site,
        base_urls=[base_url],
        engine_name="DuckDuckGo",
        search_params=search_params,
        mode="get",
    ):
        if "cached" == result["link_text"]:
            continue
        yield result


async def duckduckgo_news_search(keyword: str, site: Optional[str] = None):
    """
    Performs a DuckDuckGo news search as an async generator.

    Yields:
        A dictionary for each result found.
    """
    base_url = "https://html.duckduckgo.com/html"
    search_query = f'"{keyword}" site:{site}' if site else f'"{keyword}"'
    search_params = {"q": search_query, "iar": "news", "ia": "news"}

    # Instead of returning a list, we now iterate over the results yielded by search_engine.
    async for result in search_engine(
        keyword=keyword,
        site=site,
        base_urls=[base_url],
        engine_name="DuckDuckGo",
        search_params=search_params,
        mode="get",
    ):
        if "cached" == result["link_text"]:
            continue
        yield result

async def bbc_search(keyword: str):
    """
    Performs a BBC search and yields results as they are found.

    Yields:
        Dict[str, str]: A dictionary containing details about a single search result.
    """
    base_url = "https://www.bbc.co.uk/search"
    search_params = {"q": keyword, "d": "NEWS_PS"}

    # Since search_engine is now an async generator, we iterate over it with `async for`.
    async for result in search_engine(
        keyword=keyword,
        site=None,
        base_urls=[base_url],
        engine_name="BBC",
        search_params=search_params,
        mode="get",
        query_param="q",
        page_param="page",
        min_wait=30,
    ):
        yield result


async def searx_search(keyword: str, site: Optional[str] = None):
    """
    Performs a Searx search and yields search results as they are found.
    """
    search_params = {
        "category_general": "1",
        "language": "en-GB",
        "time_range": "",
        "safesearch": "0",
        "theme": "simple",
    }
    domain_list = await get_searx_domains()
    async for result in search_engine(
        keyword=keyword,
        site=site,
        base_urls=domain_list,
        engine_name="Searx",
        search_params=search_params,
        mode="post",
        min_wait=5,
        page_param="pageno",
        retries=6,  # Increased retries to account for multiple paths
        max_pages=20,  # Ensure max_pages is set; optional since default is 20
    ):
        if "cached" == result["link_text"]:
            continue
        yield result


async def searx_search_news(keyword: str, site: Optional[str] = None):
    """
    Performs a Searx news search and yields results as they are found.
    """
    search_params = {
        "language": "en-GB",
        "time_range": "",
        "safesearch": "0",
        "theme": "simple",
        "category_news": "1",
        "categories": "news",
    }
    domain_list = await get_searx_domains()
    async for result in search_engine(
        keyword=keyword,
        site=site,
        base_urls=domain_list,
        engine_name="Searx",
        search_params=search_params,
        mode="post",
        min_wait=100,
        page_param="pageno",
        retries=6,  # Increased retries to account for multiple paths
        max_pages=20,  # Ensure max_pages is set; optional since default is 20
    ):
        if "cached" == result["link_text"]:
            continue
        yield result


async def lookup_keyword(
    keyword: str, domain: Optional[str] = None
) -> List[Dict[str, str]]:
    """
    Looks up a keyword across multiple search engines and aggregates the results.
    """
    results = []
    searx_gen = searx_search(keyword, domain)
    async for i in searx_gen:
        results.append(i)
    return results


# Example usage
async def main():
    domain = None  # or specify a site like "example.com"
    import keywords

    for keywork in keywords.KEYWORDS:
        print("Searching for keyword:", keywork)
        async for result in searx_search_news(keywork, domain):
            print(result)


if __name__ == "__main__":
    asyncio.run(main())
