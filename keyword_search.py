import asyncio
import random
import csv
from urllib.parse import urlencode, urlparse, parse_qs, urljoin
import aiohttp
from bs4 import BeautifulSoup
import urllib
import utils.keywords as kw
import ssl
import logging


async def test():
    domains = []
    url = "https://searx.space/data/instances.json"
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            if response.status == 200:
                data = await response.json()
                for i in data["instances"]:
                    if data["instances"][i]["network_type"] == "normal":
                        domains.append(f"{i}search")
    return domains


import logging

async def fetch_page(url, session, headers, search_params=None, mode="get", timeout=30, json_mode=False):
    """Handles fetching a page and returning the response text, status code, and URL."""
    logging.debug(f"Starting fetch for URL: {url}, Mode: {mode}, JSON Mode: {json_mode}")
    logging.debug(f"Headers: {headers}")
    if search_params:
        logging.debug(f"Search parameters: {search_params}")

    try:
        if mode == "get":
            async with session.get(
                url, headers=headers, ssl=False, timeout=timeout
            ) as response:
                logging.info(f"Received response for GET request, Status: {response.status}, URL: {response.url}")
                if json_mode and response.status in {200, 202}:
                    try:
                        json_content = await response.json()
                        logging.debug(f"JSON response content: {json_content}")
                        return {
                            "content": json_content,
                            "status": response.status,
                            "url": str(response.url),
                        }
                    except Exception as json_error:
                        logging.error(f"Error parsing JSON response: {json_error}")
                elif response.status in {200, 202}:
                    text_content = await response.text()
                    logging.debug(f"Text response content (truncated): {text_content[:200]}...")
                    return {
                        "content": text_content,
                        "status": response.status,
                        "url": str(response.url),
                    }
                else:
                    logging.warning(f"Unexpected status code for GET request: {response.status}")
        
        elif mode == "post":
            async with session.post(
                url, headers=headers, ssl=False, timeout=timeout, data=search_params
            ) as response:
                logging.info(f"Received response for POST request, Status: {response.status}, URL: {response.url}")
                if json_mode and response.status in {200, 202}:
                    try:
                        json_content = await response.json()
                        logging.debug(f"JSON response content: {json_content}")
                        return {
                            "content": json_content,
                            "status": response.status,
                            "url": str(response.url),
                        }
                    except Exception as json_error:
                        logging.error(f"Error parsing JSON response: {json_error}")
                elif response.status in {200, 202}:
                    text_content = await response.text()
                    logging.debug(f"Text response content (truncated): {text_content[:200]}...")
                    return {
                        "content": text_content,
                        "status": response.status,
                        "url": str(response.url),
                    }
                else:
                    logging.warning(f"Unexpected status code for POST request: {response.status}")
    
    except aiohttp.ClientError as client_error:
        logging.error(f"Client error during fetch: {client_error}, URL: {url}")
    except asyncio.TimeoutError:
        logging.error(f"Timeout error during fetch, URL: {url}")
    except aiohttp.ClientSSLError as ssl_error:
        logging.error(f"SSL error during fetch: {ssl_error}, URL: {url}")
    except Exception as e:
        logging.error(f"Unexpected error during fetch: {e}, URL: {url}")

    # Return a default dictionary in case of an exception or unexpected result
    logging.debug("Returning default error response due to an issue during fetch.")
    return {"content": None, "status": "error", "url": url}






# Configure logging
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler()
    ]
)
async def search_engine(
    keyword,
    site,
    base_url,
    engine_name="generic",
    search_params=None,
    space=" ",
    page_param=None,
    mode="get",
    query_param="q",
    min_wait=10,  # Default minimum wait time (seconds)
    rate_limit=10,  # Default rate limit (seconds per request)
    index_param_mode="page",
    items_per_page=10,
    starting_index=None,
    json_mode=False,
    do=1
):
    for i in range(do):
        logging.info(f"Starting search for keyword: {keyword}, Engine: {engine_name}")

        base_url = base_url if isinstance(base_url, list) else [base_url]
        current_index = starting_index if starting_index is not None else (1 if index_param_mode == "page" else 0)
        keyword = keyword.replace(" ", space)

        user_agents = [
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Ubuntu Chromium/37.0.2062.94 Safari/537.36",
            "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/45.0.2454.85 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_5) AppleWebKit/600.8.9 (KHTML, like Gecko) Version/8.0.8 Safari/600.8.9",
            "Mozilla/5.0 (iPad; CPU OS 8_4_1 like Mac OS X) AppleWebKit/600.1.4 (KHTML, like Gecko) Version/8.0 Mobile/12H321 Safari/600.1.4",
        ]

        links = []
        async with aiohttp.ClientSession() as session:
            error_count = 0
            while error_count <= 3:
                logging.debug(f"Current index: {current_index}, Error count: {error_count}")

                search_params = search_params or {}
                search_params[query_param] = f'"{keyword}"{space}site:{site}' if site else f'"{keyword}"'
                if page_param:
                    search_params[page_param] = str(current_index)

                headers = {
                    "User-Agent": random.choice(user_agents),
                    "Content-Type": "application/x-www-form-urlencoded",
                    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/png,image/svg+xml,*/*;q=0.8",
                    "Accept-Language": "en-GB,en;q=0.5",
                    "Upgrade-Insecure-Requests": "1",
                    "Sec-Fetch-Dest": "document",
                    "Sec-Fetch-Mode": "navigate",
                    "Sec-Fetch-Site": "same-origin"
                }

                url = f"{random.choice(base_url)}?{urlencode(search_params)}" if mode == "get" else random.choice(base_url)
                logging.info(f"Fetching URL: {url} with headers: {headers}")

                try:
                    # Wait according to the rate limit before making the request
                    await asyncio.sleep(rate_limit)

                    result = await fetch_page(url, session, headers, search_params, mode, json_mode=json_mode)
                    content, status, url_out = result["content"], result["status"], result["url"]
                    logging.debug(f"Received response with status: {status}, URL: {url_out}")

                    if status in [429, 403]:  # Rate limit or forbidden
                        logging.warning(f"Rate limited or forbidden: status {status}")
                        await exponential_backoff(error_count)
                        error_count += 1
                        continue
                    elif status in [200, 202] and content:
                        logging.info(f"Successful response for keyword: {keyword}")
                        await wait_between_requests()
                        try:
                            if json_mode:
                                new_links = extract_links_json(content, keyword, engine_name)
                            else:
                                bs = BeautifulSoup(content, "html.parser")
                                new_links = extract_links(bs, site, url, engine_name, keyword)

                            logging.debug(f"Extracted {len(new_links)} new links")
                            links.extend(new_links)
                            if len(new_links) < items_per_page:
                                logging.info("No more links found, ending search")
                                return links

                            current_index += 1
                        except Exception as parse_error:
                            logging.error(f"Error parsing response: {parse_error}")
                            if json_mode:
                                logging.warning("Retrying with HTML mode after JSON parsing error.")
                                json_mode = False
                            continue
                    else:
                        logging.error(f"Unexpected status: {status}, URL: {url_out}")
                        error_count += 1
                        await asyncio.sleep(min(20 * error_count, min_wait))
                except Exception as e:
                    logging.error(f"Exception during fetch: {e}")
                    error_count += 1
                    await asyncio.sleep(min(20 * error_count, min_wait))

            logging.info("Maximum retries reached, ending search")
            return links




def extract_links_json(data, keyword, engine_name):
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


async def bbc_search(keyword):
    base_url = "https://www.bbc.co.uk/search"
    search_params = {"q": keyword, "d": "NEWS_PS"}
    return await search_engine(
        keyword,
        site=None,
        base_url=base_url,
        engine_name="BBC",
        search_params=search_params,
        mode="get",
        query_param="q",
        min_wait=30,
        page_param="page",
    )

from urllib.parse import urlparse, parse_qs, urlencode, urljoin

def find_next_page(
    bs,
    base_url,
    current_index,
    page_param=None,
    index_param_mode="page",
    items_per_page=10,
    engine_name=None,
    param=None,
    mode="get"
):
    print("Finding next page...")
    param = {**param} if param else {}

    # If a pagination parameter is provided, construct the next URL with updated parameters
    if page_param:
        if index_param_mode == "page":
            param[page_param] = str(current_index+1)
        elif index_param_mode == "item_count":
            param[page_param] = str(current_index * items_per_page)
        else:
            param[page_param] = str(current_index + 1)
        if mode == 'get':
            next_url = base_url + '?' + urlencode(param)
        else:
            next_url = base_url
        return next_url, param
    # Fallback to scanning the page for a "next" link if no pagination parameter is given
    next_strings = ["next", "more", "next >", ">", ">>","Next"]
    
    # First, try finding links with common "next" patterns
    for link in bs.find_all("a", href=True):
        text = link.get_text(strip=True).lower()
        if any(s in text for s in next_strings):
            next_url = urljoin(base_url, link["href"])
            return next_url, param

    # Additionally, check for rel="next" which is often used for pagination
    next_link = bs.find("a", rel="next")
    if next_link and next_link.get("href"):
        next_url = urljoin(base_url, next_link["href"])
        return next_url, param

    # If no next page is found, return None
    return None, None




def extract_links(bs, site, base_url, engine_name, keyword):
    """Extracts and returns cleaned links from a BeautifulSoup object."""
    logging.info(f"Extracting links for engine: {engine_name}, keyword: {keyword}")
    links = []
    unwanted_texts = [
        "privacy policy", "terms of service", "public instances", "source code",
        "issue tracker", "contact instance maintainer", "donate", "about"
    ]
    unwanted_domains = [
        "github.com", "searx.space", "iubenda.com", "indst.eu", "canine.tools",
        "fairkom.eu", "vojk.au", "buechter.org"
    ]

    if engine_name == "Searx":
        logging.debug("Processing Searx results")
        for i in bs.find_all('a', href=True):
            href = i.get('href')
            if any(domain in href for domain in unwanted_domains):
                logging.debug(f"Skipping unwanted domain: {href}")
                continue
            if site is None or site in href:
                if href.startswith("http") or href.startswith("https") or href.startswith("www"):
                    link_text = i.get_text(strip=True) or href
                    if any(unwanted in link_text.lower() for unwanted in unwanted_texts):
                        logging.log(f"Skipping unwanted text link: {link_text}")
                        continue
                    links.append({
                        "link": href,
                        "link_text": link_text,
                        "keyword": keyword,
                        "engine": engine_name,
                    })
                    logging.info(f"Extracted link: {href}")

    elif engine_name == "BBC":
        logging.debug("Processing BBC results")
        results_section = bs.find_all("div", class_="ssrcss-1mhwnz8-Promo")
        if not results_section:
            logging.warning("No results found in BBC search")
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

                if url and url.startswith("/"):
                    url = f"https://www.bbc.co.uk{url}"

                links.append({
                    "title": title,
                    "url": url,
                    "date_published": date_published,
                    "section": section,
                    "summary": summary,
                })
                logging.info(f"Extracted BBC link: {url}, title: {title}")

            except Exception as e:
                logging.error(f"Error extracting BBC result: {e}")

    elif engine_name == "DuckDuckGo":
        logging.debug("Processing DuckDuckGo results")
        for i in bs.find_all('div', class_='links_main'):
            for j in i.find_all('a', href=True):
                href = j.get('href')
                if site is None or (site in href):
                    link_text = j.get_text(strip=True) or href
                    links.append({
                        "link": href,
                        "link_text": link_text,
                        "keyword": keyword,
                        "engine": engine_name,
                    })
                    logging.info(f"Extracted DuckDuckGo link: {href}")

    else:
        logging.debug(f"Processing results for engine: {engine_name}")
        for link in bs.find_all("a", href=True):
            href = urljoin(base_url, link["href"])
            link_text = link.get_text(strip=True) or href
            if site is None or site in href:
                links.append({
                    "link": href,
                    "link_text": link_text,
                    "keyword": keyword,
                    "engine": engine_name,
                })
                logging.info(f"Extracted generic link: {href}")

    logging.info(f"Total links extracted: {len(links)}")
    return links


async def wait_between_requests(min_wait=5, max_wait=15):
    delay = random.uniform(min_wait, max_wait)
    await asyncio.sleep(delay)

async def exponential_backoff(error_count, min_wait=5):
    delay = min(min_wait * (2 ** error_count), 300)  # Maximum wait time of 5 minutes
    print(f"Rate limit detected. Waiting for {delay} seconds.")
    await asyncio.sleep(delay)

async def duckduckgo_search(keyword, site=None):
    base_url = "https://html.duckduckgo.com/html"
    search_params = {
        "q": f'"{keyword}" site:{site}'
        }
    return await search_engine(
        keyword,
        site,
        base_url,
        engine_name="DuckDuckGo",
        search_params=search_params,
        mode="get",
    )


async def duckduckgo_news_search(keyword, site=None):
    base_url = "https://html.duckduckgo.com/html"
    search_params = {
        "q": f'"{keyword}" site:{site}',
        "iar":"news",
        "ia":"news"
        }
    return await search_engine(
        keyword,
        site,
        base_url,
        engine_name="DuckDuckGo",
        search_params=search_params,
        mode="get",
    )

async def searx_search(keyword, site=None):
    search_params = {
        "category_general": "1",
        "language": "en-GB",
        "time_range": "",
        "safesearch": "0",
        "theme": "simple",
    }
    domain_list = await test()
    return await search_engine(
        keyword,
        site,
        domain_list,
        engine_name="Searx",
        search_params=search_params,
        mode="post",
        min_wait=5,
        page_param="pageno",
        do=40
    )



async def searx_search_news(keyword, site=None):
    search_params = {
        "language": "en-GB",
        "time_range": "",
        "safesearch": "0",
        "theme": "simple",
        "category_news":"1"
    }
    domain_list = await test()
    return await search_engine(
        keyword,
        site,
        domain_list,
        engine_name="Searx",
        search_params=search_params,
        mode="post",
        min_wait=100,
        page_param= "pageno",
        do=40
    )


async def lookup_keyword(keyword,domain=None):
    duckduckgo_results = await duckduckgo_search(keyword, domain)
    searx_results = await searx_search(keyword, domain)
    all_results = duckduckgo_results  + searx_results
    return all_results



##TODO add qwant and swisscows 



import utils.keywords as keywords
import aiofiles

# Assuming each search function returns a list of dicts with 'link', 'link_text', 'engine'
async def main():
    # Filtered keywords - only those longer than 5 characters
    filtered_keywords = [kw for kw in keywords.KEYWORDS if len(kw) >= 6]

    async with aiofiles.open("data/search_results.csv", mode="a", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=["link", "link_text", "keyword", "engine"])

        # Write the header only if the file is empty
        await file.seek(0, 2)  # Move to the end of the file
        if await file.tell() == 0:
            writer.writeheader()

        # Iterate through each filtered keyword
        for keyword in filtered_keywords:
            searx_news_results = await searx_search_news(keyword)
            for searx_news_result in searx_news_results:
                print(searx_news_result)
            # # Collect results from each search engine function
            # duckduckgo_results = await duckduckgo_search(keyword, None)
            # bbc_results = await bbc_search(keyword)
            # searx_results = await searx_search(keyword, site=None)

            # # Combine all results into a single list for ease of writing to CSV
            # all_results = duckduckgo_results + bbc_results + searx_results
            
            # # Write each result with the current keyword into the CSV
            # for result in all_results:
            #     result["keyword"] = keyword
            #     writer.writerow(result)

    print("CSV file saved as 'data/search_results.csv'")

if __name__ == "__main__":
    asyncio.run(main())