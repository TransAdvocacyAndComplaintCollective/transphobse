import asyncio
import random
import csv
from urllib.parse import urlencode, urlparse, parse_qs, urljoin
import aiohttp
from bs4 import BeautifulSoup
import urllib
import utils.keywords as kw
import ssl


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


async def fetch_page(url, session, headers, search_params=None, mode="get", timeout=30):
    """Handles fetching a page and returning the response text, status code, and URL."""
    try:
        if mode == "get":
            async with session.get(
                url, headers=headers, ssl=False, timeout=timeout
            ) as response:
                print(f"Fetching URL: {response.url}")
                if response.status in {200, 202}:
                    return {
                        "content": await response.text(),
                        "status": response.status,
                        "url": str(response.url),
                    }
                else:
                    return {
                        "content": await response.text(),
                        "status": response.status,
                        "url": str(response.url),
                    }
        elif mode == "post":
            async with session.post(
                url, headers=headers, ssl=False, timeout=timeout, data=search_params
            ) as response:
                print(f"Posting to URL: {response.url}")
                print(f"Status: {response.status}")
                print("Headers:", response.headers)
                if response.status in {200, 202}:
                    return {
                        "content": await response.text(),
                        "status": response.status,
                        "url": str(response.url),
                    }
                else:
                    return {
                        "content": await response.text(),
                        "status": response.status,
                        "url": str(response.url),
                    }
    except (aiohttp.ClientError, asyncio.TimeoutError, aiohttp.ClientSSLError) as e:
        print(f"Error fetching {url}: {type(e).__name__}")
        return {"content": None, "status": type(e).__name__, "url": url}



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
    min_wait=30,
    index_param_mode="page",
    items_per_page=10,
    starting_index=None,
):
    """Template function for search engines, simplifying the code for each engine."""
    # Normalize base_url to a list if needed
    base_url = base_url if isinstance(base_url, list) else [base_url]

    # Initialize current index based on `index_param_mode`
    current_index = starting_index if starting_index is not None else (1 if index_param_mode == "page" else 0)
    keyword = keyword.replace(" ", space)

    user_agents = [
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Ubuntu Chromium/37.0.2062.94 Safari/537.36",
        "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/45.0.2454.85 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_5) AppleWebKit/600.8.9 (KHTML, like Gecko) Version/8.0.8 Safari/600.8.9",
        "Mozilla/5.0 (iPad; CPU OS 8_4_1 like Mac OS X) AppleWebKit/600.1.4 (KHTML, like Gecko) Version/8.0 Mobile/12H321 Safari/600.1.4",
    ]
    
    links = []
    ssl_context = ssl.create_default_context()
    ssl_context.check_hostname = False
    ssl_context.verify_mode = ssl.CERT_NONE

    async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(ssl=ssl_context)) as session:
        error_count = 0
        while error_count <= 3:
            # if len(links) >= 200:
            #     await asyncio.sleep(min_wait)
            #     return links
            cont_last_new_links = 0
            # Update search parameters
            search_params = search_params or {}
            search_params[query_param] = f'"{keyword}"{space}site%3{site}' if site else f'"{keyword}"'
            if page_param:
                search_params[page_param] = str(current_index)
            print(f"Searching for {keyword} on {engine_name}...")
            print(f"Current index: {current_index}")
            print(f"Search parameters: {search_params}")
            print(f"Base URL: {base_url}")
            headers = {
                "User-Agent": random.choice(user_agents),
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Content-Type": "application/x-www-form-urlencoded",
                "Sec-Fetch-Dest": "document",
                "Sec-Fetch-Mode": "navigate",
                "Sec-Fetch-Site": "same-origin",
                "Sec-GPC": "1",
                "Accept-Language": "en-US,en;q=0.5",
                # "Accept-Encoding": "gzip, deflate, br, zstd",
                "Origin": "null",
                "Connection": "keep-alive",
                "Upgrade-Insecure-Requests": "1",
                "Priority": "u=0, i"
            }

            url = f"{random.choice(base_url)}?{urlencode(search_params)}" if mode == "get" else random.choice(base_url)
            result = await fetch_page(url, session, headers, search_params, mode)
            text, status, url_out = result["content"], result["status"], result["url"]
            print(f"Status: {status}")

            if status in [429, 403]:  # Rate limit
                print(f"Rate limited by {engine_name}. Waiting {min_wait} seconds...")
                print(f"URL: {url_out}")
                print(f"Status: {status}")
                print(f"Content: {text}")
                error_count += 1
                await asyncio.sleep(min(60 * error_count, min_wait))
                continue
            elif status in [200, 202] and text:
                print(f"Successfully fetched {url_out}")
                bs = BeautifulSoup(text, "html.parser")
                new_links = extract_links(bs, site, url, engine_name, keyword)
                
                # Add only unique links
                link_urls = {link.get("link") for link in links}
                c = [link for link in new_links if link.get("link") not in link_urls]
                links.extend(c)
                print(f"Found {len(c)} new links", len(c))
                if len(c) < cont_last_new_links or len(c) == 0 or len(c) < items_per_page:
                    return links
                cont_last_new_links = len(c)
                
                # Determine next page
                next_page_url, search_params_new = find_next_page(
                    bs, url, current_index, page_param, index_param_mode, items_per_page, param=search_params, mode=mode
                )
                if search_params_new != search_params:
                    search_params = search_params_new
                elif not next_page_url or next_page_url == url:
                    return links

                url = next_page_url
                current_index += 1
            else:
                error_count += 1
                await asyncio.sleep(min(20 * error_count, min_wait))

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
    links = []
    if engine_name == "Searx":
        for i in bs.find_all('a', href=True):
            href = i.get('href')
            print(href)
            class_ = i.get('class')
            # Corrected condition
            if site is None or site in href:
                if href.startswith("http") or href.startswith("https") or href.startswith("www") :
                    links.append(
                        {
                            "link": href,
                            "link_text": i.get_text(strip=True) or href,
                            "keyword": keyword,
                            "engine": engine_name,
                        }
                    )
    elif engine_name == "DuckDuckGo":
        for i in bs.find_all('div', class_='links_main'):
            for j in i.find_all('a', href=True):
                href = j.get('href')
                print(href)
                if site is None or (site in href):
                    links.append(
                        {
                            "link": href,
                            "link_text": j.get_text(strip=True) or href,
                            "keyword": keyword,
                            "engine": engine_name,
                        }
                    )
    else:
        # Existing code for other engines
        for link in bs.find_all("a", href=True):
            href = urljoin(base_url, link["href"])
            link_text = link.get_text(strip=True) or href
            # Corrected condition
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






async def duckduckgo_search(keyword, site=None):
    base_url = "https://html.duckduckgo.com/html"
    search_params = {"q": f'"{keyword}" site:{site}'}
    return await search_engine(
        keyword,
        site,
        base_url,
        engine_name="DuckDuckGo",
        search_params=search_params,
        mode="get",
    )


async def searx_search(keyword, site=None, News=False):
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
        min_wait=100,
        page_param= "pageno"
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
        if file.tell() == 0:
            writer.writeheader()

        # Iterate through each filtered keyword
        for keyword in filtered_keywords:
            print("Searching for:", keyword)

            # Collect results from each search engine function
            duckduckgo_results = await duckduckgo_search(keyword, None)
            bbc_results = await bbc_search(keyword)
            searx_results = await searx_search(keyword, site=None)

            # Combine all results into a single list for ease of writing to CSV
            all_results = duckduckgo_results + bbc_results + searx_results
            
            # Write each result with the current keyword into the CSV
            for result in all_results:
                result["keyword"] = keyword
                writer.writerow(result)

    print("CSV file saved as 'data/search_results.csv'")

if __name__ == "__main__":
    asyncio.run(main())