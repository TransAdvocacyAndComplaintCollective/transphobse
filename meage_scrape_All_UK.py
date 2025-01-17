import sys
import asyncio
from SPARQLWrapper import SPARQLWrapper, JSON
import utils.keywords as kw
from meage_scrape import Crawler

# Endpoint URL for the SPARQL query
ENDPOINT_URL = "https://query.wikidata.org/sparql"

# SPARQL Queries
QUERIES = {
    "query1": """
SELECT ?mass_media ?official_website ?mass_mediaLabel WHERE {
  { ?mass_media (wdt:P31/(wdt:P279)) wd:Q1193236. }
  UNION
  {
    ?mass_media (wdt:P31/(wdt:P279)) ?parent1.
    ?parent1 (wdt:P31/(wdt:P279)) wd:Q1193236.
  }
  UNION
  {
    ?mass_media (wdt:P31/(wdt:P279)) ?parent1.
    ?parent1 (wdt:P31/(wdt:P279)) ?parent2.
    ?parent2 (wdt:P31/(wdt:P279)) wd:Q1193236.
  }
  UNION
  {
    ?mass_media (wdt:P31/(wdt:P279)) ?parent1.
    ?parent1 (wdt:P31/(wdt:P279)) ?parent2.
    ?parent2 (wdt:P31/(wdt:P279)) ?parent3.
    ?parent3 (wdt:P31/(wdt:P279)) wd:Q1193236.
  }
  UNION
  {
    ?mass_media (wdt:P31/(wdt:P279)) ?parent1.
    ?parent1 (wdt:P31/(wdt:P279)) ?parent2.
    ?parent2 (wdt:P31/(wdt:P279)) ?parent3.
    ?parent3 (wdt:P31/(wdt:P279)) ?parent4.
    ?parent4 (wdt:P31/(wdt:P279)) wd:Q1193236.
  }
  ?mass_media wdt:P856 ?official_website;
    wdt:P17 wd:Q145.
}
""",
    "query2": """
SELECT ?mass_media ?official_website ?mass_mediaLabel WHERE {
  { ?mass_media (wdt:P31/(wdt:P279*)) wd:Q1193236. }
  UNION
  {
    ?mass_media (wdt:P31/(wdt:P279*)) ?parent1.
    ?parent1 (wdt:P31*/(wdt:P279*)) wd:Q1193236.
  }
  UNION
  {
    ?mass_media (wdt:P31/(wdt:P279*)) ?parent1.
    ?parent1 (wdt:P31/(wdt:P279*)) ?parent2.
    ?parent2 (wdt:P31/(wdt:P279*)) wd:Q1193236.
  }
  UNION
  {
    ?mass_media (wdt:P31/(wdt:P279*)) ?parent1.
    ?parent1 (wdt:P31/(wdt:P279*)) ?parent2.
    ?parent2 (wdt:P31/(wdt:P279*)) ?parent3.
    ?parent3 (wdt:P31/(wdt:P279*)) wd:Q1193236.
  }
  UNION
  {
    ?mass_media (wdt:P31/(wdt:P279*)) ?parent1.
    ?parent1 (wdt:P31/(wdt:P279*)) ?parent2.
    ?parent2 (wdt:P31/(wdt:P279*)) ?parent3.
    ?parent3 (wdt:P31/(wdt:P279*)) ?parent4.
    ?parent4 (wdt:P31/(wdt:P279*)) wd:Q1193236.
  }
  UNION
  { ?mass_media wdt:P452 wd:Q11030. }
  ?mass_media wdt:P856 ?official_website;
    wdt:P17 ?country.
  FILTER(?country = wd:Q145)
}
""",
    "query3": """
SELECT ?mass_media ?official_website ?mass_mediaLabel ?newspaper_format ?newspaper_formatLabel ?parent1 ?parent1Label ?web_feed_URL WHERE {
  { ?mass_media (wdt:P31/wdt:P279) wd:Q1193236. }
  UNION
  {
    ?mass_media (wdt:P31/wdt:P279) ?parent1.
    ?parent1 (wdt:P31/wdt:P279) wd:Q1193236.
  }
  UNION
  { ?mass_media wdt:P3912 ?newspaper_format. }
  ?mass_media wdt:P856 ?official_website;
    wdt:P495 wd:Q145.
  OPTIONAL { ?mass_media wdt:P1019 ?web_feed_URL. }
}
"""
}

def get_results(endpoint_url, query):
    """
    Execute the SPARQL query and return the results in JSON format.

    Args:
        endpoint_url (str): The SPARQL endpoint URL.
        query (str): The SPARQL query string.

    Returns:
        dict: The JSON result of the query.
    """
    user_agent = f"WDQS-Python/{sys.version_info[0]}.{sys.version_info[1]}"
    sparql = SPARQLWrapper(endpoint_url, agent=user_agent)
    sparql.setQuery(query)
    sparql.setReturnFormat(JSON)

    try:
        return sparql.query().convert()
    except Exception as e:
        print(f"Error executing query: {e}")
        return None

def parse_results(results):
    """
    Parse the results of the SPARQL query.

    Args:
        results (dict): The JSON result of the query.

    Returns:
        dict: A dictionary mapping websites to their properties.
    """
    urls = {}
    if results:
        for result in results.get("results", {}).get("bindings", []):
            mass_media = result.get("mass_mediaLabel", {}).get("value", "N/A")
            website = result.get("official_website", {}).get("value", "N/A")
            if "wikidata.org" in website:
                continue

            urls[website] = {
                "mass_media": mass_media,
            }
    else:
        print("No results found or there was an error with the query.")
    return urls

def collect_urls(endpoint_url, queries):
    """
    Collect URLs from executing multiple SPARQL queries.

    Args:
        endpoint_url (str): The SPARQL endpoint URL.
        queries (dict): A dictionary of query names to query strings.

    Returns:
        dict: A dictionary of collected URLs and their properties.
    """
    collected_urls = {}
    for name, query in queries.items():
        print(f"Executing {name}...")
        results = get_results(endpoint_url, query)
        urls = parse_results(results)
        collected_urls.update(urls)
    return collected_urls

async def main():
    collected_urls = collect_urls(ENDPOINT_URL, QUERIES)
    urls = list(collected_urls.keys())

    # Use your existing keywords from utils.keywords
    keywords = kw.KEYWORDS

    # Create the crawler instance
    crawler = Crawler(
        start_urls=urls,
        allowed_url=urls,
        feeds=[],
        name="UK_news_mage_scrape",
        keywords=keywords,
        allowed_subdirs=None,
        find_new_site=True,
    )

    # Start the crawl
    await crawler.crawl_start()

if __name__ == "__main__":
    asyncio.run(main())
