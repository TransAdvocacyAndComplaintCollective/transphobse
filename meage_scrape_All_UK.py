import sys
import asyncio
from SPARQLWrapper import SPARQLWrapper, JSON
import utils.keywords as kw
from meage_scrape import KeyPhraseFocusCrawler

# Endpoint URL for the SPARQL query
ENDPOINT_URL = "https://query.wikidata.org/sparql"

# SPARQL Queries
QUERIES = {
    "query1": """
    SELECT ?mass_media ?official_website ?country_of_originLabel ?countryLabel 
           ?place_of_publication ?place_of_publicationLabel 
           ?language_of_work_or_name ?language_of_work_or_nameLabel ?mass_mediaLabel
    WHERE {
      SERVICE wikibase:label { bd:serviceParam wikibase:language "[AUTO_LANGUAGE],en". }
      
      ?mass_media (wdt:P31/(wdt:P279*)) wd:Q1193236;
                  wdt:P856 ?official_website.
      
      OPTIONAL { ?mass_media wdt:P495 ?country_of_origin. }
      OPTIONAL { ?mass_media wdt:P17 ?country. }
      OPTIONAL { ?mass_media wdt:P407 ?language_of_work_or_name. }
      ?mass_media wdt:P291 ?place_of_publication.
      
      FILTER (
        ?place_of_publication = wd:Q145 ||
        EXISTS { ?place_of_publication wdt:P17 wd:Q145. }
      )
    }
    """,
    "query2": """
        SELECT ?mass_media ?official_website ?country_of_originLabel ?countryLabel ?place_of_publication ?place_of_publicationLabel ?language_of_work_or_name ?language_of_work_or_nameLabel ?mass_mediaLabel ?web_feed_URL WHERE {
          SERVICE wikibase:label { bd:serviceParam wikibase:language "[AUTO_LANGUAGE],en". }
          ?mass_media (wdt:P31/(wdt:P279*)) wd:Q1193236;
            wdt:P856 ?official_website.
          OPTIONAL { ?mass_media wdt:P495 ?country_of_origin. }
          OPTIONAL { ?mass_media wdt:P17 ?country. }
          OPTIONAL { ?mass_media wdt:P407 ?language_of_work_or_name. }
          OPTIONAL { ?mass_media wdt:P291 ?place_of_publication. }
          FILTER((?country = wd:Q145) || (?language_of_work_or_name = wd:Q7979))
          OPTIONAL { ?mass_media wdt:P1019 ?web_feed_URL. }
        }
    """,
    "query3": """
    SELECT ?mass_media ?official_website ?official_website_languageLabel ?country_of_originLabel ?countryLabel ?place_of_publication ?place_of_publicationLabel ?language_of_work_or_name ?language_of_work_or_nameLabel ?mass_mediaLabel ?web_feed_URL WHERE {
      SERVICE wikibase:label { bd:serviceParam wikibase:language "[AUTO_LANGUAGE],en". }
      ?mass_media (wdt:P31/(wdt:P279*)) wd:Q1193236;
        wdt:P856 ?official_website.
      OPTIONAL { ?mass_media wdt:P495 ?country_of_origin. }
      OPTIONAL { ?mass_media wdt:P17 ?country. }
      OPTIONAL { ?mass_media wdt:P407 ?language_of_work_or_name. }
      OPTIONAL { ?mass_media wdt:P291 ?place_of_publication. }
      ?mass_media p:P856 ?website_statement.
      ?website_statement ps:P856 ?official_website;
        pq:P407 ?official_website_language.
      FILTER((?official_website_language = wd:Q7979))
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
        for result in results["results"]["bindings"]:
            mass_media = result.get("mass_mediaLabel", {}).get("value", "N/A")
            website = result.get("official_website", {}).get("value", "N/A")
            country_of_origin = result.get("country_of_originLabel", {}).get("value", "N/A")
            country = result.get("countryLabel", {}).get("value", "N/A")
            place_of_publication = result.get("place_of_publicationLabel", {}).get("value", "N/A")
            language = result.get("language_of_work_or_nameLabel", {}).get("value", "N/A")

            urls[website] = {
                'mass_media': mass_media,
                'country_of_origin': country_of_origin,
                'country': country,
                'place_of_publication': place_of_publication,
                'language': language
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
    if not collected_urls:
        print("No URLs collected. Exiting.")
        return

    start_urls = list(collected_urls.keys())
    allowed_subdirs_cruel = start_urls
    keywords = kw.KEYWORDS

    crawler = KeyPhraseFocusCrawler(
        start_urls=start_urls,
        feeds=[],
        name="UK_news_mage_scrape",
        allowed_subdirs_cruel=allowed_subdirs_cruel,
        keywords=keywords,
        start_data=None,
        end_data=None,
        exclude_lag=[],
        # exclude_subdirs_scrape=[],
        # allow_for_recruiting=False,
    )
    await crawler.crawl_start()

if __name__ == "__main__":
    asyncio.run(main())
