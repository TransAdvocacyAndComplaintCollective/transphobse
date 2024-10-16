from SPARQLWrapper import SPARQLWrapper, JSON
import pandas as pd


def fetch_news_media_data():
    # Set up the endpoint and query
    endpoint_url = "https://query.wikidata.org/sparql"
    sparql_query = """
    SELECT ?news_media ?news_mediaLabel ?official_website ?Reddit_username ?Instagram_username ?language_of_work_or_name ?language_of_work_or_nameLabel ?web_feed_URL ?calendar_feed_URL ?Facebook_username ?X_username ?X_numeric_user_ID ?countryLabel WHERE {
      SERVICE wikibase:label { bd:serviceParam wikibase:language "[AUTO_LANGUAGE],mul,en". }
      ?news_media (wdt:P31/(wdt:P279*)) wd:Q1193236;
        wdt:P856 ?official_website.
      VALUES ?country {
        wd:Q145
        wd:Q174193
      }
      OPTIONAL { ?news_media wdt:P4265 ?Reddit_username. }
      OPTIONAL { ?news_media wdt:P2003 ?Instagram_username. }
      OPTIONAL { ?news_media wdt:P407 ?language_of_work_or_name. }
      { ?news_media wdt:P856 ?official_website. }
      ?news_media (wdt:P495/wdt:P17) ?country.
      OPTIONAL { ?news_media wdt:P1019 ?web_feed_URL. }
      OPTIONAL { ?news_media wdt:P6818 ?calendar_feed_URL. }
      OPTIONAL { ?news_media wdt:P2013 ?Facebook_username. }
      OPTIONAL { ?news_media wdt:P2002 ?X_username. }
      OPTIONAL { ?news_media wdt:P6552 ?X_numeric_user_ID. }
    }
    """

    # Set up the SPARQL client
    sparql = SPARQLWrapper(endpoint_url)
    sparql.setQuery(sparql_query)
    sparql.setReturnFormat(JSON)

    # Execute the query and convert the result to a dictionary
    results = sparql.query().convert()
    data = {}

    # Extract relevant data and store it in a dictionary
    for result in results["results"]["bindings"]:
        news_media = result.get("news_media", {}).get("value", "")

        # Initialize an entry for the news media if not already present
        data.setdefault(news_media, {
            "official_website": set(),
            "country": set(),
            "X_username": set(),
            "web_feed_URL": set(),
            "Reddit_username": set(),
            "Instagram_username": set(),
            "news_mediaLabel": set(),
        })

        # Add data to the dictionary, checking for each optional field
        if "official_website" in result and result["official_website"]["value"] not in data[news_media]["official_website"]:
            if result["official_website"]["value"] not in data[news_media]["country"]:
                data[news_media]["official_website"].add(result["official_website"]["value"])

        if "countryLabel" in result:
            if result["countryLabel"]["value"] not in data[news_media]["country"]:
                data[news_media]["country"].add(result["countryLabel"]["value"])

        if "X_username" in result:
            if result["X_username"]["value"] not in data[news_media]["X_username"]:
                data[news_media]["X_username"].add(result["X_username"]["value"])

        if "web_feed_URL" in result:
            if result["web_feed_URL"]["value"] not in data[news_media]["web_feed_URL"]:
                data[news_media]["web_feed_URL"].add(result["web_feed_URL"]["value"])

        if "Reddit_username" in result:
            if result["Reddit_username"]["value"] not in data[news_media]["X_username"]:
                data[news_media]["Reddit_username"].add(result["Reddit_username"]["value"])

        if "Instagram_username" in result:
            if result["Instagram_username"]["value"] not in data[news_media]["Instagram_username"]:
                data[news_media]["Instagram_username"].add(result["Instagram_username"]["value"])

        if "news_mediaLabel" in result:
            if result["news_mediaLabel"]["value"] not in data[news_media]["news_mediaLabel"]:
                data[news_media]["news_mediaLabel"].add(result["news_mediaLabel"]["value"])
    return data