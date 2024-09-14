from typing import List, Optional
from bs4 import BeautifulSoup
import requests


def search_keywords_in_url(url: str, keywords: List[str]) -> Optional[str]:
    """Fetch a webpage and search for specific keywords in its text content."""
    try:
        with requests.get(url, headers={'User-Agent': 'Mozilla/5.0'}, timeout=30) as response:
            response.raise_for_status()

            soup = BeautifulSoup(response.text, "html.parser")
            for script in soup(["script", "noscript"]):
                script.extract()
            page_text = soup.get_text().lower()

            for keyword in keywords:
                if keyword.lower() in page_text:
                    return keyword
        return None
    except requests.RequestException as e:
        return None
