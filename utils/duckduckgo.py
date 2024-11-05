from time import sleep
from duckduckgo_search import DDGS
from duckduckgo_search.exceptions import RatelimitException 

def lookup_duckduckgo(keyword, domain):
    for i in range(10):
        try:
            ddgs = DDGS()
            text = f"\"{keyword}\" site:{domain}"
            results = ddgs.text(text)
            for result in results:
                yield result
            return
        except RatelimitException as e:
            sleep(5)  # Retry delay if there is an error
            return
        except Exception as e:
            print("Error:", e)
    

def lookup_duckduckgos(keywords, domain):
    for keyword in keywords:
        cout = 0
        for result in lookup_duckduckgo(keyword, domain):
            if result is None:
                return
            cout += 1
            print(cout)
            yield result
        if cout == 0:
            return

