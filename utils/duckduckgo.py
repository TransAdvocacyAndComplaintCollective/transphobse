
from time import sleep
from duckduckgo_search import DDGS

def lookup_duckduckgo(keyword,domain):
    for i in range(10):
        try:
            ddgs = DDGS()
            text = f"{keyword} site:{domain}"
            print(text)
            results = ddgs.text(text, max_results=10)
            for result in results:
                print(result)
                yield result
            break
        except Exception as e:
            sleep(60*30)
            print(dir(e))
            print(e.args)
