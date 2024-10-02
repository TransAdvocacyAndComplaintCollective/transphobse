from googlesearch import search
from fp.fp import FreeProxy
def lookup(keyword,domain):
    while True:
        try:
            re = search(f"{keyword} site:{domain}", sleep_interval=60, num_results=100,ssl_verify=False)
            for i in re:
                print(i.url)
                print(i.title)
                print(i.description)
            return
        except Exception as e:
            print("Error",e)
            continue
lookup("gender-critical feminism","bbc.co.uk")