from bs4 import BeautifulSoup
from javascript import require
import requests

# Import the ProcessArticle function from the JS file
processArticle = require("./ProcessArticle.js")

def main(url):
    # Fetch the web page using requests
    req = requests.get(url)
    
    # Call the ProcessArticle function with HTML and URL
    obj = processArticle.ProcessArticle(req.text, url)
    content = BeautifulSoup(str(obj.content), "html.parser")
    print(content)
    print(obj.excerpt)
    print(obj.siteName)
    print(obj.lang)
    print(obj.byline)
    print(obj.tlite)

# Example URL
url = "http://www.bbc.co.uk/news/business-57468351/"
main(url)
