import requests
import time
from bs4 import BeautifulSoup
import os

# configs
symbol, repeat_count="-", 130
outfilepath="./out.html"

def decorate_with(symbol="-", repeat_count=50):
    def decorator(func):
        def wrapper(*args, **kwargs):
            # print(str(symbol)*repeat_count)
            res=func(*args, **kwargs)
            print(str(symbol)*repeat_count)
            return res
        return wrapper
    return decorator

def cal_time(func):
    def wrapper(*args, **kwargs):
        st_time=time.time()
        res = func(*args, **kwargs)
        print("Total execution time: ", time.time()-st_time)
        return res
    return wrapper

@decorate_with(symbol, repeat_count)
def fetch(url, outfilepath=None):
    print("Fetching url: ", url)
    st_time = time.time()
    res=requests.get(url)
    end_time = time.time()
    # print(res.text)
    # print(dir(res))
    # print(res.request.headers)
    print("Response code: ", res.status_code)
    print("Total time: ", end_time-st_time, "seconds")
    if outfilepath:
        with open(outfilepath, "w") as f:
            f.write(res.text)
    else:
        return res.text

class HTMLDocumentParser:
    def __init__(self, origin, html_doc):
        self.origin=origin
        if os.path.exists(html_doc):
            self.html_doc = self.load_html_doc(html_doc)
        else:
            self.html_doc = html_doc
        http_protocol=origin.split("://")[0]
        if http_protocol=="https":
            self.http_protocol="https://"
        else:
            self.http_protocol="http://"
        # Create a BeautifulSoup object
        self.soup = BeautifulSoup(self.html_doc, 'html.parser')
        # Accessing elements
        print(f"Title: {self.soup.title.string}")
    
    @classmethod
    def load_html_doc(filepath):
        print("Loading html document: ", filepath)
        with open(filepath, "r") as f:
            html_doc = f.read()
        return html_doc
        
    @decorate_with(symbol, repeat_count)
    def get_all_links(self):
        data=[]
        for link in self.soup.find_all('a'):
            if not link.get('href').startswith("https://") and not link.get('href').startswith("http://"):
                # print(f" - Link Text: {link.string} \n URL: {link.get('href')}\n Full URL: {origin+link.get('href')}")
                # print(f"URL: {origin+link.get('href')}")
                data.append(self.origin+link.get('href'))
        return data

url="https://www.w3schools.com/"
origin="https://www.w3schools.com/"

all_links=[]
def rwebscraper(url, origin):
    if url in all_links:
        print("Duplicate URL found! URL: ", url)
        return
    print("Scraping URL: ", url)
    all_links.append(url)
    html_content=fetch(url)
    ws=HTMLDocumentParser(origin, html_content)
    links=ws.get_all_links()
    print("Total links found: ", len(links))
    if links:
        for link in links:
            rwebscraper(link, origin)

@cal_time
@decorate_with()
def webscraper(origin):
    print("Scraping Domain: ", origin)
    rwebscraper(url=origin, origin=origin)

webscraper(origin)
print("Total links visited: ", len(all_links))