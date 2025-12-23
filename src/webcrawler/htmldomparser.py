from bs4 import BeautifulSoup
import os
from urllib.parse import urlparse, urlunsplit
import logging

from decorators import decorate_with

from helper import is_valid_url

class HTMLDocumentParser:
    def __init__(self, doc_url, html_doc):
        self.doc_url=doc_url
        self.parsed_doc_url=urlparse(doc_url)
        if os.path.exists(html_doc):
            self.html_doc = self.load_html_doc(html_doc)
        else:
            self.html_doc = html_doc
        # Create a BeautifulSoup object
        self.soup = BeautifulSoup(self.html_doc, 'html.parser')
        # Accessing elements
        logging.info(f"soup type: {type(self.soup)}")
        logging.info(f"Parsing HTML Document with Title: {self.soup.title}")
    
    @classmethod
    def load_html_doc(cls, filepath):
        logging.debug(f"Loading html document: {filepath}")
        with open(filepath, "r") as f:
            html_doc = f.read()
        return html_doc
        
    # @decorate_with(symbol, repeat_count)
    def get_all_links(self, default_path=None):
        all_links = []
        for link_tag in self.soup.find_all(href=True): # 'href=True' ensures the tag has a 'href' attribute
            href_value = link_tag['href']
            logging.debug(f"Link: {href_value}, Is valid: {is_valid_url(href_value)}")
            if not is_valid_url(href_value):
                try:
                    parsed_link=urlparse(href_value)
                    path=parsed_link.path
                    if ( not path or path == "/" ) and default_path:
                        path=default_path
                    elif not path:
                        path="/"
                    abs_href_value=urlunsplit((self.parsed_doc_url.scheme, self.parsed_doc_url.netloc, path, parsed_link.query, parsed_link.fragment))
                except ValueError as e:
                    logging.error(e)
                    logging.error(f"href_value: {href_value}")
                    # exit(1)
                logging.debug(f"Created abs_href_value: {abs_href_value}")
                all_links.append(abs_href_value)
            elif urlparse(href_value).netloc == self.parsed_doc_url.netloc:
                all_links.append(href_value)
            else:
                logging.debug(f"Skipping, CROSS ORIGIN link: {href_value}")
        logging.debug(all_links)
        return all_links

if __name__ == '__main__':
    pass