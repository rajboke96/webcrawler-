import json
from urllib.parse import urlparse, urlunsplit

def is_valid_url(url):
    try:
        result = urlparse(url)
        return all([result.scheme, result.netloc])
    except ValueError:
        return False
    
def format_bytes(val):
    if val <= 1024:
        return f"{val}B"
    elif val <= 1024 * 1024:
        return f"{val/1024:.2f}KB"
    elif val <= 1024 * 1024 * 1024:
        return f"{val/(1024*1024):.2f}MB"
    else:
        return f"{val/(1024*1024*1024):.2f}GB"