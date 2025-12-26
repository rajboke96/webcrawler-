import aiohttp
import logging

from urllib.parse import urlparse

def is_valid_url(url):
    try:
        result = urlparse(url)
        return all([result.scheme, result.netloc])
    except ValueError:
        return False

async def fetch(url, outfilepath=None, accept_type='text/html'):
    try:
        logging.debug(f"Fetching url: {url}")
        if not is_valid_url(url):
            logging.error(f"Invalid URL: {url}")
            return
        async with aiohttp.ClientSession() as session:
            # st_time = time.time()
            async with session.get(url) as response:
                logging.debug(f"url : {url}, Status: {response.status}, headers: {response.headers}")
                if response.status == 200 and accept_type in response.headers['Content-Type']:
                    html = await response.text()
                    # logging.debug("Body:", html)
                    # end_time = time.time()
                    # logging.debug(res.text)
                    # logging.debug(dir(res))
                    # logging.debug(res.request.headers)
                    # logging.debug("Response code: ", response.status_code)
                    # logging.debug("Total time: ", end_time-st_time, "seconds")
                    if outfilepath:
                        logging.debug(f"Writting response to file: {outfilepath}")
                        with open(outfilepath, "w") as f:
                            f.write(html)
                        return outfilepath
                    else:
                        return html
    except Exception as e:
        logging.error(f"Fetching url failed: url: {url}, error: {e}")
