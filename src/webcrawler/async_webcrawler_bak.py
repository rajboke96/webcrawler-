import asyncio
import logging

from decorators import cal_time, decorate_with
from htmldomparser import HTMLDocumentParser

from helper import is_valid_url
from api_helper import fetch

from config import outfilepath, repeat_count, symbol
from config import level, filename, filemode

logging.basicConfig(level=level, filename=filename, filemode=filemode)

async def rwebscraper(url, default_path=None):
    global visited_links, skipped_visited_count

    logging.debug(f"Scraping URL: {url}")
    visited_links.append(url)
    html_doc=await fetch(url, outfilepath=outfilepath)
    if html_doc:
        ws=HTMLDocumentParser(url, html_doc)
        links=ws.get_all_links(default_path=default_path)
        logging.debug(f"Total links found: {len(links)}")
        task_list=[]
        if links:
            for link in links:
                if link in visited_links:
                    skipped_visited_count+=1
                    logging.warning(f"Skipping, Already visited link: {link}")
                    continue
                task = asyncio.create_task(rwebscraper(link))
                task_list.append(task)
            res=await asyncio.gather(*task_list)
            logging.info(f"Total tasks completed: {len(res)}")

# @cal_time
# @decorate_with()
async def webscraper(url, default_path=None):
    logging.info(f"Scraping Domain: {url}")
    task = asyncio.create_task(rwebscraper(url=url, default_path=default_path))
    await task

visited_links=[]
skipped_visited_count=0
def main():
    default_path=None
    # url="https://www.w3schools.com/"

    asyncio.run(webscraper(url, default_path=default_path))
    logging.info(f"Total visited links: {len(visited_links)}")
    logging.info(f"Skipped already visited links: {skipped_visited_count}")

if __name__ == '__main__':
    main()