import asyncio
import logging

from decorators import cal_time, decorate_with
from htmldomparser import HTMLDocumentParser

from helper import is_valid_url
from api_helper import fetch

import async_webcrawler_config as config

logging.basicConfig(level=config.level, filename=config.filename, filemode=config.filemode)

# async def link_producer(queue, link):
#     await queue.put(link)

async def task_producer(queue, semaphore, tasks, name="task_producer"):
    logging.debug(f"{name} producing tasks of size: {len(tasks)}")
    for task in tasks:
        logging.debug(f"Is full: {queue.full()}")
        async with semaphore:
            await queue.put(task)

task_producer_count=0
def add_queue_worker(queue, semaphore):
    def decorator(func):
        async def wrapper(*args, **kwargs):
            global task_producer_count
            while True:
                task=await queue.get()
                logging.debug(f"Task: {task}")
                if not task:
                    break
                logging.debug(f"Processing task: {task}")
                sub_tasks = await func(task, *args, **kwargs)
                logging.debug(f"sub_tasks: {sub_tasks}")
                if sub_tasks:
                    asyncio.create_task(task_producer(queue, semaphore, sub_tasks, name=f"task_producer-{task_producer_count+1}"))
                    task_producer_count+=1
                queue.task_done()
        return wrapper
    return decorator

# Allow only 3 concurrent tasks
concurrency_limit = 10
semaphore = asyncio.Semaphore(value=concurrency_limit)
queue=asyncio.Queue(maxsize=10)

@add_queue_worker(queue, semaphore)
async def consumer(url, default_path=None):
    global visited_links, skipped_visited_count
    task_list=[]
    logging.debug(f"Scraping URL: {url}")
    visited_links.append(url)
    html_doc=await fetch(url, outfilepath=config.outfilepath)
    # logging.debug(html_doc)
    if html_doc:
        ws=HTMLDocumentParser(url, html_doc)
        links=ws.get_all_links(default_path=default_path)
        logging.debug(f"Total links found: {len(links)}")
        if links:
            for task in links:
                if task in visited_links:
                    skipped_visited_count+=1
                    logging.debug(f"Skipping, Already visited link: {task}")
                    continue
                task_list.append(task)
    return task_list

# @cal_time
# @decorate_with()
async def webscraper(url, default_path=None):
    try:
        logging.info(f"Scraping Domain: {url}")
        workers = [asyncio.create_task(consumer(default_path=default_path)) for i in range(20)]
        await queue.put(url)
        await queue.join()
        [await worker for worker in workers]
        [worker.cancel() for worker in workers]
    except Exception as e:
        logging.error(f"webscraper error: {e}")
        import traceback
        traceback.logging.debug_exc()

visited_links=[]
skipped_visited_count=0

def main():
    default_path=None

    url="https://www.w3schools.com/"

    asyncio.run(webscraper(url, default_path=default_path))
    logging.info(f"Total visited links: {len(visited_links)}")
    logging.info(f"Skipped already visited links: {skipped_visited_count}")

if __name__ == '__main__':
    main()