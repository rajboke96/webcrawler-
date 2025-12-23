import asyncio
import logging

from file_buffer_queue import FileQueue
from file_buffer_queue import QueueOverFLow, QueueUnderFlow

from decorators import cal_time, decorate_with
from htmldomparser import HTMLDocumentParser

from helper import is_valid_url
from api_helper import fetch

import async_webcrawler_config as config

from config import log_dir
from config import data_dir

level=logging.DEBUG
filename=f"{log_dir}/test.log"
filemode="w"

fqueue_dir=f"{data_dir}/queue"
fqueue_item_size=500

max_mqueue_size=20

local_buffer_size=100
max_local_buffer=fqueue_item_size+local_buffer_size

logging.basicConfig(level=level, filename=filename, filemode=filemode)

async def task(item):
    items=[]
    await asyncio.sleep(1) # Work
                
    # Add new work to LOCAL list, not a new task
    # if item < 20:
    # enqueue
    for i in range(10):
        pass
        # items.append(f"{item}")
    return items

async def worker(name, queue, task):
    # Each worker has its own small local buffer
    local_buffer = []
    logging.info(f"Worker {name} - Entering infinite loop..")
    file_queue=FileQueue(queue_dir=fqueue_dir)
    while True:
        # await asyncio.sleep(0.5)
        # 1. Try to get a task from the main queue
        # If queue is empty AND local_buffer is empty, we wait.
        # If local_buffer has items, we should prioritize flushing them.
        try:
            logging.debug(f"Worker {name} - local_buffer size: {len(local_buffer)}")
            # If we have local items, don't block on get(); 
            # try to get one if available, otherwise move to flush.
            logging.debug(f"Worker {name} - Not waiting for dequeuing!")
            # item = queue.get_nowait()
            item = await queue.get()
        except asyncio.QueueEmpty:
            logging.warning(f"Worker {name} - Queue Empty")
            item = None
            # await asyncio.sleep(0.1)

        try:
            if item is not None:
                logging.debug(f"Worker {name} - processing {item}")
                local_buffer.extend(await task(item, name))
        finally:
            if item is not None:
                queue.task_done()
        
        # load local_buffer from file_queue if queue is empty!
        if not local_buffer:
            if queue.empty():
                if file_queue.is_empty():
                    logging.info("All items are processed! Exiting!")
                    break
                else:
                    local_buffer=file_queue.dequeue_bulk()

        # FLUSH: Move items from local buffer to the queue
        logging.debug(f"Worker {name} - local_buffer size: {len(local_buffer)}")
        while local_buffer:
            try:
                task_to_add = local_buffer[0]
                logging.debug(f"Worker {name} - Not waiting for enqueing!")
                queue.put_nowait(task_to_add)
                local_buffer.pop(0) # Remove only if successfully added
            except asyncio.QueueFull:
                logging.warning(f"Worker {name} - Queue Full")
                await asyncio.sleep(0.1)
                # Main queue is full. Stop flushing. 
                # Remaining items stay in local_buffer for the next cycle.
                break
        
        # Re-enqueue to file_queue if local buffer is max full!
        if len(local_buffer) > max_local_buffer:
            logging.debug(f"Worker {name} - {local_buffer[0:fqueue_item_size]}")
            file_queue.enqueue_bulk(local_buffer[0:fqueue_item_size])
            local_buffer=local_buffer[fqueue_item_size:]

async def consumer(url, name="Worker", default_path=None):
    global visited_links, skipped_visited_count
    filtered_links=[]
    if url in visited_links:
        logging.debug(f"Worker {name} - Skipping, Already visited link: {url}")
        return []
    logging.debug(f"Worker {name} - Scraping URL: {url}")
    logging.debug(f"Worker {name} - visited links size: {len(visited_links)}")
    logging.debug(f"Worker {name} - skipped_visited_count: {skipped_visited_count}")
    visited_links.append(url)
    html_doc=await fetch(url, outfilepath=config.outfilepath)
    await asyncio.sleep(2)
    # logging.debug(html_doc)
    if html_doc:
        ws=HTMLDocumentParser(url, html_doc)
        links=ws.get_all_links(default_path=default_path)
        logging.debug(f"Worker {name} - Total links found: {len(links)}")
        if links:
            for link in links:
                if link in visited_links:
                    skipped_visited_count+=1
                    logging.debug(f"Worker {name} - Skipping, Already visited link: {link}")
                    continue
                filtered_links.append(f"{link}")
    return filtered_links
    # return []

visited_links=[]
skipped_visited_count=0
async def main():
    default_path=None
    # url="https://bloo.io/"

    url="https://www.w3schools.com/"

    # url="https://premium.mysirg.com/learn"
    # default_path="/learn"

    # url="https://dnif.it/"
    queue = asyncio.Queue(maxsize=max_mqueue_size)
    file_queue=FileQueue(queue_dir=fqueue_dir)
    if file_queue.is_empty():
        # queue.put_nowait(f"link1\n") # Start seed
        # queue.put_nowait(f"link2\n") # Start seed
        # queue.put_nowait(f"link3\n") # Start seed
        queue.put_nowait(f"{url}\n") # Start seed
    workers = [asyncio.create_task(worker(f"W-{i}", queue, consumer)) for i in range(20)]
    await queue.join()
    [await worker for worker in workers]

asyncio.run(main())