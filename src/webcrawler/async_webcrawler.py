import asyncio
import logging
import time
import os
from datetime import datetime

from file_queue import FileQueue
from file_queue import QueueOverFLow, QueueUnderFlow

from decorators import cal_time, decorate_with
from htmldomparser import HTMLDocumentParser

from helper import format_bytes
from api_helper import fetch, is_valid_url

import async_webcrawler_config as config

from config import log_dir
from config import data_dir

service_name=__name__
if __name__=="__main__":
    service_name="async_webcrawler"
level=logging.INFO
filename=f"{log_dir}/{service_name}.log"
filemode="w"

fqueue_name="queue"
fqueue_dir=f"{data_dir}"
fqueue_item_size=500

max_mqueue_size=200

local_buffer_size=100
max_local_buffer=fqueue_item_size+local_buffer_size

logging.basicConfig(level=level, filename=filename, filemode=filemode)

async def worker(name, queue, file_queue, task):
    # Each worker has its own small local buffer
    local_buffer = []
    logging.info(f"{name} - Entering infinite loop..")
    chk_time=time.time()+10
    while True:
        # log metrics
        if time.time() > chk_time:
            total_bytes=0
            count=0
            for d in os.scandir(fqueue_dir):
                total_bytes+=d.stat().st_size
                count+=1
            metric_log={
                "timestamp": datetime.now().isoformat(),
                "type": "metric_detail",
                "level": "info",
                "data":{
                    "fqueue_dir_size": format_bytes(total_bytes),
                    "fqueue_dir_size_in_bytes": total_bytes,
                    "fqueue_size": count*fqueue_item_size,
                    "local_buffer_size": len(local_buffer),
                    "mqueue_size": queue.qsize(),
                    "visited_links_cache_size": len(visited_links),
                    "skipped_duplicate_links_count": skipped_visited_count,
                    "skipped_other_content_types_links_count": skipped_other_content_type,
                    "worker": name
                },
                "message": "Montioring health!"
            }
            logging.info(f"{'-'*10}Metrics{'-'*10}")
            logging.info(metric_log)
            logging.info(f"{'-'*10}Metrics End{'-'*10}")
            chk_time=time.time()+10 # reset

        # await asyncio.sleep(0.5)
        # 1. Try to get a task from the main queue
        # If queue is empty AND local_buffer is empty, we wait.
        # If local_buffer has items, we should prioritize flushing them.
        try:
            # logging.debug(f"{name} - local_buffer size: {len(local_buffer)}")
            # If we have local items, don't block on get(); 
            # try to get one if available, otherwise move to flush.
            if local_buffer or not file_queue.is_empty():
                logging.debug(f"{name} - Not waiting for dequeuing!")
                item = queue.get_nowait()
            else:
                item = await queue.get()
        except asyncio.QueueEmpty:
            logging.debug(f"{name} - Queue Empty")
            item = None
            # await asyncio.sleep(0.1)

        try:
            if item is not None:
                logging.debug(f"{name} - processing {item}")
                local_buffer.extend(await task(item, f"{name}>Consumer"))
        finally:
            if item is not None:
                queue.task_done()
        
        # load local_buffer from file_queue if queue is empty!
        if not local_buffer:
            if queue.empty():
                if file_queue.is_empty():
                    # logging.info(f"{name} All items are processed! Exiting!")
                    # break
                    pass
                else:
                    local_buffer=file_queue.dequeue_bulk()

        # FLUSH: Move items from local buffer to the queue
        # logging.debug(f"{name} - local_buffer size: {len(local_buffer)}")
        while local_buffer:
            try:
                task_to_add = local_buffer[0]
                logging.debug(f"{name} - Not waiting for enqueing!")
                queue.put_nowait(task_to_add)
                local_buffer.pop(0) # Remove only if successfully added
            except asyncio.QueueFull:
                logging.debug(f"{name} - Queue Full")
                # await asyncio.sleep(0.1)
                # Main queue is full. Stop flushing. 
                # Remaining items stay in local_buffer for the next cycle.
                break
        
        # Re-enqueue to file_queue if local buffer is max full!
        while len(local_buffer) > max_local_buffer:
            logging.warning({
                "timestamp": datetime.now().isoformat(),
                "type": "max_threshold_crossed",
                "level": "warning",
                "data":{
                    "local_buffer_size": len(local_buffer),
                    "max_local_buffer_size": max_local_buffer,
                    "worker": name
                },
                "message": f"{name} - local_buffer max threshold - {max_local_buffer} crossed to {len(local_buffer)}"
                })
            # logging.warning(f"{name} - local_buffer max threshold - {max_local_buffer} crossed to {len(local_buffer)}")
            logging.warning(f"{name} enqueuing {len(local_buffer[0:fqueue_item_size])} links from local_buffer to file_queue")
            file_queue.enqueue_bulk(local_buffer[0:fqueue_item_size])
            local_buffer=local_buffer[fqueue_item_size:]

async def consumer(url, name="Consumer", default_path=None):
    global visited_links, skipped_visited_count, skipped_other_content_type
    filtered_links=[]
    if url in visited_links:
        logging.debug(f"{name} - Skipping, Already visited link: {url}")
        skipped_visited_count+=1
        return []
    logging.debug(f"{name} - Scraping URL: {url}")
    logging.debug(f"{name} - visited links size: {len(visited_links)}")
    logging.debug(f"{name} - skipped_visited_count: {skipped_visited_count}")
    visited_links.append(url)
    html_doc=await fetch(url, outfilepath=config.outfilepath)
    await asyncio.sleep(2)
    # logging.debug(html_doc)
    links=[]
    if html_doc:
        ws=HTMLDocumentParser(url, html_doc)
        links=ws.get_all_links(default_path=default_path)
        logging.debug(f"{name} - Total links found: {len(links)}")
        if links:
            for link in links:
                if link in visited_links:
                    skipped_visited_count+=1
                    logging.debug(f"{name} - Skipping, Already visited link: {link}")
                    continue
                filtered_links.append(f"{link}")
    else:
        skipped_other_content_type+=1
    return filtered_links

visited_links=[]
skipped_visited_count=0
skipped_other_content_type=0
async def main():
    default_path=None
    url="https://www.w3schools.com/"
    queue = asyncio.Queue(maxsize=max_mqueue_size)
    file_queue=FileQueue(queue_dir=fqueue_dir, queue_name="queue")
    if file_queue.is_empty():
        # queue.put_nowait(f"link1\n") # Start seed
        # queue.put_nowait(f"link2\n") # Start seed
        # queue.put_nowait(f"link3\n") # Start seed
        logging.info(f"Seeding with - {url}")
        queue.put_nowait(f"{url}") # Start seed
    workers = [asyncio.create_task(worker(f"Worker-{i}", queue, file_queue, consumer)) for i in range(5)]
    await queue.join()
    [await worker for worker in workers]

if __name__ == "__main__":
    asyncio.run(main())