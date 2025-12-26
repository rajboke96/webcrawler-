import asyncio
import logging
import time
import os
from datetime import datetime

from file_queue import FileQueue

from htmldomparser_helper import HTMLDocumentParser

from helper import format_bytes
from api_helper import fetch

import async_webcrawler_config as conf

logging.basicConfig(level=conf.LOG_LEVEL, filename=conf.LOG_FILENAME, filemode=conf.LOG_FILEMODE)

async def worker(name, queue, file_queue, task):
    global local_buffer
    logging.info(f"{name} - Entering infinite loop..")
    chk_time=time.time()+10
    while True:
        # log metrics
        if time.time() > chk_time:
            total_bytes=0
            count=0
            for d in os.scandir(f"{conf.FILEQ_DIR}/{conf.FILEQ_NAME}"):
                total_bytes+=d.stat().st_size
                count+=1
            metric_log={
                "timestamp": datetime.now().isoformat(),
                "type": "metric_detail",
                "level": "info",
                "data":{
                    "fqueue_dir_size": format_bytes(total_bytes),
                    "fqueue_dir_size_in_bytes": total_bytes,
                    "fqueue_size": count*conf.MAX_FILEQ_ITEMSIZE,
                    "local_buffer_size": len(local_buffer),
                    "mqueue_size": queue.qsize(),
                    "visited_links_cache_size": len(visited_links_cache),
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

        try:
            if local_buffer or not file_queue.is_empty():
                logging.debug(f"{name} - Not waiting for dequeuing!")
                item = queue.get_nowait()
            else:
                item = await queue.get()
        except asyncio.QueueEmpty:
            logging.debug(f"{name} - Queue Empty")
            item = None

        try:
            if item is not None:
                logging.debug(f"{name} - processing {item}")
                local_buffer.extend(await task(item, f"{name}>Consumer"))
        finally:
            if item is not None:
                queue.task_done()
        
        if not local_buffer:
            if queue.empty():
                if not file_queue.is_empty():
                    local_buffer=file_queue.dequeue_bulk()

        while local_buffer:
            try:
                task_to_add = local_buffer[0]
                logging.debug(f"{name} - Not waiting for enqueing!")
                queue.put_nowait(task_to_add)
                local_buffer.pop(0) # Remove only if successfully added
            except asyncio.QueueFull:
                logging.debug(f"{name} - Queue Full")
                break
        
        while len(local_buffer) > conf.MAX_WORKERS_BUFFERSIZE:
            logging.warning({
                "timestamp": datetime.now().isoformat(),
                "type": "max_threshold_crossed",
                "level": "warning",
                "data":{
                    "local_buffer_size": len(local_buffer),
                    "max_local_buffer_size": conf.MAX_WORKERS_BUFFERSIZE,
                    "worker": name
                },
                "message": f"{name} - local_buffer max threshold - {conf.MAX_WORKERS_BUFFERSIZE} crossed to {len(local_buffer)}"
                })
            logging.warning(f"{name} enqueuing {len(local_buffer[0:conf.MAX_FILEQ_ITEMSIZE])} links from local_buffer to file_queue")
            file_queue.enqueue_bulk(local_buffer[0:conf.MAX_FILEQ_ITEMSIZE])
            local_buffer=local_buffer[conf.MAX_FILEQ_ITEMSIZE:]

async def consumer(url, name="Consumer", default_path=None):
    global visited_links_cache, skipped_visited_count, skipped_other_content_type
    filtered_links=[]
    if url in visited_links_cache:
        logging.debug(f"{name} - Skipping, Already visited link: {url}")
        skipped_visited_count+=1
        return []
    logging.debug(f"{name} - Scraping URL: {url}")
    logging.debug(f"{name} - visited links size: {len(visited_links_cache)}")
    logging.debug(f"{name} - skipped_visited_count: {skipped_visited_count}")
    visited_links_cache.add(url)
    html_doc=await fetch(url, outfilepath=conf.RESPONSE_OUTPATH)
    await asyncio.sleep(2)
    # logging.debug(html_doc)
    links=[]
    if html_doc:
        ws=HTMLDocumentParser(url, html_doc)
        links=ws.get_all_links(default_path=default_path)
        logging.debug(f"{name} - Total links found: {len(links)}")
        if links:
            for link in links:
                if link in visited_links_cache:
                    skipped_visited_count+=1
                    logging.debug(f"{name} - Skipping, Already visited link: {link}")
                    continue
                filtered_links.append(f"{link}")
    else:
        skipped_other_content_type+=1
    return filtered_links

local_buffer = []
visited_links_cache=set()
skipped_visited_count=0
skipped_other_content_type=0
async def main(url):
    queue = asyncio.Queue(maxsize=conf.MAX_MAINQ_SIZE)
    file_queue=FileQueue(queue_dir=conf.FILEQ_DIR, queue_name=conf.FILEQ_NAME)
    if file_queue.is_empty():
        logging.info(f"Seeding with - {url}")
        queue.put_nowait(f"{url}") # Start seed
    workers = [asyncio.create_task(worker(f"Worker-{i}", queue, file_queue, consumer)) for i in range(5)]
    await queue.join()
    [await worker for worker in workers]

if __name__ == "__main__":
    url="https://www.w3schools.com/"
    asyncio.run(main(url=url))