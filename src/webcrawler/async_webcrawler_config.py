from config import DATA_DIR
from config import LOG_DIR

SERVICE_NAME="async_webcrawler"

# decorator_with configs
SYMBOL, REPEAT_COUNT="-", 130

# response file path
# RESPONSE_OUTPATH="./out.html"
RESPONSE_OUTPATH=None

import logging
LOG_LEVEL=logging.INFO
LOG_FILENAME=f"{LOG_DIR}/{SERVICE_NAME}.log"
LOG_FILEMODE="w"

# fqueue(file queue) - if mainq is full and localbuffer is full then worker enqueues tasks in fileq.
FILEQ_NAME="fqueue"
FILEQ_DIR=f"{DATA_DIR}"
MAX_FILEQ_ITEMSIZE=1000

# mainq(Asyncio queue) - worker uses this queue for processing tasks.
MAX_MAINQ_SIZE=500

# worker_buffer(local worker array) - workerS keeps tasks in this buffer if mainq is full.
WORKERS_BUFFERSIZE=500
MAX_WORKERS_BUFFERSIZE=MAX_FILEQ_ITEMSIZE+WORKERS_BUFFERSIZE