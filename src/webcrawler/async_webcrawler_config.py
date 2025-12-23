from config import log_dir

import logging
level=logging.DEBUG
filename, filemode=f"{log_dir}/async_webcrawler.log", "w"

# decorator_with configs
symbol, repeat_count="-", 130

# response file path
# outfilepath="./out.html"
outfilepath=None