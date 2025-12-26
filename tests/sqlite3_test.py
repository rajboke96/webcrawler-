import sqlite3
import logging
import os


service_name="sqlite3_test"

# home
app_dir=os.environ["APP_DIR"]

# Data
data_dir=f"{app_dir}/data"

# logs
log_dir=f"{app_dir}/log"

level=logging.DEBUG
log_filename=f"{log_dir}/{service_name}.log"
log_filemode="w"
logging.basicConfig(level=level, filename=log_filename, filemode=log_filemode)

db_name="test_sqlite3.db"
db_file_path=f"{data_dir}/{db_name}"

logging.info(f"Connecting to DB: {db_file_path}")
db_conn=sqlite3.connect(db_file_path)

cursor=db_conn.cursor()

query="SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%';"
result_cursor=cursor.execute(query)
tables=[]
for d in result_cursor:
    tables.append(d)
logging.debug(f"Tables:{tables}")
if __name__ == '__main__':
    pass

