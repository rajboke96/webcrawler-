import time
import logging

def decorate_with(symbol="-", repeat_count=50):
    def decorator(func):
        def wrapper(*args, **kwargs):
            # logging.debug(str(symbol)*repeat_count)
            res=func(*args, **kwargs)
            logging.debug(str(symbol)*repeat_count)
            return res
        return wrapper
    return decorator

def cal_time(func):
    def wrapper(*args, **kwargs):
        st_time=time.time()
        res = func(*args, **kwargs)
        logging.debug(f"Total execution time: {time.time()-st_time}")
        return res
    return wrapper
