import time
import requests
from functools import wraps
def retry(max_attempts=15, delay=2):
    def decorator_retry(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            nonlocal max_attempts  # 声明为非局部变量以便在内部函数中修改
            attempts = 0
            while attempts < max_attempts:
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    attempts += 1
                    if attempts == max_attempts:
                        raise
                    time.sleep(delay)  # 等待一段时间后重试
        return wrapper
    return decorator_retry
@retry()
def requests_post(url,headers={},data={},cookies={},timeout=300):
    return requests.post(url,headers=headers,data=data,cookies=cookies,timeout=timeout)
@retry()
def requests_get(url,headers={},params={},cookies={}):
    return requests.get(url,headers=headers,params=params,cookies=cookies)