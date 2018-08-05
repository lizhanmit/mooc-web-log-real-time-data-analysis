#coding=UTF-8

import random
import time

url_paths = [
    "class/112.html",
    "class/128.html",
    "class/145.html",
    "class/146.html",
    "class/131.html",
    "class/130.html",
    "learn/821",
    "course/list"
]

ip_slices = [132,156,124,10,29,167,143,187,30,46,55,63,72,87,98,168]

http_referers = [
    "http://www.baidu.com/s?wd={query}",
    "https://www.sogou.com/web?query={query}",
    "http://cn.bing.com/search?q={query}",
    "https://search.yahoo.com/search?p={query}"
]

search_keyword = [
    "Spark SQL Tutorial",
    "Hadoop Basics",
    "Storm Tutorial",
    "Spark Streaming Tutorial",
    "Big Data Interview Skills",
    "HBase Tutorial"
]

status_codes = ["200", "404", "500"]

def sample_url():
    return random.sample(url_paths, 1)[0]


def sample_ip():
    slice = random.sample(ip_slices, 4)
    return ".".join([str(item) for item in slice])


def sample_referer():
    if random.uniform(0, 1) > 0.2:
        return "-"

    refer_str = random.sample(http_referers, 1)[0]
    query_str = random.sample(search_keyword, 1)[0]
    return refer_str.format(query=query_str)


def sample_status_code():
    return random.sample(status_codes, 1)[0]


def generate_log(count = 10): # default value is 10
    time_str = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())

    f = open("/home/hadoop/IdeaProjects/sparktrain/src/main/resources/logGenerator/log/access.log", "w+")

    while count >= 1:
        query_log = "{ip}\t{local_time}\t\"GET /{url} HTTP/1.1\"\t{status_code}\t{referer}".format(local_time=time_str, url=sample_url(), ip=sample_ip(), referer=sample_referer(), status_code=sample_status_code())
        print(query_log)
        f.write(query_log + "\n") # write query_log into files
        count -= 1


if __name__ == '__main__':
    generate_log(100)
