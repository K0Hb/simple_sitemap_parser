import queue
from urllib import request
import requests
import aiohttp
import asyncio
from urllib.parse import urlparse, urljoin
from bs4 import BeautifulSoup


ALL_URLS = set()
        
# Количество посещенных URL-адресов
ALL_COUNT = 0

BUFFER = []

# Проверяем URL

def valid_url(url):
    parsed = urlparse(url)
    return bool(parsed.netloc) and bool(parsed.scheme)

# def get_html(url):
#     try:
#         html = requests.get(url, timeout=5)
#     except Exception:
#         return None
#     if html.status_code != 200:
#         return None
#     return html

async def get_html(session, url):
        async with session.get(url) as resp:
            try:
                assert resp.status == 200
            except Exception as e:
                print(f'Error \n {e}')
                return None
            respons = await resp.text()
            print(resp.status, url)
            BUFFER.append((url, respons))
            return respons


async def run_get_loop_html(url_list):
    async with aiohttp.ClientSession() as session:
        tasks = []
        for url in url_list:
            task = asyncio.create_task(get_html(session, url))
            tasks.append(task)
        await asyncio.gather(*tasks)

    
def parse_links(url_html):
    url, html = url_html
    urls = set()
    count = 0
    # извлекаем доменное имя из URL
    domain_name = urlparse(url).netloc
    # print(html.headers)
    if html is None:
        return None
    soup = BeautifulSoup(html, "html.parser")
    # for tag in soup.find_all('lastmod'):
    #     print(tag)
    for a_tag in soup.findAll("a"):
        href = a_tag.attrs.get("href")
        if href == "" or href is None:
            # href пустой тег
            continue
        # присоединить URL, если он относительный (не абсолютная ссылка)
        href = urljoin(url, href)
        parsed_href = urlparse(href)
        # удалить параметры URL GET, фрагменты URL и т. д.
        href = parsed_href.scheme + "://" + parsed_href.netloc + parsed_href.path
        # print(href)
        if not valid_url(href):
            # недействительный URL
            continue
        if href in urls:
            # уже в локальном наборе
            continue
        if href in ALL_URLS:
            # уже в главном наборе
            continue
        if domain_name not in href:
            # внешняя ссылка
            continue
        # print(f"[*] Internal link: {href}")
        count += 1
        urls.add(href)
        ALL_URLS.add(href)
    if len(urls) == 0:
        # страница без ссылок
        return None
    # ALL_COUNT += count
    return iter(urls)

def lol(queue_urls):
    asyncio.run(run_get_loop_html(queue_urls))

def deep_crawl_website(base_url):
    queue_urls = []
    # for iter in queue_urls:
    #     if iter is not None:
    #         asyncio.run(run_get_loop_html(iter))
    asyncio.run(run_get_loop_html([base_url,]))
    while True:
        new_urls = parse_links(BUFFER.pop())
        if new_urls is not None:
            queue_urls.append(iter(new_urls))
        try:
            lol(queue_urls.pop())
            print(f"sum packegs {len(queue_urls)}")
        except IndexError:
            continue
            
        
deep_crawl_website('http://google.com/')
