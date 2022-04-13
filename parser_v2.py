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


async def get_html(session, url):
        async with session.get(url, ssl=False) as resp:
            try:
                status = resp.status
                assert status == 200
            except Exception:
                print(f'Статус {status}, URL {url}')
                return None
            respons = await resp.text()
            print(f'Статус {status}, URL {url}')
            BUFFER.append((url, respons))
            return respons


async def create_loop_and_session(url_list):
    async with aiohttp.ClientSession(connector = aiohttp.TCPConnector(verify_ssl=False)) as session:
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
        return []
    # ALL_COUNT += count
    print(f"Парсер отработал , найдено {len(urls)} внутренних ссылок.")
    return iter(urls)

def lol(queue_urls):
    asyncio.run(create_loop_and_session(queue_urls))

def deep_crawl_website(base_url):
    iter_urls = [base_url,]   
    while True:
        lol(iter_urls)
        if len(BUFFER) > 0:
            html = BUFFER.pop()
            iter_urls = parse_links(html)
        else:
            print("Exit")
            return 

deep_crawl_website('https://stackoverflow.com')
