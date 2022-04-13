import queue
from urllib import request
import requests
import aiohttp
import asyncio
from urllib.parse import urlparse, urljoin
from bs4 import BeautifulSoup


class Parser():
    all_urls = set()
            
    # Количество посещенных URL-адресов
    count_all_urls = 0

    def __init__(self, url) -> None:
        self.base_url = url

    # Проверяем URL
    @staticmethod
    def valid_url(url):
        parsed = urlparse(url)
        return bool(parsed.netloc) and bool(parsed.scheme)

    def get_html(self, url):
        try:
            html = requests.get(url, timeout=5)
        except Exception:
            return None
        if html.status_code != 200:
            return None
        return html
    
    def parse_links(self, url):
        urls = set()
        count = 0
        # извлекаем доменное имя из URL
        domain_name = urlparse(url).netloc
        # скачиваем HTML-контент вэб-страницы
        html = self.get_html(url)
        # print(html.headers)
        if html is None:
            return None
        soup = BeautifulSoup(html.content, "html.parser")
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
            if not self.valid_url(href):
                # недействительный URL
                continue
            if href in urls:
                # уже в локальном наборе
                continue
            if href in self.all_urls:
                # уже в главном наборе
                continue
            if domain_name not in href:
                # внешняя ссылка
                continue
            # print(f"[*] Internal link: {href}")
            count += 1
            urls.add(href)
            self.all_urls.add(href)
        if len(urls) == 0:
            # страница без ссылок
            return None
        self.count_all_urls += count
        return iter(urls)

    def deep_crawl_website(self):
        queue_urls = []
        queue_urls.append(self.parse_links(self.base_url))
        for iter in queue_urls:
            if iter is not None:
                for url in iter:
                    print(url)
                    queue_urls.append(self.parse_links(url))
            


class Parse_page(Parser):

    # Счетчик URL
    count_urls = 0

    # Все внутренние URL на странице
    urls = set()

    def __init__(self, url):
        self.url = url
    
    def parse_links(self):
        self.urls, self.count_urls = super().parse_links(self.url)
        return self.urls , self.count_urls
        

lol = Parser("http://crawler-test.com/")
lol.deep_crawl_website()
print(lol.count_all_urls)