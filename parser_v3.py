from email import iterators
import multiprocessing
import aiohttp
import asyncio
import aiofiles
from datetime import datetime
from urllib.parse import urlparse, urljoin
from bs4 import BeautifulSoup
from typing import List
from multiprocessing import Pool


class Parser():

    max_page = 5000

    # Множество всех внутренних ссылок
    all_urls = set()
            
    # Количество посещенных URL-адресов
    all_count = 0

    # Структура для обмена URL/HTML между ассинхронными функциями 
    buffer_urls_and_html = []

    # Структура для обмена <url>page_info</url> между ассинхронными функциями
    buffer_page_info = []

    # Имя xml файла 
    file_name_xml = ''

    def __init__(self, base_url: str) -> None:
        self.base_url = base_url

    # Проверяем URL
    def valid_url(self, url: str) -> bool:
        "Проверяем валидность URL"
        if len(url) > 100:
            return False
        parsed = urlparse(url)
        return bool(parsed.netloc) and bool(parsed.scheme)

    def create_page_info(self, url: str, lastmode: str) ->None:
        "Формируем информацию о странице, для записи в xml файл"
        info = f'''
        <url>
        <loc>{url}</loc>
        <lastmod>{lastmode}</lastmod>
        </url>
        '''
        self.buffer_page_info.append(info)

    async def write_xml(self) -> None:
        "Асинхронно пишем в xml файл"
        try:
            info = self.buffer_page_info.pop()
        except IndexError:
            pass
        else:
            async with aiofiles.open(self.file_name_xml, mode='a') as f:
                await f.write(info)


    async def get_html(self, session, url: str) -> str:
            "Асинхронно деалем запрос по URL"
            try:
                async with session.get(url, ssl=False, timeout=10) as resp:
                    status = resp.status
                    respons = await resp.text()
            except Exception as error:
                print(f'\nОщибка: {error}, URL: {url}.\n')
                return None
            else:
                lastmod = resp.headers.get('Date', None)
                if 'last-modified' in resp.headers:
                    lastmod = resp.headers.get('Last-Modified')
                print(f'Статус: {status}, URL: {url}, lastmode {lastmod}')
                if 200 >= status < 400: 
                    self.create_page_info(url, lastmod)
                    self.buffer_urls_and_html.append((url, respons))
            return respons


    async def create_loop_and_session(self, url_list: iterators):
        "Формируем событий цикл с задачами"
        timeout = aiohttp.ClientTimeout(total=10, sock_connect=25)
        async with aiohttp.ClientSession(
            requote_redirect_url= False,
            timeout=timeout, 
            connector = aiohttp.TCPConnector(verify_ssl=False),
            ) as session:
            tasks = []
            for url in url_list:
                task_request = asyncio.create_task(self.get_html(session, url))
                task_write_xml = asyncio.create_task(self.write_xml())
                tasks.append(task_request)
                tasks.append(task_write_xml)
            await asyncio.gather(*tasks)

        
    def parse_links(self, url_html: List[tuple]) -> iterators:
        "Парсим HTML страничку на наличиие внутренние ссылок"
        url, html = url_html
        urls = set()
        count = 0
        # извлекаем доменное имя из URL
        domain_name = urlparse(url).netloc
        if html is None:
            return None
        soup = BeautifulSoup(html, "html.parser")
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
            count += 1
            urls.add(href)
            self.all_urls.add(href)
        if len(urls) == 0:
            # страница без ссылок
            return []
        self.all_count += count
        print(f"\nПарсер отработал страницу {url}, найдено {count} внутренних ссылок.\n")
        # В ТЗ просили использовать итераторы/генераторы
        return iter(urls)

    def create_name_file(self) -> None:
        "Формируем имя xml файла"
        now = datetime.now() 
        current_time = now.strftime("%H:%M:%S")
        self.file_name_xml = f'sitemap_{urlparse(self.base_url).netloc}_{current_time}.xml'


    def create_file_xml(self) -> None:
        "Создаем xml файл"
        self.create_name_file()
        start = '<?xml version="1.0" encoding="UTF-8"?>\n<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">'
        with open(self.file_name_xml, "w") as file:
            file.write(start)

    def run_loop(self, queue_urls: iterators) -> None:
        "Функция для запуска асинхронного событийного цикла "
        asyncio.run(self.create_loop_and_session(queue_urls))

    def run_multiprocess_parser(self):
        with multiprocessing.Pool(multiprocessing.cpu_count()) as p:
            p.map(self.parse_links, self.buffer_urls_and_html)
            self.buffer_urls_and_html.clear()

    def run_deep_crawl(self) -> None:
        self.create_file_xml()
        # В ТЗ просили использовать итераторы/генераторы 
        iter_urls = iter([self.base_url,])
        while True:
            self.run_loop(iter_urls)
            if self.all_count <= self.max_page:
                if len(self.buffer_urls_and_html) > 0:
                    # self.run_multiprocess_parser()
                    html = self.buffer_urls_and_html.pop()
                    iter_urls = self.parse_links(html)
            else:
                with open(self.file_name_xml, "a") as file:
                    file.write('</urlset>')
                print(
                    f"\nСкрипт завершил работу. Найдено: {self.all_count} страниц.",
                    f"Sitemap записана в файл: {self.file_name_xml} ."
                    )
                return self.file_name_xml, self.all_count

lol = Parser('http://crawler-test.com/')
lol.run_deep_crawl()