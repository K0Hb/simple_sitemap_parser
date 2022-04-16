from email import iterators
import multiprocessing
import aiohttp
import asyncio
from datetime import datetime
from urllib.parse import urlparse, urljoin
from bs4 import BeautifulSoup
from typing import List
from multiprocessing import Manager


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
    file_name_xml = 'buffer.txt'

    manager = Manager()

    buffer_urls = manager.list()

    def __init__(self, base_url: str) -> None:
        self.base_url = base_url
        self.file_name_xml = f'sitemap_{urlparse(base_url).netloc}.xml'

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
        self.write_xml(info)


    def write_xml(self, text: str) -> None:
        file_name = self.file_name_xml
        with open(file_name, mode='a') as f:
            f.write(text)


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
                    # Общее множество URL-ов
                    self.all_urls.add(url)
                    # Счетчик обработанных страниц
                    self.all_count += 1
                    self.create_page_info(url, lastmod)
                    self.buffer_urls_and_html.append((url, respons))
            return respons


    async def create_loop_and_session(self) -> None:
        "Формируем событий цикл с задачами"
        timeout = aiohttp.ClientTimeout(total=10, sock_connect=25)
        async with aiohttp.ClientSession(
            requote_redirect_url= False,
            timeout=timeout, 
            connector = aiohttp.TCPConnector(verify_ssl=False),
            ) as session:
            tasks = []
            for iter_urls in self.buffer_urls:
                for url in iter_urls:
                    task_request = asyncio.create_task(self.get_html(session, url))
                    tasks.append(task_request)
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
        if len(urls) == 0:
            # страница без ссылок
            return []
        print(f"На {url}, найдено {count} внутренних ссылки.")
        # В ТЗ просили использовать итераторы/генераторы
        self.buffer_urls.append(urls)
        return urls


    def create_files(self) -> None:
        "Создаем xml файл"
        start = '<?xml version="1.0" encoding="UTF-8"?>\n<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">'
        with open(self.file_name_xml, "w") as file:
            file.write(start)

    def run_loop_requests(self) -> None:
        "Функция для запуска асинхронного событийного цикла "
        asyncio.run(self.create_loop_and_session())


    def run_multiprocess_parser(self) -> None:
        manager = Manager()
        self.buffer_urls = manager.list()
        with multiprocessing.Pool(multiprocessing.cpu_count()) as p:
            p.map(self.parse_links, self.buffer_urls_and_html)
            p.close()
            p.join()


    def new(self) -> None:
        start = datetime.now()
        self.create_files()
        self.buffer_urls.append((self.base_url,))
        while self.all_count < self.max_page:
            self.run_loop_requests()
            self.run_multiprocess_parser()
        self.write_xml('</urlset>')
        self.final_time = datetime.now() - start
        print(f"""
        Sitemap сформирован.\n
        Файл: {self.file_name_xml}.\n
        Кол-во страниц: {self.all_count}.\n
        Время работы: {self.final_time}
        """)
        return self.file_name_xml, self.all_count, 

lol = Parser('http://crawler-test.com/')
lol.new()