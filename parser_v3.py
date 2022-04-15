import aiohttp
import asyncio
import aiofiles
from datetime import datetime
from urllib.parse import urlparse, urljoin
from bs4 import BeautifulSoup


class Parser():

    # Множество всех внутренних ссылок
    all_urls = set()
            
    # Количество посещенных URL-адресов
    all_count = 0

    # Структура для обмена URL/HTML между ассинхронными функциями 
    buffer_urls_and_html = []

    # Структура для обмена <url>page_info</url> между ассинхронными функциями
    buffer_page_info = []

    # Проверяем URL

    def valid_url(self, url: str) -> bool:
        parsed = urlparse(url)
        return bool(parsed.netloc) and bool(parsed.scheme)

    def create_page_info(self, url: str, lastmode: str) ->None:
        info = f'''
        <url>
        <loc>{url}</loc>
        <lastmod>{lastmode}</lastmod>
        </url>
        '''
        self.buffer_page_info.append(info)

    async def write_xml(self, filename):
        try:
            info = self.buffer_page_info.pop()
        except IndexError:
            pass
        else:
            async with aiofiles.open(filename, mode='a') as f:
                await f.write(info)

    async def get_html(self, session, url):
            async with session.get(url, ssl=False) as resp:
                try:
                    status = resp.status
                    assert status == 200
                except Exception:
                    print(f'Статус {status}, URL {url}')
                    return None
                respons = await resp.text()
                if 'last-modified' in resp.headers:
                    lastmod = resp.headers['Last-Modified']
                else:
                    lastmod = resp.headers['Date']
                print(f'Статус: {status}, URL: {url}, lastmode {lastmod}')
                self.create_page_info(url, lastmod)
                self.buffer_urls_and_html.append((url, respons))
                return respons


    async def create_loop_and_session(self, url_list, file):
        async with aiohttp.ClientSession(connector = aiohttp.TCPConnector(verify_ssl=False)) as session:
            tasks = []
            for url in url_list:
                task_request = asyncio.create_task(self.get_html(session, url))
                task_write_xml = asyncio.create_task(self.write_xml(file))
                tasks.append(task_request)
                tasks.append(task_write_xml)
            await asyncio.gather(*tasks)

        
    def parse_links(self, url_html):
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
            return []
        self.all_count += count
        print(f"\nПарсер отработал страницу {url}, найдено {count} внутренних ссылок.\n")
        return iter(urls)

    def create_file_xml(self, base_url):
        now = datetime.now() 
        current_time = now.strftime("%H:%M:%S")
        filename = f'sitemap_{urlparse(base_url).netloc}_{current_time}.xml'
        start = '<?xml version="1.0" encoding="UTF-8"?>\n<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">'
        with open(filename, "w") as file:
            file.write(start)
        return filename

    def run_loop(self, queue_urls, file):
        asyncio.run(self.create_loop_and_session(queue_urls, file))

    def deep_crawl_website(self, base_url):
        file = self.create_file_xml(base_url)
        iter_urls = [base_url,]
        while True:
            self.run_loop(iter_urls, file)
            if len(self.buffer_urls_and_html) > 0:
                html = self.buffer_urls_and_html.pop()
                iter_urls = self.parse_links(html)
            else:
                with open(file, "a") as file:
                    file.write('</urlset>')
                print(f"\nСкрипт завершил работу. Найдено {self.all_count} страниц")
                return 

lol = Parser()
lol.deep_crawl_website('https://ru.hexlet.io')