from parser_v4 import Parser
from write_db import add_info_sitemap


def main():
    urls = [
        'https://vk.com',
        'http://crawler-test.com/',
        'http://google.com/',
        'https://yandex.ru',
        'https://stackoverflow.com'
    ]
    for url in urls:
        task = Parser(url)
        path, count, time = task.run_parser()
        add_info_sitemap(
            url=url,
            time=time,
            count=count,
            path_to_file=path)
    print("\n Скрипт закончил работу")


if __name__ == '__main__':
    main()