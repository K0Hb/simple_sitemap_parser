import os
import pymysql
from dotenv import load_dotenv

load_dotenv()
HOST = os.getenv('HOST')
USER_DB = os.getenv('USER_DB')
PASSWORD = os.getenv('PASSWORD')
DB_NAME = os.getenv('DB_NAME')
TABLE_CREATE = '''
        CREATE TABLE sitemap (
            URL varchar(100),
            time varchar(20),
            count INT,
            path_to_file varchar(100)
            );
    '''


connection = pymysql.connect(host=HOST,
                             user=USER_DB,
                             password=PASSWORD,
                             database=DB_NAME,
                             cursorclass=pymysql.cursors.DictCursor)


def add_info_sitemap(URL, time, count, path_to_file, connection=connection):
    '''
    Функция создает пользователся в БД
    '''
    with connection.cursor() as cursor:
        connection.ping()
        try:
            result = cursor.execute(
                """
                INSERT INTO sitemap 
                (URL, time, count, path_to_file) 
                VALUES (
                    %(URL)s,
                    %(time)s,
                    %(count)s,
                    %(path_to_file)s,
                    );
                """,
                {'URL': URL,
                'time': time,
                'count': count,
                'path_to_file': path_to_file })
        except Exception:
            return None
        connection.commit()
        connection.close()
        return result