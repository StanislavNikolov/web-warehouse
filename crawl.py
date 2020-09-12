import psycopg2
import requests
import datetime
import time
from urllib.parse import urlparse
import random

db_conn = psycopg2.connect(dbname='web_warehouse', host='localhost', user='postgres')


def get_random_url_generator():
    while True:
        cur = db_conn.cursor()

        cur.execute("""
            SELECT id, url
            FROM urls
            OFFSET floor(random() * (SELECT COUNT(*) FROM urls))
            LIMIT 10
        """)

        for db_url_id, url in cur:
            yield (db_url_id, url)


def fetch(db_url_id, url):
    if urlparse(url).scheme == '':
        url = "http://" + url

    request_start_time = datetime.datetime.now()
    req = requests.get(url, allow_redirects=True)
    request_end_time = datetime.datetime.now()

    cur = db_conn.cursor()
    cur.execute("""
        INSERT INTO pages (url_id, request_start_time, request_end_time, response_code, response_text)
        VALUES (%s, %s, %s, %s, %s)
    """, (db_url_id, request_start_time, request_end_time, req.status_code, req.text))
    db_conn.commit()


random_url_generator = get_random_url_generator()
while True:
    db_url_id, url = next(random_url_generator)
    print(f'Sampled {url}', flush=True)
    try:
        fetch(db_url_id, url)
    except Exception as error:
        print(f'Error {error}', flush=True)
