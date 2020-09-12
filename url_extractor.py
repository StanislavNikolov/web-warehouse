import psycopg2
import requests
import datetime
import time
import re
from bs4 import BeautifulSoup
from urlextract import URLExtract
import urllib.parse

db_conn = psycopg2.connect(dbname='web_warehouse',
                           host='localhost',
                           user='postgres')

'''
def get_unparsed_page():
    cur = db_conn.cursor()

    # TODO performance: The offset calculation is slow, because getting precise count is slow
    # maybe estimate count and retry on errors?
    # ORDER BY random() is faster??
    cur.execute("""
        SELECT pages.id, response_text, url
        FROM pages
        JOIN urls ON pages.url_id = urls.id
        WHERE NOT EXISTS (SELECT page_id FROM urlextractor_page_url WHERE page_id = pages.id)
        OFFSET floor( random() * (
                SELECT COUNT(*)
                FROM pages
                WHERE NOT EXISTS (SELECT page_id FROM urlextractor_page_url WHERE page_id = pages.id)
        ))
        LIMIT 1
    """)
'''


def get_unparsed_page_generator():
    while True:
        cur = db_conn.cursor()

        cur.execute("""
            SELECT pages.id, response_text, url
            FROM pages
            JOIN urls ON pages.url_id = urls.id
            WHERE NOT EXISTS (SELECT page_id FROM urlextractor_page_url WHERE page_id = pages.id)
            ORDER BY random()
            LIMIT 50
        """)

        for page_id, resp_text, url in cur:
            yield (page_id, resp_text, url)


def find_urls(page, page_url):
    # urls intermingled in html can get escaping artifacts
    # thats why we put the text through beautifulsoup
    a = time.time()
    soup = BeautifulSoup(page, 'lxml')
    soup_urls = []
    b = time.time()
    for a_el in soup.find_all('a'):
        href = a_el.get('href')

        if urllib.parse.urlparse(href).netloc == '':  # relative url
            final_url = urllib.parse.urljoin(page_url, href)
        else:  # absolute
            final_url = href

        soup_urls.append(final_url)
    c = time.time()

    # for nonhtml pages we need this instead
    text_urls = URLExtract().find_urls(page)
    d = time.time()

    bs = c-a
    urlex = d-c
    print(f'{bs:.4f} {urlex:.4f}', flush=True)

    print(len(set(soup_urls)), len(set(text_urls)), len(set(soup_urls + text_urls)))

    # + [page_url] added so that find_urls returns at least 1 url
    return list(set(soup_urls + text_urls + [page_url]))


# requeires len(urls) >= 1
def insert(db_page_id, urls):
    cur = db_conn.cursor()
    cur.execute("BEGIN")

    args = [cur.mogrify("(%s)", (url,)).decode('utf-8') for url in urls]
    args_str = ','.join(args)

    cur.execute("INSERT INTO urls (url) VALUES " + args_str + " ON CONFLICT DO NOTHING")

    # There is a MD5 index, utilizing it for 1000x performance
    cur.execute("""
        INSERT INTO urlextractor_page_url (page_id, url_id)
        SELECT %s, id FROM urls WHERE MD5(url) IN (
            SELECT MD5(UNNEST(%s))
        )
    """, (db_page_id, urls))

    cur.execute("COMMIT")


page_getter = get_unparsed_page_generator()
while True:
    a = time.time()

    db_page_id, page, url = next(page_getter)
    b = time.time()

    urls = find_urls(page, url)
    c = time.time()

    # insert can crash, because multiple extractors can get the same page from the database
    try:
        insert(db_page_id, urls)
    except psycopg2.errors.UniqueViolation as error:
        print('Non-fatal error:', error)

    d = time.time()

    print(f'Got {db_page_id} -> {len(urls)} urls; GET:{b-a:.2f}s, EXT:{c-b:.2f}s INS:{d-c:.2f}s; {url}', flush=True)
