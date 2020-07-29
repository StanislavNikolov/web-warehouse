import psycopg2
import requests
import datetime
import time
import re
from bs4 import BeautifulSoup
from urlextract import URLExtract
import urllib.parse

db_conn = psycopg2.connect(dbname = 'web_warehouse', host = 'localhost', user = 'postgres')

def get_unparsed_page():
	cur = db_conn.cursor()
	cur.execute("""
		SELECT pages.id, response_text, url
		FROM pages
		JOIN urls ON pages.url_id = urls.id
		WHERE pages.id NOT IN (SELECT page_id FROM urlextractor_page_url)
		LIMIT 1
	""")
	return cur.fetchone()

def is_absolute(url):
	return bool()

def find_urls(page, page_url):
	# urls intermingled in html can get escaping artifacts
	# thats why we put the text through beautifulsoup
	soup = BeautifulSoup(page, 'lxml')
	soup_urls = []
	for a_el in soup.find_all('a'):
		href = a_el.get('href')

		if urllib.parse.urlparse(href).netloc == '': # relative url
			final_url = urllib.parse.urljoin(page_url, href)
		else: # absolute
			final_url = href

		soup_urls.append(final_url)

	# for nonhtml pages we need this instead
	text_urls = URLExtract().find_urls(page)

	# + [page_url] added so that find_urls returns at least 1 url
	return list(set(soup_urls + text_urls + [page_url]))

# requeires len(urls) >= 1
def insert(db_page_id, urls):
	cur = db_conn.cursor()
	cur.execute("BEGIN")

	args = [cur.mogrify("(%s)", (url,)).decode('utf-8') for url in urls]
	args_str = ','.join(args)
	cur.execute("INSERT INTO urls (url) VALUES " + args_str + " ON CONFLICT DO NOTHING")

	cur.execute("""
		INSERT INTO urlextractor_page_url (page_id, url_id)
		SELECT %s, id FROM urls WHERE url = ANY(%s)
	""", (db_page_id, urls))

	cur.execute("COMMIT")


while True:
	db_page_id, page, url = get_unparsed_page()
	urls = find_urls(page, url)

	print(f'Got {db_page_id} {url} -> {len(urls)} urls')

	insert(db_page_id, urls)

