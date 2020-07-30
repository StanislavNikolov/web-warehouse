import psycopg2
import requests
import datetime
import time
from urllib.parse import urlparse
import random

db_conn = psycopg2.connect(dbname = 'web_warehouse', host = 'localhost', user = 'postgres')

def get_random_url_generator():
	while True:
		cur = db_conn.cursor()

		if random.random() > 0.1:
			cur.execute("""
				SELECT id, url
				FROM urls
				WHERE url NOT LIKE '%wikipedia.org%' AND url NOT LIKE '%wikimedia.org%'
				OFFSET floor(random() * (SELECT COUNT(*) FROM urls WHERE url NOT LIKE '%wikipedia.org%' AND url NOT LIKE '%wikimedia.org%'))
				LIMIT 10
			""")
		else:
			cur.execute("""
				SELECT urls.id, url
				FROM urls
				LEFT JOIN pages ON pages.url_id = urls.id
				WHERE url LIKE '%gwern.net%' AND request_end_time IS NULL
				ORDER BY random()
				LIMIT 1
			""")

		for db_url_id, url in cur:
			yield (db_url_id, url)

def fetch(db_url_id, url):
	if urlparse(url).scheme == '': url = "http://" + url

	request_start_time = datetime.datetime.now()
	req = requests.get(url, allow_redirects = True)
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
	print(f'Sampled {url}', flush = True)
	try:
		fetch(db_url_id, url)
	except Exception as error:
		print(f'Error {error}', flush = True)
