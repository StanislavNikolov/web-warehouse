import psycopg2
import requests
import datetime
import time
from urllib.parse import urlparse

db_conn = psycopg2.connect(dbname = 'web_warehouse', host = 'localhost', user = 'postgres')

def get_random_url():
	cur = db_conn.cursor()
	cur.execute("""
		SELECT id, url
		FROM urls
		OFFSET floor(random() * (SELECT COUNT(*) FROM urls) )
		LIMIT 1
	""")
	return cur.fetchone()

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


while True:
	db_url_id, url = get_random_url()
	print(f'Sampled {url}')
	try:
		fetch(db_url_id, url)
	except Exception as error:
		print(f'Error {error}')

