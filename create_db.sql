CREATE TABLE urls (
	id INT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
	url TEXT
);
CREATE UNIQUE INDEX unique_url_index on urls (MD5(url));

CREATE TABLE pages (
	id INT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
	url_id INTEGER,
	response_text TEXT,
	response_code INTEGER,
	request_start_time TIMESTAMP,
	request_end_time TIMESTAMP,
	collector_ip TEXT,
	collector_headers TEXT,
	CONSTRAINT fk_url FOREIGN KEY (url_id) REFERENCES urls(id)
);

-- urlextractor marks that page_id contains url_id
CREATE TABLE urlextractor_page_url (
	id INT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
	page_id INTEGER,
	url_id INTEGER,
	CONSTRAINT fk_page FOREIGN KEY (page_id) REFERENCES pages(id),
	CONSTRAINT fk_url  FOREIGN KEY (url_id) REFERENCES urls(id),
	UNIQUE(page_id, url_id)
);

-- sample data
INSERT INTO urls (url) VALUES ('https://www.gwern.net/')
