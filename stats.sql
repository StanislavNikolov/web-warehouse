--SELECT response_code, COUNT(id) FROM pages GROUP BY response_code;
--SELECT COUNT(id) FROM urls WHERE id NOT IN (SELECT url_id from urlextractor_page_url);


-- pages that don't have their urls extracted
SELECT COUNT(id)
FROM pages
WHERE NOT EXISTS (
	SELECT page_id
	FROM urlextractor_page_url
	WHERE page_id = pages.id
)


--- urls that have multiple pages
SELECT COUNT(*) AS cnt, url_id FROM pages
GROUP BY url_id
HAVING COUNT(*) > 1
ORDER BY cnt DESC;


--- urls without pages
select id, url from urls
where id not in (select url_id from pages group by url_id)
order by id;

---- slower???
-- SELECT id, url FROM urls
-- where NOT EXISTS (SELECT url_id FROM pages WHERE url_id = urls.id)
-- ORDER BY id;
-- or simply count
select COUNT(*) from urls
where not exists (select url_id from pages where url_id = urls.id);


-- most pointed at urls
SELECT COUNT(*), url_id, url
FROM urlextractor_page_url
JOIN urls ON urls.id = url_id
--WHERE url NOT LIKE '%wikipedia.org%' AND url NOT LIKE '%wikimedia.org%'
GROUP BY url_id, url
ORDER BY count desc;

