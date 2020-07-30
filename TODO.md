best:
```

SELECT COUNT(id)
FROM pages
WHERE NOT EXISTS (SELECT page_id from urlextractor_page_url WHERE page_id = pages.id)
```

Why does
```
EXPLAIN ANALYZE SELECT pages.id, response_text
FROM pages
WHERE pages.id NOT IN (SELECT page_id FROM urlextractor_page_url GROUP BY page_id)
LIMIT 1
```
take 42 ms, but
```
EXPLAIN ANALYZE SELECT pages.id, response_text
FROM pages
WHERE pages.id NOT IN (SELECT page_id FROM urlextractor_page_url)
LIMIT 1

```
takes 30 SECONDS???!

Why is GROUP BY better than distinct?
