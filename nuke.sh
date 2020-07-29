psql -U postgres -h localhost -c 'DROP DATABASE web_warehouse';
psql -U postgres -h localhost -c 'CREATE DATABASE web_warehouse';
psql -U postgres -h localhost web_warehouse < create_db.sql;
