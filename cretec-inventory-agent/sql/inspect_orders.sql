-- Run this first. If simulator column names differ, adjust engine.parse_cdc().
SHOW CREATE TABLE orderdb.orders;
SELECT * FROM orderdb.orders ORDER BY 1 DESC LIMIT 5;
