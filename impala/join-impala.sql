
shell echo "# join-impala.sql";

USE default;
DROP DATABASE IF EXISTS benchmark CASCADE;
CREATE DATABASE benchmark COMMENT 'part of H2O h2oai/db-benchmark';
USE benchmark;

shell echo "${var:SRC_X_DIR}";
shell echo "${var:SRC_Y_DIR}";

CREATE EXTERNAL TABLE src_x (KEY INT, X2 INT)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ","
STORED AS TEXTFILE
LOCATION '${var:SRC_X_DIR}';
CREATE EXTERNAL TABLE src_y (KEY INT, Y2 INT)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ","
STORED AS TEXTFILE
LOCATION '${var:SRC_Y_DIR}';

CREATE TABLE x STORED AS PARQUET AS SELECT * FROM src_x WHERE key IS NOT NULL;
CREATE TABLE y STORED AS PARQUET AS SELECT * FROM src_y WHERE key IS NOT NULL;

DROP STATS x;
DROP STATS y;
REFRESH x;
REFRESH y;
SELECT COUNT(*) FROM (SELECT STRAIGHT_JOIN x.key, x.x2, y.y2 FROM x INNER JOIN /* +SHUFFLE */ y ON x.key=y.key) t;
SELECT UNIX_TIMESTAMP() _timestamp, 'join' _task, '' _data, x.in_rows in_rows, NULL out_rows, 'impala' solution, 'INNER JOIN' fun, 1 run, NULL time_sec, NULL mem_gb
FROM (SELECT COUNT(*) in_rows FROM x) x;

DROP STATS x;
DROP STATS y;
REFRESH x;
REFRESH y;
SELECT COUNT(*) FROM (SELECT STRAIGHT_JOIN x.key, x.x2, y.y2 FROM x INNER JOIN /* +SHUFFLE */ y ON x.key=y.key) t;
SELECT UNIX_TIMESTAMP() _timestamp, 'join' _task, '' _data, x.in_rows in_rows, NULL out_rows, 'impala' solution, 'INNER JOIN' fun, 2 run, NULL time_sec, NULL mem_gb
FROM (SELECT COUNT(*) in_rows FROM x) x;

DROP STATS x;
DROP STATS y;
REFRESH x;
REFRESH y;
SELECT COUNT(*) FROM (SELECT STRAIGHT_JOIN x.key, x.x2, y.y2 FROM x INNER JOIN /* +SHUFFLE */ y ON x.key=y.key) t;
SELECT UNIX_TIMESTAMP() _timestamp, 'join' _task, '' _data, x.in_rows in_rows, NULL out_rows, 'impala' solution, 'INNER JOIN' fun, 3 run, NULL time_sec, NULL mem_gb
FROM (SELECT COUNT(*) in_rows FROM x) x;

USE default;
DROP DATABASE benchmark CASCADE;
