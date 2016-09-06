
shell echo "# join-impala.sql";

USE default;
DROP DATABASE IF EXISTS benchmark CASCADE;
CREATE DATABASE benchmark COMMENT 'part of H2O h2oai/db-benchmark';
USE benchmark;

shell echo "${var:SRC_X_DIR}";
shell echo "${var:SRC_Y_DIR}";

CREATE EXTERNAL TABLE src_x (KEY BIGINT, X2 BIGINT)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ","
STORED AS TEXTFILE
LOCATION '${var:SRC_X_DIR}'
TBLPROPERTIES('skip.header.line.count'='1') -- should be compatible with 2.6.0
;
CREATE EXTERNAL TABLE src_y (KEY BIGINT, Y2 BIGINT)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ","
STORED AS TEXTFILE
LOCATION '${var:SRC_Y_DIR}'
TBLPROPERTIES('skip.header.line.count'='1') -- should be compatible with 2.6.0
;

CREATE TABLE x STORED AS PARQUET AS SELECT CAST(KEY AS BIGINT) KEY, CAST(X2 AS BIGINT) X2 FROM src_x WHERE key IS NOT NULL -- skip header row and ensure valid data types, required till 2.6.0?
;
CREATE TABLE y STORED AS PARQUET AS SELECT CAST(KEY AS BIGINT) KEY, CAST(Y2 AS BIGINT) Y2 FROM src_y WHERE key IS NOT NULL;

CREATE TABLE x_count STORED AS PARQUET AS SELECT COUNT(*) in_rows FROM x;
CREATE TABLE y_count STORED AS PARQUET AS SELECT COUNT(*) in_rows FROM y;

shell echo "impala-out-test-body";

-- cache=FALSE
DROP STATS x;
DROP STATS y;
REFRESH x;
REFRESH y;
SELECT COUNT(*) FROM (SELECT STRAIGHT_JOIN x.key, x.x2, y.y2 FROM x INNER JOIN /* +SHUFFLE */ y ON x.key=y.key) t;
SELECT UNIX_TIMESTAMP() _timestamp, 'join' _task, CONCAT(REGEXP_EXTRACT('${var:SRC_X_DIR}','[^/]+$',0), '.csv', '-', REGEXP_EXTRACT('${var:SRC_Y_DIR}','[^/]+$',0), '.csv') _data, x_count.in_rows _in_rows, 'inner join' _question, '' _out_rows, 'impala' _solution, 'INNER JOIN' _fun, 1 _run, 'FALSE' _cache FROM x_count;

DROP STATS x;
DROP STATS y;
REFRESH x;
REFRESH y;
SELECT COUNT(*) FROM (SELECT STRAIGHT_JOIN x.key, x.x2, y.y2 FROM x INNER JOIN /* +SHUFFLE */ y ON x.key=y.key) t;
SELECT UNIX_TIMESTAMP() _timestamp, 'join' _task, CONCAT(REGEXP_EXTRACT('${var:SRC_X_DIR}','[^/]+$',0), '.csv', '-', REGEXP_EXTRACT('${var:SRC_Y_DIR}','[^/]+$',0), '.csv') _data, x_count.in_rows _in_rows, 'inner join' _question, '' _out_rows, 'impala' _solution, 'INNER JOIN' _fun, 2 _run, 'FALSE' _cache FROM x_count;

DROP STATS x;
DROP STATS y;
REFRESH x;
REFRESH y;
SELECT COUNT(*) FROM (SELECT STRAIGHT_JOIN x.key, x.x2, y.y2 FROM x INNER JOIN /* +SHUFFLE */ y ON x.key=y.key) t;
SELECT UNIX_TIMESTAMP() _timestamp, 'join' _task, CONCAT(REGEXP_EXTRACT('${var:SRC_X_DIR}','[^/]+$',0), '.csv', '-', REGEXP_EXTRACT('${var:SRC_Y_DIR}','[^/]+$',0), '.csv') _data, x_count.in_rows _in_rows, 'inner join' _question, '' _out_rows, 'impala' _solution, 'INNER JOIN' _fun, 3 _run, 'FALSE' _cache FROM x_count;

-- cache=TRUE
DROP STATS x;
DROP STATS y;
REFRESH x;
REFRESH y;
CREATE TABLE ans STORED AS PARQUET AS SELECT STRAIGHT_JOIN x.key, x.x2, y.y2 FROM x INNER JOIN /* +SHUFFLE */ y ON x.key=y.key;
SELECT COUNT(*) FROM ans;
SELECT concat_ws(';', CAST(SUM(x2) AS STRING), CAST(SUM(y2) AS STRING)) chk FROM ans;
SELECT UNIX_TIMESTAMP() _timestamp, 'join' _task, CONCAT(REGEXP_EXTRACT('${var:SRC_X_DIR}','[^/]+$',0), '.csv', '-', REGEXP_EXTRACT('${var:SRC_Y_DIR}','[^/]+$',0), '.csv') _data, x_count.in_rows _in_rows, 'inner join' _question, '' _out_rows, 'impala' _solution, 'INNER JOIN' _fun, 1 _run, 'TRUE' _cache FROM x_count;
DROP TABLE ans;

DROP STATS x;
DROP STATS y;
REFRESH x;
REFRESH y;
CREATE TABLE ans STORED AS PARQUET AS SELECT STRAIGHT_JOIN x.key, x.x2, y.y2 FROM x INNER JOIN /* +SHUFFLE */ y ON x.key=y.key;
SELECT COUNT(*) FROM ans;
SELECT concat_ws(';', CAST(SUM(x2) AS STRING), CAST(SUM(y2) AS STRING)) chk FROM ans;
SELECT UNIX_TIMESTAMP() _timestamp, 'join' _task, CONCAT(REGEXP_EXTRACT('${var:SRC_X_DIR}','[^/]+$',0), '.csv', '-', REGEXP_EXTRACT('${var:SRC_Y_DIR}','[^/]+$',0), '.csv') _data, x_count.in_rows _in_rows, 'inner join' _question, '' _out_rows, 'impala' _solution, 'INNER JOIN' _fun, 2 _run, 'TRUE' _cache FROM x_count;
DROP TABLE ans;

DROP STATS x;
DROP STATS y;
REFRESH x;
REFRESH y;
CREATE TABLE ans STORED AS PARQUET AS SELECT STRAIGHT_JOIN x.key, x.x2, y.y2 FROM x INNER JOIN /* +SHUFFLE */ y ON x.key=y.key;
SELECT COUNT(*) FROM ans;
SELECT concat_ws(';', CAST(SUM(x2) AS STRING), CAST(SUM(y2) AS STRING)) chk FROM ans;
SELECT UNIX_TIMESTAMP() _timestamp, 'join' _task, CONCAT(REGEXP_EXTRACT('${var:SRC_X_DIR}','[^/]+$',0), '.csv', '-', REGEXP_EXTRACT('${var:SRC_Y_DIR}','[^/]+$',0), '.csv') _data, x_count.in_rows _in_rows, 'inner join' _question, '' _out_rows, 'impala' _solution, 'INNER JOIN' _fun, 3 _run, 'TRUE' _cache FROM x_count;
DROP TABLE ans;

shell echo "impala-out-test-body";

USE default;
DROP DATABASE benchmark CASCADE;
