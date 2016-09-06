
shell echo "# sort-impala.sql";

USE default;
DROP DATABASE IF EXISTS benchmark CASCADE;
CREATE DATABASE benchmark COMMENT 'part of H2O h2oai/db-benchmark';
USE benchmark;

shell echo "${var:SRC_X_DIR}";

CREATE EXTERNAL TABLE src_x (KEY BIGINT, X2 BIGINT)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ","
STORED AS TEXTFILE
LOCATION '${var:SRC_X_DIR}'
TBLPROPERTIES('skip.header.line.count'='1') -- should be compatible with 2.6.0
;

CREATE TABLE x STORED AS PARQUET AS SELECT CAST(KEY AS BIGINT) KEY, CAST(X2 AS BIGINT) X2 FROM src_x WHERE key IS NOT NULL -- skip header row and ensure valid data types, required till 2.6.0?
;

CREATE TABLE x_count STORED AS PARQUET AS SELECT COUNT(*) in_rows FROM x;

shell echo "impala-out-test-body";

/* ORDER BY not possible without printing all results in cli
-- cache=FALSE
DROP STATS x;
REFRESH x;
SELECT COUNT(*) FROM (SELECT x.key, x.x2 FROM x ORDER BY x.key ASC) t;
--Query: select COUNT(*) FROM (SELECT x.key, x.x2 FROM x ORDER BY x.key ASC) t
--WARNINGS: Ignoring ORDER BY clause without LIMIT or OFFSET: ORDER BY x.key ASC.
--An ORDER BY appearing in a view, subquery, union operand, or an insert/ctas statement has no effect on the query result unless a LIMIT and/or OFFSET is used in conjunction with the ORDER BY.
SELECT UNIX_TIMESTAMP() _timestamp, 'sort' _task, CONCAT(REGEXP_EXTRACT('${var:SRC_X_DIR}','[^/]+$',0), '.csv') _data, x_count.in_rows _in_rows, 'by int KEY' _question, '' _out_rows, 'impala' _solution, 'ORDER BY' _fun, 1 _run, 'FALSE' _cache FROM x_count;

-- cache=TRUE
DROP STATS x;
REFRESH x;
CREATE TABLE ans STORED AS PARQUET AS SELECT x.key, x.x2 FROM x ORDER BY x.key ASC;
--WARNINGS: Ignoring ORDER BY clause without LIMIT or OFFSET: ORDER BY x.key ASC.
--An ORDER BY appearing in a view, subquery, union operand, or an insert/ctas statement has no effect on the query result unless a LIMIT and/or OFFSET is used in conjunction with the ORDER BY.
SELECT COUNT(*) FROM ans;
SELECT CAST(SUM(x2) AS STRING) chk FROM ans;
SELECT UNIX_TIMESTAMP() _timestamp, 'sort' _task, CONCAT(REGEXP_EXTRACT('${var:SRC_X_DIR}','[^/]+$',0), '.csv') _data, x_count.in_rows _in_rows, 'by int KEY' _question, '' _out_rows, 'impala' _solution, 'ORDER BY' _fun, 1 _run, 'TRUE' _cache FROM x_count;
DROP TABLE ans;
*/

-- Using ROW_NUMBER() OVER (ORDER BY ...) workaround

/* this one unfortunately doesnt materialize results, we must use cache=TRUE then
-- cache=FALSE
DROP STATS x;
REFRESH x;
SELECT COUNT(*) FROM (SELECT x.key, x.x2, ROW_NUMBER() OVER (ORDER BY x.key) row_num FROM x) t;
SELECT UNIX_TIMESTAMP() _timestamp, 'sort' _task, CONCAT(REGEXP_EXTRACT('${var:SRC_X_DIR}','[^/]+$',0), '.csv') _data, x_count.in_rows _in_rows, 'by int KEY' _question, '' _out_rows, 'impala' _solution, 'ROW_NUMBER ORDER BY' _fun, 1 _run, 'FALSE' _cache FROM x_count;

DROP STATS x;
REFRESH x;
SELECT COUNT(*) FROM (SELECT x.key, x.x2, ROW_NUMBER() OVER (ORDER BY x.key) row_num FROM x) t;
SELECT UNIX_TIMESTAMP() _timestamp, 'sort' _task, CONCAT(REGEXP_EXTRACT('${var:SRC_X_DIR}','[^/]+$',0), '.csv') _data, x_count.in_rows _in_rows, 'by int KEY' _question, '' _out_rows, 'impala' _solution, 'ROW_NUMBER ORDER BY' _fun, 2 _run, 'FALSE' _cache FROM x_count;

DROP STATS x;
REFRESH x;
SELECT COUNT(*) FROM (SELECT x.key, x.x2, ROW_NUMBER() OVER (ORDER BY x.key) row_num FROM x) t;
SELECT UNIX_TIMESTAMP() _timestamp, 'sort' _task, CONCAT(REGEXP_EXTRACT('${var:SRC_X_DIR}','[^/]+$',0), '.csv') _data, x_count.in_rows _in_rows, 'by int KEY' _question, '' _out_rows, 'impala' _solution, 'ROW_NUMBER ORDER BY' _fun, 3 _run, 'FALSE' _cache FROM x_count;
*/

-- cache=TRUE
DROP STATS x;
REFRESH x;
CREATE TABLE ans STORED AS PARQUET AS SELECT x.key, x.x2, ROW_NUMBER() OVER (ORDER BY x.key) row_num FROM x;
SELECT COUNT(*) FROM ans;
SELECT CAST(SUM(x2) AS STRING) chk FROM ans;
SELECT UNIX_TIMESTAMP() _timestamp, 'sort' _task, CONCAT(REGEXP_EXTRACT('${var:SRC_X_DIR}','[^/]+$',0), '.csv') _data, x_count.in_rows _in_rows, 'by int KEY' _question, '' _out_rows, 'impala' _solution, 'ROW_NUMBER ORDER BY' _fun, 1 _run, 'TRUE' _cache FROM x_count;
DROP TABLE ans;

DROP STATS x;
REFRESH x;
CREATE TABLE ans STORED AS PARQUET AS SELECT x.key, x.x2, ROW_NUMBER() OVER (ORDER BY x.key) row_num FROM x;
SELECT COUNT(*) FROM ans;
SELECT CAST(SUM(x2) AS STRING) chk FROM ans;
SELECT UNIX_TIMESTAMP() _timestamp, 'sort' _task, CONCAT(REGEXP_EXTRACT('${var:SRC_X_DIR}','[^/]+$',0), '.csv') _data, x_count.in_rows _in_rows, 'by int KEY' _question, '' _out_rows, 'impala' _solution, 'ROW_NUMBER ORDER BY' _fun, 2 _run, 'TRUE' _cache FROM x_count;
DROP TABLE ans;

DROP STATS x;
REFRESH x;
CREATE TABLE ans STORED AS PARQUET AS SELECT x.key, x.x2, ROW_NUMBER() OVER (ORDER BY x.key) row_num FROM x;
SELECT COUNT(*) FROM ans;
SELECT CAST(SUM(x2) AS STRING) chk FROM ans;
SELECT UNIX_TIMESTAMP() _timestamp, 'sort' _task, CONCAT(REGEXP_EXTRACT('${var:SRC_X_DIR}','[^/]+$',0), '.csv') _data, x_count.in_rows _in_rows, 'by int KEY' _question, '' _out_rows, 'impala' _solution, 'ROW_NUMBER ORDER BY' _fun, 3 _run, 'TRUE' _cache FROM x_count;
DROP TABLE ans;

shell echo "impala-out-test-body";

USE default;
DROP DATABASE benchmark CASCADE;
