
shell echo "# groupby-impala.sql";

USE default;
DROP DATABASE IF EXISTS benchmark CASCADE;
CREATE DATABASE benchmark COMMENT 'part of H2O h2oai/db-benchmark';
USE benchmark;

shell echo "${var:SRC_GRP_DIR}";

--CREATE EXTERNAL TABLE src_grp (id1 STRING, id2 STRING, id3 STRING, id4 INT, id5 INT, id6 INT, v1 INT, v2 INT, v3 FLOAT) # in 2.6.0?
CREATE EXTERNAL TABLE src_grp (id1 STRING, id2 STRING, id3 STRING, id4 STRING, id5 STRING, id6 STRING, v1 STRING, v2 STRING, v3 STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ","
STORED AS TEXTFILE
LOCATION '${var:SRC_GRP_DIR}'
TBLPROPERTIES('skip.header.line.count'='1') -- should be compatible with 2.6.0
;

CREATE TABLE x STORED AS PARQUET AS SELECT id1, id2, id3, CAST(id4 AS INT) id4, CAST(id5 AS INT) id5, CAST(id6 AS INT) id6, CAST(v1 AS INT) v1, CAST(v2 AS INT) v2, CAST(v3 AS FLOAT) v3 FROM src_grp WHERE id5!='id5' -- exclude column headers
;
CREATE TABLE x_count STORED AS PARQUET AS SELECT COUNT(*) in_rows FROM x;

shell echo "impala-out-test-body";

-- cache = FALSE

-- question = "sum v1 by id1" #1
DROP STATS x;
REFRESH x;
SELECT COUNT(*) FROM (SELECT SUM(v1) v1 FROM x GROUP BY id1) t;
SELECT UNIX_TIMESTAMP() _timestamp, 'groupby' _task, CONCAT(REGEXP_EXTRACT('${var:SRC_GRP_DIR}','[^/]+$',0), '.csv') _data, x_count.in_rows _in_rows, 'sum v1 by id1' _question, '' _out_rows, 'impala' _solution, 'SUM GROUP BY' _fun, 1 _run, 'FALSE' _cache FROM x_count;

DROP STATS x;
REFRESH x;
SELECT COUNT(*) FROM (SELECT SUM(v1) v1 FROM x GROUP BY id1) t;
SELECT UNIX_TIMESTAMP() _timestamp, 'groupby' _task, CONCAT(REGEXP_EXTRACT('${var:SRC_GRP_DIR}','[^/]+$',0), '.csv') _data, x_count.in_rows _in_rows, 'sum v1 by id1' _question, '' _out_rows, 'impala' _solution, 'SUM GROUP BY' _fun, 2 _run, 'FALSE' _cache FROM x_count;

DROP STATS x;
REFRESH x;
SELECT COUNT(*) FROM (SELECT SUM(v1) v1 FROM x GROUP BY id1) t;
SELECT UNIX_TIMESTAMP() _timestamp, 'groupby' _task, CONCAT(REGEXP_EXTRACT('${var:SRC_GRP_DIR}','[^/]+$',0), '.csv') _data, x_count.in_rows _in_rows, 'sum v1 by id1' _question, '' _out_rows, 'impala' _solution, 'SUM GROUP BY' _fun, 3 _run, 'FALSE' _cache FROM x_count;

-- question = "sum v1 by id1:id2" #2
DROP STATS x;
REFRESH x;
SELECT COUNT(*) FROM (SELECT SUM(v1) v1 FROM x GROUP BY id1, id2) t;
SELECT UNIX_TIMESTAMP() _timestamp, 'groupby' _task, CONCAT(REGEXP_EXTRACT('${var:SRC_GRP_DIR}','[^/]+$',0), '.csv') _data, x_count.in_rows _in_rows, 'sum v1 by id1:id2' _question, '' _out_rows, 'impala' _solution, 'SUM GROUP BY' _fun, 1 _run, 'FALSE' _cache FROM x_count;

DROP STATS x;
REFRESH x;
SELECT COUNT(*) FROM (SELECT SUM(v1) v1 FROM x GROUP BY id1, id2) t;
SELECT UNIX_TIMESTAMP() _timestamp, 'groupby' _task, CONCAT(REGEXP_EXTRACT('${var:SRC_GRP_DIR}','[^/]+$',0), '.csv') _data, x_count.in_rows _in_rows, 'sum v1 by id1:id2' _question, '' _out_rows, 'impala' _solution, 'SUM GROUP BY' _fun, 2 _run, 'FALSE' _cache FROM x_count;

DROP STATS x;
REFRESH x;
SELECT COUNT(*) FROM (SELECT SUM(v1) v1 FROM x GROUP BY id1, id2) t;
SELECT UNIX_TIMESTAMP() _timestamp, 'groupby' _task, CONCAT(REGEXP_EXTRACT('${var:SRC_GRP_DIR}','[^/]+$',0), '.csv') _data, x_count.in_rows _in_rows, 'sum v1 by id1:id2' _question, '' _out_rows, 'impala' _solution, 'SUM GROUP BY' _fun, 3 _run, 'FALSE' _cache FROM x_count;

-- question = "sum v1 mean v3 by id3" #3
DROP STATS x;
REFRESH x;
SELECT COUNT(*) FROM (SELECT SUM(v1) v1, AVG(v3) v3 FROM x GROUP BY id3) t;
SELECT UNIX_TIMESTAMP() _timestamp, 'groupby' _task, CONCAT(REGEXP_EXTRACT('${var:SRC_GRP_DIR}','[^/]+$',0), '.csv') _data, x_count.in_rows _in_rows, 'sum v1 mean v3 by id3' _question, '' _out_rows, 'impala' _solution, 'SUM AVG GROUP BY' _fun, 1 _run, 'FALSE' _cache FROM x_count;

DROP STATS x;
REFRESH x;
SELECT COUNT(*) FROM (SELECT SUM(v1) v1, AVG(v3) v3 FROM x GROUP BY id3) t;
SELECT UNIX_TIMESTAMP() _timestamp, 'groupby' _task, CONCAT(REGEXP_EXTRACT('${var:SRC_GRP_DIR}','[^/]+$',0), '.csv') _data, x_count.in_rows _in_rows, 'sum v1 mean v3 by id3' _question, '' _out_rows, 'impala' _solution, 'SUM AVG GROUP BY' _fun, 2 _run, 'FALSE' _cache FROM x_count;

DROP STATS x;
REFRESH x;
SELECT COUNT(*) FROM (SELECT SUM(v1) v1, AVG(v3) v3 FROM x GROUP BY id3) t;
SELECT UNIX_TIMESTAMP() _timestamp, 'groupby' _task, CONCAT(REGEXP_EXTRACT('${var:SRC_GRP_DIR}','[^/]+$',0), '.csv') _data, x_count.in_rows _in_rows, 'sum v1 mean v3 by id3' _question, '' _out_rows, 'impala' _solution, 'SUM AVG GROUP BY' _fun, 3 _run, 'FALSE' _cache FROM x_count;

-- question = "mean v1:v3 by id4" #4
DROP STATS x;
REFRESH x;
SELECT COUNT(*) FROM (SELECT AVG(v1) v1, AVG(v2) v2, AVG(v3) v3 FROM x GROUP BY id4) t;
SELECT UNIX_TIMESTAMP() _timestamp, 'groupby' _task, CONCAT(REGEXP_EXTRACT('${var:SRC_GRP_DIR}','[^/]+$',0), '.csv') _data, x_count.in_rows _in_rows, 'mean v1:v3 by id4' _question, '' _out_rows, 'impala' _solution, 'AVG GROUP BY' _fun, 1 _run, 'FALSE' _cache FROM x_count;

DROP STATS x;
REFRESH x;
SELECT COUNT(*) FROM (SELECT AVG(v1) v1, AVG(v2) v2, AVG(v3) v3 FROM x GROUP BY id4) t;
SELECT UNIX_TIMESTAMP() _timestamp, 'groupby' _task, CONCAT(REGEXP_EXTRACT('${var:SRC_GRP_DIR}','[^/]+$',0), '.csv') _data, x_count.in_rows _in_rows, 'mean v1:v3 by id4' _question, '' _out_rows, 'impala' _solution, 'AVG GROUP BY' _fun, 2 _run, 'FALSE' _cache FROM x_count;

DROP STATS x;
REFRESH x;
SELECT COUNT(*) FROM (SELECT AVG(v1) v1, AVG(v2) v2, AVG(v3) v3 FROM x GROUP BY id4) t;
SELECT UNIX_TIMESTAMP() _timestamp, 'groupby' _task, CONCAT(REGEXP_EXTRACT('${var:SRC_GRP_DIR}','[^/]+$',0), '.csv') _data, x_count.in_rows _in_rows, 'mean v1:v3 by id4' _question, '' _out_rows, 'impala' _solution, 'AVG GROUP BY' _fun, 3 _run, 'FALSE' _cache FROM x_count;

-- question = "sum v1:v3 by id6" #5
DROP STATS x;
REFRESH x;
SELECT COUNT(*) FROM (SELECT SUM(v1) v1, SUM(v2) v2, SUM(v3) v3 FROM x GROUP BY id6) t;
SELECT UNIX_TIMESTAMP() _timestamp, 'groupby' _task, CONCAT(REGEXP_EXTRACT('${var:SRC_GRP_DIR}','[^/]+$',0), '.csv') _data, x_count.in_rows _in_rows, 'sum v1:v3 by id6' _question, '' _out_rows, 'impala' _solution, 'SUM GROUP BY' _fun, 1 _run, 'FALSE' _cache FROM x_count;

DROP STATS x;
REFRESH x;
SELECT COUNT(*) FROM (SELECT SUM(v1) v1, SUM(v2) v2, SUM(v3) v3 FROM x GROUP BY id6) t;
SELECT UNIX_TIMESTAMP() _timestamp, 'groupby' _task, CONCAT(REGEXP_EXTRACT('${var:SRC_GRP_DIR}','[^/]+$',0), '.csv') _data, x_count.in_rows _in_rows, 'sum v1:v3 by id6' _question, '' _out_rows, 'impala' _solution, 'SUM GROUP BY' _fun, 2 _run, 'FALSE' _cache FROM x_count;

DROP STATS x;
REFRESH x;
SELECT COUNT(*) FROM (SELECT SUM(v1) v1, SUM(v2) v2, SUM(v3) v3 FROM x GROUP BY id6) t;
SELECT UNIX_TIMESTAMP() _timestamp, 'groupby' _task, CONCAT(REGEXP_EXTRACT('${var:SRC_GRP_DIR}','[^/]+$',0), '.csv') _data, x_count.in_rows _in_rows, 'sum v1:v3 by id6' _question, '' _out_rows, 'impala' _solution, 'SUM GROUP BY' _fun, 3 _run, 'FALSE' _cache FROM x_count;

-- cache = TRUE
-- making 'chk' to be simplified once https://issues.cloudera.org/browse/IMPALA-3436 solved, impala 3.0?

-- question = "sum v1 by id1" #1
DROP STATS x;
REFRESH x;
CREATE TABLE ans STORED AS PARQUET AS SELECT SUM(v1) v1 FROM x GROUP BY id1;
SELECT COUNT(*) FROM ans;
SELECT concat_ws(';', CAST(SUM(v1) AS STRING)) chk FROM ans;
SELECT UNIX_TIMESTAMP() _timestamp, 'groupby' _task, CONCAT(REGEXP_EXTRACT('${var:SRC_GRP_DIR}','[^/]+$',0), '.csv') _data, x_count.in_rows _in_rows, 'sum v1 by id1' _question, '' _out_rows, 'impala' _solution, 'SUM GROUP BY' _fun, 1 _run, 'TRUE' _cache FROM x_count;
DROP TABLE ans;

DROP STATS x;
REFRESH x;
CREATE TABLE ans STORED AS PARQUET AS SELECT SUM(v1) v1 FROM x GROUP BY id1;
SELECT COUNT(*) FROM ans;
SELECT concat_ws(';', CAST(SUM(v1) AS STRING)) chk FROM ans;
SELECT UNIX_TIMESTAMP() _timestamp, 'groupby' _task, CONCAT(REGEXP_EXTRACT('${var:SRC_GRP_DIR}','[^/]+$',0), '.csv') _data, x_count.in_rows _in_rows, 'sum v1 by id1' _question, '' _out_rows, 'impala' _solution, 'SUM GROUP BY' _fun, 2 _run, 'TRUE' _cache FROM x_count;
DROP TABLE ans;

DROP STATS x;
REFRESH x;
CREATE TABLE ans STORED AS PARQUET AS SELECT SUM(v1) v1 FROM x GROUP BY id1;
SELECT COUNT(*) FROM ans;
SELECT concat_ws(';', CAST(SUM(v1) AS STRING)) chk FROM ans;
SELECT UNIX_TIMESTAMP() _timestamp, 'groupby' _task, CONCAT(REGEXP_EXTRACT('${var:SRC_GRP_DIR}','[^/]+$',0), '.csv') _data, x_count.in_rows _in_rows, 'sum v1 by id1' _question, '' _out_rows, 'impala' _solution, 'SUM GROUP BY' _fun, 3 _run, 'TRUE' _cache FROM x_count;
DROP TABLE ans;

-- question = "sum v1 by id1:id2" #2
DROP STATS x;
REFRESH x;
CREATE TABLE ans STORED AS PARQUET AS SELECT SUM(v1) v1 FROM x GROUP BY id1, id2;
SELECT COUNT(*) FROM ans;
SELECT concat_ws(';', CAST(SUM(v1) AS STRING)) chk FROM ans;
SELECT UNIX_TIMESTAMP() _timestamp, 'groupby' _task, CONCAT(REGEXP_EXTRACT('${var:SRC_GRP_DIR}','[^/]+$',0), '.csv') _data, x_count.in_rows _in_rows, 'sum v1 by id1:id2' _question, '' _out_rows, 'impala' _solution, 'SUM GROUP BY' _fun, 1 _run, 'TRUE' _cache FROM x_count;
DROP TABLE ans;

DROP STATS x;
REFRESH x;
CREATE TABLE ans STORED AS PARQUET AS SELECT SUM(v1) v1 FROM x GROUP BY id1, id2;
SELECT COUNT(*) FROM ans;
SELECT concat_ws(';', CAST(SUM(v1) AS STRING)) chk FROM ans;
SELECT UNIX_TIMESTAMP() _timestamp, 'groupby' _task, CONCAT(REGEXP_EXTRACT('${var:SRC_GRP_DIR}','[^/]+$',0), '.csv') _data, x_count.in_rows _in_rows, 'sum v1 by id1:id2' _question, '' _out_rows, 'impala' _solution, 'SUM GROUP BY' _fun, 2 _run, 'TRUE' _cache FROM x_count;
DROP TABLE ans;

DROP STATS x;
REFRESH x;
CREATE TABLE ans STORED AS PARQUET AS SELECT SUM(v1) v1 FROM x GROUP BY id1, id2;
SELECT COUNT(*) FROM ans;
SELECT concat_ws(';', CAST(SUM(v1) AS STRING)) chk FROM ans;
SELECT UNIX_TIMESTAMP() _timestamp, 'groupby' _task, CONCAT(REGEXP_EXTRACT('${var:SRC_GRP_DIR}','[^/]+$',0), '.csv') _data, x_count.in_rows _in_rows, 'sum v1 by id1:id2' _question, '' _out_rows, 'impala' _solution, 'SUM GROUP BY' _fun, 3 _run, 'TRUE' _cache FROM x_count;
DROP TABLE ans;

-- question = "sum v1 mean v3 by id3" #3
DROP STATS x;
REFRESH x;
CREATE TABLE ans STORED AS PARQUET AS SELECT SUM(v1) v1, AVG(v3) v3 FROM x GROUP BY id3;
SELECT COUNT(*) FROM ans;
SELECT concat_ws(';', CAST(SUM(v1) AS STRING), CAST(CAST(SUM(v3) AS DECIMAL(38,3)) AS STRING)) chk FROM ans;
SELECT UNIX_TIMESTAMP() _timestamp, 'groupby' _task, CONCAT(REGEXP_EXTRACT('${var:SRC_GRP_DIR}','[^/]+$',0), '.csv') _data, x_count.in_rows _in_rows, 'sum v1 mean v3 by id3' _question, '' _out_rows, 'impala' _solution, 'SUM AVG GROUP BY' _fun, 1 _run, 'TRUE' _cache FROM x_count;
DROP TABLE ans;

DROP STATS x;
REFRESH x;
CREATE TABLE ans STORED AS PARQUET AS SELECT SUM(v1) v1, AVG(v3) v3 FROM x GROUP BY id3;
SELECT COUNT(*) FROM ans;
SELECT concat_ws(';', CAST(SUM(v1) AS STRING), CAST(CAST(SUM(v3) AS DECIMAL(38,3)) AS STRING)) chk FROM ans;
SELECT UNIX_TIMESTAMP() _timestamp, 'groupby' _task, CONCAT(REGEXP_EXTRACT('${var:SRC_GRP_DIR}','[^/]+$',0), '.csv') _data, x_count.in_rows _in_rows, 'sum v1 mean v3 by id3' _question, '' _out_rows, 'impala' _solution, 'SUM AVG GROUP BY' _fun, 2 _run, 'TRUE' _cache FROM x_count;
DROP TABLE ans;

DROP STATS x;
REFRESH x;
CREATE TABLE ans STORED AS PARQUET AS SELECT SUM(v1) v1, AVG(v3) v3 FROM x GROUP BY id3;
SELECT COUNT(*) FROM ans;
SELECT concat_ws(';', CAST(SUM(v1) AS STRING), CAST(CAST(SUM(v3) AS DECIMAL(38,3)) AS STRING)) chk FROM ans;
SELECT UNIX_TIMESTAMP() _timestamp, 'groupby' _task, CONCAT(REGEXP_EXTRACT('${var:SRC_GRP_DIR}','[^/]+$',0), '.csv') _data, x_count.in_rows _in_rows, 'sum v1 mean v3 by id3' _question, '' _out_rows, 'impala' _solution, 'SUM AVG GROUP BY' _fun, 3 _run, 'TRUE' _cache FROM x_count;
DROP TABLE ans;

-- question = "mean v1:v3 by id4" #4
DROP STATS x;
REFRESH x;
CREATE TABLE ans STORED AS PARQUET AS SELECT AVG(v1) v1, AVG(v2) v2, AVG(v3) v3 FROM x GROUP BY id4;
SELECT COUNT(*) FROM ans;
SELECT concat_ws(';', CAST(CAST(SUM(v1) AS DECIMAL(38,3)) AS STRING), CAST(CAST(SUM(v2) AS DECIMAL(38,3)) AS STRING), CAST(CAST(SUM(v3) AS DECIMAL(38,3)) AS STRING)) chk FROM ans;
SELECT UNIX_TIMESTAMP() _timestamp, 'groupby' _task, CONCAT(REGEXP_EXTRACT('${var:SRC_GRP_DIR}','[^/]+$',0), '.csv') _data, x_count.in_rows _in_rows, 'mean v1:v3 by id4' _question, '' _out_rows, 'impala' _solution, 'AVG GROUP BY' _fun, 1 _run, 'TRUE' _cache FROM x_count;
DROP TABLE ans;

DROP STATS x;
REFRESH x;
CREATE TABLE ans STORED AS PARQUET AS SELECT AVG(v1) v1, AVG(v2) v2, AVG(v3) v3 FROM x GROUP BY id4;
SELECT COUNT(*) FROM ans;
SELECT concat_ws(';', CAST(CAST(SUM(v1) AS DECIMAL(38,3)) AS STRING), CAST(CAST(SUM(v2) AS DECIMAL(38,3)) AS STRING), CAST(CAST(SUM(v3) AS DECIMAL(38,3)) AS STRING)) chk FROM ans;
SELECT UNIX_TIMESTAMP() _timestamp, 'groupby' _task, CONCAT(REGEXP_EXTRACT('${var:SRC_GRP_DIR}','[^/]+$',0), '.csv') _data, x_count.in_rows _in_rows, 'mean v1:v3 by id4' _question, '' _out_rows, 'impala' _solution, 'AVG GROUP BY' _fun, 2 _run, 'TRUE' _cache FROM x_count;
DROP TABLE ans;

DROP STATS x;
REFRESH x;
CREATE TABLE ans STORED AS PARQUET AS SELECT AVG(v1) v1, AVG(v2) v2, AVG(v3) v3 FROM x GROUP BY id4;
SELECT COUNT(*) FROM ans;
SELECT concat_ws(';', CAST(CAST(SUM(v1) AS DECIMAL(38,3)) AS STRING), CAST(CAST(SUM(v2) AS DECIMAL(38,3)) AS STRING), CAST(CAST(SUM(v3) AS DECIMAL(38,3)) AS STRING)) chk FROM ans;
SELECT UNIX_TIMESTAMP() _timestamp, 'groupby' _task, CONCAT(REGEXP_EXTRACT('${var:SRC_GRP_DIR}','[^/]+$',0), '.csv') _data, x_count.in_rows _in_rows, 'mean v1:v3 by id4' _question, '' _out_rows, 'impala' _solution, 'AVG GROUP BY' _fun, 3 _run, 'TRUE' _cache FROM x_count;
DROP TABLE ans;

-- question = "sum v1:v3 by id6" #5
DROP STATS x;
REFRESH x;
CREATE TABLE ans STORED AS PARQUET AS SELECT SUM(v1) v1, SUM(v2) v2, SUM(v3) v3 FROM x GROUP BY id6;
SELECT COUNT(*) FROM ans;
SELECT concat_ws(';', CAST(SUM(v1) AS STRING), CAST(SUM(v2) AS STRING), CAST(CAST(SUM(v3) AS DECIMAL(38,3)) AS STRING)) chk FROM ans;
SELECT UNIX_TIMESTAMP() _timestamp, 'groupby' _task, CONCAT(REGEXP_EXTRACT('${var:SRC_GRP_DIR}','[^/]+$',0), '.csv') _data, x_count.in_rows _in_rows, 'sum v1:v3 by id6' _question, '' _out_rows, 'impala' _solution, 'SUM GROUP BY' _fun, 1 _run, 'TRUE' _cache FROM x_count;
DROP TABLE ans;

DROP STATS x;
REFRESH x;
CREATE TABLE ans STORED AS PARQUET AS SELECT SUM(v1) v1, SUM(v2) v2, SUM(v3) v3 FROM x GROUP BY id6;
SELECT COUNT(*) FROM ans;
SELECT concat_ws(';', CAST(SUM(v1) AS STRING), CAST(SUM(v2) AS STRING), CAST(CAST(SUM(v3) AS DECIMAL(38,3)) AS STRING)) chk FROM ans;
SELECT UNIX_TIMESTAMP() _timestamp, 'groupby' _task, CONCAT(REGEXP_EXTRACT('${var:SRC_GRP_DIR}','[^/]+$',0), '.csv') _data, x_count.in_rows _in_rows, 'sum v1:v3 by id6' _question, '' _out_rows, 'impala' _solution, 'SUM GROUP BY' _fun, 2 _run, 'TRUE' _cache FROM x_count;
DROP TABLE ans;

DROP STATS x;
REFRESH x;
CREATE TABLE ans STORED AS PARQUET AS SELECT SUM(v1) v1, SUM(v2) v2, SUM(v3) v3 FROM x GROUP BY id6;
SELECT COUNT(*) FROM ans;
SELECT concat_ws(';', CAST(SUM(v1) AS STRING), CAST(SUM(v2) AS STRING), CAST(CAST(SUM(v3) AS DECIMAL(38,3)) AS STRING)) chk FROM ans;
SELECT UNIX_TIMESTAMP() _timestamp, 'groupby' _task, CONCAT(REGEXP_EXTRACT('${var:SRC_GRP_DIR}','[^/]+$',0), '.csv') _data, x_count.in_rows _in_rows, 'sum v1:v3 by id6' _question, '' _out_rows, 'impala' _solution, 'SUM GROUP BY' _fun, 3 _run, 'TRUE' _cache FROM x_count;
DROP TABLE ans;

shell echo "impala-out-test-body";

USE default;
DROP DATABASE benchmark CASCADE;
