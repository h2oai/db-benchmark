
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

CREATE TABLE x STORED AS PARQUET AS SELECT id1, id2, id3, CAST(id4 AS INT) id4, CAST(id5 AS INT) id5, CAST(id6 AS INT) id6, CAST(v1 AS INT) v1, CAST(v2 AS INT) v2, CAST(v3 AS FLOAT) v3 FROM src_grp WHERE id4!='id4';

shell echo "###impala-body";

-- question = "sum v1 by id1" #1
DROP STATS x;
REFRESH x;
SELECT COUNT(*) FROM (SELECT SUM(v1) v1 FROM x GROUP BY id1) t;
SELECT UNIX_TIMESTAMP() _timestamp, 'groupby' _task, CONCAT(REGEXP_EXTRACT('${var:SRC_GRP_DIR}','[^/]+$',0), '.csv') _data, x.in_rows _in_rows, 'sum v1 by id1' _question, 'impala' _solution, 'SUM GROUP BY' _fun, 1 _run FROM (SELECT COUNT(*) in_rows FROM x) x;

DROP STATS x;
REFRESH x;
SELECT COUNT(*) FROM (SELECT SUM(v1) v1 FROM x GROUP BY id1) t;
SELECT UNIX_TIMESTAMP() _timestamp, 'groupby' _task, CONCAT(REGEXP_EXTRACT('${var:SRC_GRP_DIR}','[^/]+$',0), '.csv') _data, x.in_rows _in_rows, 'sum v1 by id1' _question, 'impala' _solution, 'SUM GROUP BY' _fun, 2 _run FROM (SELECT COUNT(*) in_rows FROM x) x;

DROP STATS x;
REFRESH x;
SELECT COUNT(*) FROM (SELECT SUM(v1) v1 FROM x GROUP BY id1) t;
SELECT UNIX_TIMESTAMP() _timestamp, 'groupby' _task, CONCAT(REGEXP_EXTRACT('${var:SRC_GRP_DIR}','[^/]+$',0), '.csv') _data, x.in_rows _in_rows, 'sum v1 by id1' _question, 'impala' _solution, 'SUM GROUP BY' _fun, 3 _run FROM (SELECT COUNT(*) in_rows FROM x) x;

-- question = "sum v1 by id1:id2" #2
DROP STATS x;
REFRESH x;
SELECT COUNT(*) FROM (SELECT SUM(v1) v1 FROM x GROUP BY id1, id2) t;
SELECT UNIX_TIMESTAMP() _timestamp, 'groupby' _task, CONCAT(REGEXP_EXTRACT('${var:SRC_GRP_DIR}','[^/]+$',0), '.csv') _data, x.in_rows _in_rows, 'sum v1 by id1:id2' _question, 'impala' _solution, 'SUM GROUP BY' _fun, 1 _run FROM (SELECT COUNT(*) in_rows FROM x) x;

DROP STATS x;
REFRESH x;
SELECT COUNT(*) FROM (SELECT SUM(v1) v1 FROM x GROUP BY id1, id2) t;
SELECT UNIX_TIMESTAMP() _timestamp, 'groupby' _task, CONCAT(REGEXP_EXTRACT('${var:SRC_GRP_DIR}','[^/]+$',0), '.csv') _data, x.in_rows _in_rows, 'sum v1 by id1:id2' _question, 'impala' _solution, 'SUM GROUP BY' _fun, 2 _run FROM (SELECT COUNT(*) in_rows FROM x) x;

DROP STATS x;
REFRESH x;
SELECT COUNT(*) FROM (SELECT SUM(v1) v1 FROM x GROUP BY id1, id2) t;
SELECT UNIX_TIMESTAMP() _timestamp, 'groupby' _task, CONCAT(REGEXP_EXTRACT('${var:SRC_GRP_DIR}','[^/]+$',0), '.csv') _data, x.in_rows _in_rows, 'sum v1 by id1:id2' _question, 'impala' _solution, 'SUM GROUP BY' _fun, 3 _run FROM (SELECT COUNT(*) in_rows FROM x) x;

-- question = "sum v1 mean v3 by id3" #3
DROP STATS x;
REFRESH x;
SELECT COUNT(*) FROM (SELECT SUM(v1) v1, AVG(v3) v3 FROM x GROUP BY id3) t;
SELECT UNIX_TIMESTAMP() _timestamp, 'groupby' _task, CONCAT(REGEXP_EXTRACT('${var:SRC_GRP_DIR}','[^/]+$',0), '.csv') _data, x.in_rows _in_rows, 'sum v1 mean v3 by id3' _question, 'impala' _solution, 'SUM AVG GROUP BY' _fun, 1 _run FROM (SELECT COUNT(*) in_rows FROM x) x;

DROP STATS x;
REFRESH x;
SELECT COUNT(*) FROM (SELECT SUM(v1) v1, AVG(v3) v3 FROM x GROUP BY id3) t;
SELECT UNIX_TIMESTAMP() _timestamp, 'groupby' _task, CONCAT(REGEXP_EXTRACT('${var:SRC_GRP_DIR}','[^/]+$',0), '.csv') _data, x.in_rows _in_rows, 'sum v1 mean v3 by id3' _question, 'impala' _solution, 'SUM AVG GROUP BY' _fun, 2 _run FROM (SELECT COUNT(*) in_rows FROM x) x;

DROP STATS x;
REFRESH x;
SELECT COUNT(*) FROM (SELECT SUM(v1) v1, AVG(v3) v3 FROM x GROUP BY id3) t;
SELECT UNIX_TIMESTAMP() _timestamp, 'groupby' _task, CONCAT(REGEXP_EXTRACT('${var:SRC_GRP_DIR}','[^/]+$',0), '.csv') _data, x.in_rows _in_rows, 'sum v1 mean v3 by id3' _question, 'impala' _solution, 'SUM AVG GROUP BY' _fun, 3 _run FROM (SELECT COUNT(*) in_rows FROM x) x;

-- question = "mean v1:v3 by id4" #4
DROP STATS x;
REFRESH x;
SELECT COUNT(*) FROM (SELECT AVG(v1) v1, AVG(v2) v2, AVG(v3) v3 FROM x GROUP BY id4) t;
SELECT UNIX_TIMESTAMP() _timestamp, 'groupby' _task, CONCAT(REGEXP_EXTRACT('${var:SRC_GRP_DIR}','[^/]+$',0), '.csv') _data, x.in_rows _in_rows, 'mean v1:v3 by id4' _question, 'impala' _solution, 'AVG GROUP BY' _fun, 1 _run FROM (SELECT COUNT(*) in_rows FROM x) x;

DROP STATS x;
REFRESH x;
SELECT COUNT(*) FROM (SELECT AVG(v1) v1, AVG(v2) v2, AVG(v3) v3 FROM x GROUP BY id4) t;
SELECT UNIX_TIMESTAMP() _timestamp, 'groupby' _task, CONCAT(REGEXP_EXTRACT('${var:SRC_GRP_DIR}','[^/]+$',0), '.csv') _data, x.in_rows _in_rows, 'mean v1:v3 by id4' _question, 'impala' _solution, 'AVG GROUP BY' _fun, 2 _run FROM (SELECT COUNT(*) in_rows FROM x) x;

DROP STATS x;
REFRESH x;
SELECT COUNT(*) FROM (SELECT AVG(v1) v1, AVG(v2) v2, AVG(v3) v3 FROM x GROUP BY id4) t;
SELECT UNIX_TIMESTAMP() _timestamp, 'groupby' _task, CONCAT(REGEXP_EXTRACT('${var:SRC_GRP_DIR}','[^/]+$',0), '.csv') _data, x.in_rows _in_rows, 'mean v1:v3 by id4' _question, 'impala' _solution, 'AVG GROUP BY' _fun, 3 _run FROM (SELECT COUNT(*) in_rows FROM x) x;

-- question = "sum v1:v3 by id6" #5
DROP STATS x;
REFRESH x;
SELECT COUNT(*) FROM (SELECT SUM(v1) v1, SUM(v2) v2, SUM(v3) v3 FROM x GROUP BY id6) t;
SELECT UNIX_TIMESTAMP() _timestamp, 'groupby' _task, CONCAT(REGEXP_EXTRACT('${var:SRC_GRP_DIR}','[^/]+$',0), '.csv') _data, x.in_rows _in_rows, 'sum v1:v3 by id6' _question, 'impala' _solution, 'SUM GROUP BY' _fun, 1 _run FROM (SELECT COUNT(*) in_rows FROM x) x;

DROP STATS x;
REFRESH x;
SELECT COUNT(*) FROM (SELECT SUM(v1) v1, SUM(v2) v2, SUM(v3) v3 FROM x GROUP BY id6) t;
SELECT UNIX_TIMESTAMP() _timestamp, 'groupby' _task, CONCAT(REGEXP_EXTRACT('${var:SRC_GRP_DIR}','[^/]+$',0), '.csv') _data, x.in_rows _in_rows, 'sum v1:v3 by id6' _question, 'impala' _solution, 'SUM GROUP BY' _fun, 2 _run FROM (SELECT COUNT(*) in_rows FROM x) x;

DROP STATS x;
REFRESH x;
SELECT COUNT(*) FROM (SELECT SUM(v1) v1, SUM(v2) v2, SUM(v3) v3 FROM x GROUP BY id6) t;
SELECT UNIX_TIMESTAMP() _timestamp, 'groupby' _task, CONCAT(REGEXP_EXTRACT('${var:SRC_GRP_DIR}','[^/]+$',0), '.csv') _data, x.in_rows _in_rows, 'sum v1:v3 by id6' _question, 'impala' _solution, 'SUM GROUP BY' _fun, 3 _run FROM (SELECT COUNT(*) in_rows FROM x) x;

shell echo "###impala-body";

USE default;
DROP DATABASE benchmark CASCADE;
