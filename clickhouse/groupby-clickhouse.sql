--variables requires: https://github.com/yandex/ClickHouse/issues/3815
--data_name='G1_1e7_1e2_0_0'
--task='groupby'
--fun='select group by'
--cache=0

/* q1 */
--question='sum v1 by id1'

SELECT id1, sum(v1) AS v1 FROM G1_1e7_1e2_0_0 GROUP BY id1;

SELECT 1 AS run, toUnixTimestamp(now()) AS timestamp, 'groupby' AS task, 'G1_1e7_1e2_0_0' AS data_name, read_rows AS in_rows, 'sum v1 by id1' AS question, result_rows AS out_rows, NULL AS out_cols, 'clickhouse' AS solution, version() AS version, NULL AS git, 'select group by' AS fun, query_duration_ms/1000 AS time_sec, memory_usage/1073741824 AS mem_gb, 0 AS cache, NULL AS chk, NULL AS chk_time_sec
FROM system.query_log WHERE type=2 AND query='SELECT id1, sum(v1) AS v1 FROM G1_1e7_1e2_0_0 GROUP BY id1;'
ORDER BY query_start_time DESC LIMIT 1 INTO OUTFILE 'clickhouse/log_groupby_q1_r1.csv' FORMAT CSVWithNames;

SELECT id1, sum(v1) AS v1 FROM G1_1e7_1e2_0_0 GROUP BY id1;

SELECT 2 AS run, toUnixTimestamp(now()) AS timestamp, 'groupby' AS task, 'G1_1e7_1e2_0_0' AS data_name, read_rows AS in_rows, 'sum v1 by id1' AS question, result_rows AS out_rows, NULL AS out_cols, 'clickhouse' AS solution, version() AS version, NULL AS git, 'select group by' AS fun, query_duration_ms/1000 AS time_sec, memory_usage/1073741824 AS mem_gb, 0 AS cache, NULL AS chk, NULL AS chk_time_sec
FROM system.query_log WHERE type=2 AND query='SELECT id1, sum(v1) AS v1 FROM G1_1e7_1e2_0_0 GROUP BY id1;'
ORDER BY query_start_time DESC LIMIT 1 INTO OUTFILE 'clickhouse/log_groupby_q1_r2.csv' FORMAT CSVWithNames;

/* q2 */
--question='sum v1 by id1:id2'

SELECT id1, id2, sum(v1) AS v1 FROM G1_1e7_1e2_0_0 GROUP BY id1, id2;

SELECT 1 AS run, toUnixTimestamp(now()) AS timestamp, 'groupby' AS task, 'G1_1e7_1e2_0_0' AS data_name, read_rows AS in_rows, 'sum v1 by id1:id2' AS question, result_rows AS out_rows, NULL AS out_cols, 'clickhouse' AS solution, version() AS version, NULL AS git, 'select group by' AS fun, query_duration_ms/1000 AS time_sec, memory_usage/1073741824 AS mem_gb, 0 AS cache, NULL AS chk, NULL AS chk_time_sec
FROM system.query_log WHERE type=2 AND query='SELECT id1, id2, sum(v1) AS v1 FROM G1_1e7_1e2_0_0 GROUP BY id1, id2;'
ORDER BY query_start_time DESC LIMIT 1 INTO OUTFILE 'clickhouse/log_groupby_q2_r1.csv' FORMAT CSVWithNames;

SELECT id1, id2, sum(v1) AS v1 FROM G1_1e7_1e2_0_0 GROUP BY id1, id2;

SELECT 2 AS run, toUnixTimestamp(now()) AS timestamp, 'groupby' AS task, 'G1_1e7_1e2_0_0' AS data_name, read_rows AS in_rows, 'sum v1 by id1:id2' AS question, result_rows AS out_rows, NULL AS out_cols, 'clickhouse' AS solution, version() AS version, NULL AS git, 'select group by' AS fun, query_duration_ms/1000 AS time_sec, memory_usage/1073741824 AS mem_gb, 0 AS cache, NULL AS chk, NULL AS chk_time_sec
FROM system.query_log WHERE type=2 AND query='SELECT id1, id2, sum(v1) AS v1 FROM G1_1e7_1e2_0_0 GROUP BY id1, id2;'
ORDER BY query_start_time DESC LIMIT 1 INTO OUTFILE 'clickhouse/log_groupby_q2_r2.csv' FORMAT CSVWithNames;

/* q3 */
--question='sum v1 mean v3 by id3'

SELECT id3, sum(v1) AS v1, avg(v3) AS v3 FROM G1_1e7_1e2_0_0 GROUP BY id3;

SELECT 1 AS run, toUnixTimestamp(now()) AS timestamp, 'groupby' AS task, 'G1_1e7_1e2_0_0' AS data_name, read_rows AS in_rows, 'sum v1 mean v3 by id3' AS question, result_rows AS out_rows, NULL AS out_cols, 'clickhouse' AS solution, version() AS version, NULL AS git, 'select group by' AS fun, query_duration_ms/1000 AS time_sec, memory_usage/1073741824 AS mem_gb, 0 AS cache, NULL AS chk, NULL AS chk_time_sec
FROM system.query_log WHERE type=2 AND query='SELECT id3, sum(v1) AS v1, avg(v3) AS v3 FROM G1_1e7_1e2_0_0 GROUP BY id3;'
ORDER BY query_start_time DESC LIMIT 1 INTO OUTFILE 'clickhouse/log_groupby_q3_r1.csv' FORMAT CSVWithNames;

SELECT id3, sum(v1) AS v1, avg(v3) AS v3 FROM G1_1e7_1e2_0_0 GROUP BY id3;

SELECT 2 AS run, toUnixTimestamp(now()) AS timestamp, 'groupby' AS task, 'G1_1e7_1e2_0_0' AS data_name, read_rows AS in_rows, 'sum v1 mean v3 by id3' AS question, result_rows AS out_rows, NULL AS out_cols, 'clickhouse' AS solution, version() AS version, NULL AS git, 'select group by' AS fun, query_duration_ms/1000 AS time_sec, memory_usage/1073741824 AS mem_gb, 0 AS cache, NULL AS chk, NULL AS chk_time_sec
FROM system.query_log WHERE type=2 AND query='SELECT id3, sum(v1) AS v1, avg(v3) AS v3 FROM G1_1e7_1e2_0_0 GROUP BY id3;'
ORDER BY query_start_time DESC LIMIT 1 INTO OUTFILE 'clickhouse/log_groupby_q3_r2.csv' FORMAT CSVWithNames;

/* q4 */
--question='mean v1:v3 by id4'

SELECT id4, avg(v1) AS v1, avg(v2) AS v2, avg(v3) AS v3 FROM G1_1e7_1e2_0_0 GROUP BY id4;

SELECT 1 AS run, toUnixTimestamp(now()) AS timestamp, 'groupby' AS task, 'G1_1e7_1e2_0_0' AS data_name, read_rows AS in_rows, 'mean v1:v3 by id4' AS question, result_rows AS out_rows, NULL AS out_cols, 'clickhouse' AS solution, version() AS version, NULL AS git, 'select group by' AS fun, query_duration_ms/1000 AS time_sec, memory_usage/1073741824 AS mem_gb, 0 AS cache, NULL AS chk, NULL AS chk_time_sec
FROM system.query_log WHERE type=2 AND query='SELECT id4, avg(v1) AS v1, avg(v2) AS v2, avg(v3) AS v3 FROM G1_1e7_1e2_0_0 GROUP BY id4;'
ORDER BY query_start_time DESC LIMIT 1 INTO OUTFILE 'clickhouse/log_groupby_q4_r1.csv' FORMAT CSVWithNames;

SELECT id4, avg(v1) AS v1, avg(v2) AS v2, avg(v3) AS v3 FROM G1_1e7_1e2_0_0 GROUP BY id4;

SELECT 2 AS run, toUnixTimestamp(now()) AS timestamp, 'groupby' AS task, 'G1_1e7_1e2_0_0' AS data_name, read_rows AS in_rows, 'mean v1:v3 by id4' AS question, result_rows AS out_rows, NULL AS out_cols, 'clickhouse' AS solution, version() AS version, NULL AS git, 'select group by' AS fun, query_duration_ms/1000 AS time_sec, memory_usage/1073741824 AS mem_gb, 0 AS cache, NULL AS chk, NULL AS chk_time_sec
FROM system.query_log WHERE type=2 AND query='SELECT id4, avg(v1) AS v1, avg(v2) AS v2, avg(v3) AS v3 FROM G1_1e7_1e2_0_0 GROUP BY id4;'
ORDER BY query_start_time DESC LIMIT 1 INTO OUTFILE 'clickhouse/log_groupby_q4_r2.csv' FORMAT CSVWithNames;

/* q5 */
--question='sum v1:v3 by id6'

SELECT id6, sum(v1) AS v1, sum(v2) AS v2, sum(v3) AS v3 FROM G1_1e7_1e2_0_0 GROUP BY id6;

SELECT 1 AS run, toUnixTimestamp(now()) AS timestamp, 'groupby' AS task, 'G1_1e7_1e2_0_0' AS data_name, read_rows AS in_rows, 'sum v1:v3 by id6' AS question, result_rows AS out_rows, NULL AS out_cols, 'clickhouse' AS solution, version() AS version, NULL AS git, 'select group by' AS fun, query_duration_ms/1000 AS time_sec, memory_usage/1073741824 AS mem_gb, 0 AS cache, NULL AS chk, NULL AS chk_time_sec
FROM system.query_log WHERE type=2 AND query='SELECT id6, sum(v1) AS v1, sum(v2) AS v2, sum(v3) AS v3 FROM G1_1e7_1e2_0_0 GROUP BY id6;'
ORDER BY query_start_time DESC LIMIT 1 INTO OUTFILE 'clickhouse/log_groupby_q5_r1.csv' FORMAT CSVWithNames;

SELECT id6, sum(v1) AS v1, sum(v2) AS v2, sum(v3) AS v3 FROM G1_1e7_1e2_0_0 GROUP BY id6;

SELECT 2 AS run, toUnixTimestamp(now()) AS timestamp, 'groupby' AS task, 'G1_1e7_1e2_0_0' AS data_name, read_rows AS in_rows, 'sum v1:v3 by id6' AS question, result_rows AS out_rows, NULL AS out_cols, 'clickhouse' AS solution, version() AS version, NULL AS git, 'select group by' AS fun, query_duration_ms/1000 AS time_sec, memory_usage/1073741824 AS mem_gb, 0 AS cache, NULL AS chk, NULL AS chk_time_sec
FROM system.query_log WHERE type=2 AND query='SELECT id6, sum(v1) AS v1, sum(v2) AS v2, sum(v3) AS v3 FROM G1_1e7_1e2_0_0 GROUP BY id6;'
ORDER BY query_start_time DESC LIMIT 1 INTO OUTFILE 'clickhouse/log_groupby_q5_r2.csv' FORMAT CSVWithNames;

/* q6 */
--question='median v3 sd v3 by id2 id4'
--SELECT id2, id4, medianExact(v3) AS median_v3, stddevPop(v3) AS sd_v3 FROM G1_1e7_1e2_0_0 GROUP BY id2, id4;
/* q7 */
--question='max v1 - min v2 by id2 id4'
--SELECT id2, id4, max(v1) - min(v2) AS range_v1_v2 FROM G1_1e7_1e2_0_0 GROUP BY id2, id4;
/* q8 - https://stackoverflow.com/questions/54887869/top-n-rows-by-group-in-clickhouse */
--question='largest two v3 by id2 id4'
--SELECT id2, id4, any(v3) AS v3 FROM G1_1e7_1e2_0_0 GROUP BY id2, id4 ORDER BY v3 DESC LIMIT 2 BY id2, id4;
/* q9 */
--question='regression v1 v2 by id2 id4'
--SELECT id2, id4, pow(corr(v1, v2), 2) AS r2 FROM G1_1e7_1e2_0_0 GROUP BY id2, id4;
/* q10 */
--question='sum v3 count by id1:id6'
--SELECT id1, id2, id3, id4, id5, id6, sum(v3) AS v3, count() AS cnt FROM G1_1e7_1e2_0_0 GROUP BY id1, id2, id3, id4, id5, id6;
