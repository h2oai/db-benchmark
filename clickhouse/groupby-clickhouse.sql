SET log_queries=1;

--variables requires: https://github.com/yandex/ClickHouse/issues/3815
--data_name='G1_1e7_1e2_0_0'
--task='groupby'
--cache=0

/* q1 */
--question='sum v1 by id1'
SELECT id1, sum(v1) AS v1 FROM G1_1e7_1e2_0_0 GROUP BY id1;
SELECT 1 AS run,
  'groupby' AS task, 'G1_1e7_1e2_0_0' AS data_name, read_rows AS in_rows, 'sum v1 by id1' AS question, result_rows AS out_rows, NULL AS out_cols, 'clickhouse' AS solution, version() AS version, NULL AS git,
  query_duration_ms/1000 AS time_sec, memory_usage/1073741824 AS mem_gb,
  0 AS cache, NULL AS chk, NULL AS chk_time_sec
FROM system.query_log
WHERE type=2
  AND query='SELECT id1, sum(v1) AS v1 FROM G1_1e7_1e2_0_0 GROUP BY id1';

/* q2 */
--question='sum v1 by id1:id2'
SELECT id1, id2, sum(v1) AS v1 FROM G1_1e7_1e2_0_0 GROUP BY id1, id2;
/* q3 */
--question='sum v1 mean v3 by id3'
SELECT id3, sum(v1) AS v1, avg(v3) AS v3 FROM G1_1e7_1e2_0_0 GROUP BY id3;
/* q4 */
--question='mean v1:v3 by id4'
SELECT id4, avg(v1) AS v1, avg(v2) AS v2, avg(v3) AS v3 FROM G1_1e7_1e2_0_0 GROUP BY id4;
/* q5 */
--question='sum v1:v3 by id6'
SELECT id6, sum(v1) AS v1, sum(v2) AS v2, sum(v3) AS v3 FROM G1_1e7_1e2_0_0 GROUP BY id6;
/* q6 */
--question='median v3 sd v3 by id2 id4'
SELECT id2, id4, medianExact(v3) AS median_v3, stddevPop(v3) AS sd_v3 FROM G1_1e7_1e2_0_0 GROUP BY id2, id4;
/* q7 */
--question='max v1 - min v2 by id2 id4'
SELECT id2, id4, max(v1) - min(v2) AS range_v1_v2 FROM G1_1e7_1e2_0_0 GROUP BY id2, id4;
/* q8 */
--question='largest two v3 by id2 id4'
SELECT id2, id4, v3 FROM G1_1e7_1e2_0_0 GROUP BY id2, id4, v3 ORDER BY v3 DESC LIMIT 2 BY id2, id4; /* group by also by v3? */
/* q9 */
--question='regression v1 v2 by id2 id4'
SELECT id2, id4, pow(corr(v1, v2), 2) AS r2 FROM G1_1e7_1e2_0_0 GROUP BY id2, id4;
/* q10 */
--question='sum v3 count by id1:id6'
SELECT id1, id2, id3, id4, id5, id6, sum(v3) AS v3, count() AS cnt FROM G1_1e7_1e2_0_0 GROUP BY id1, id2, id3, id4, id5, id6;
