
.timer on

CREATE TABLE ans_1_1 AS SELECT x.*, small.id4 AS small_id4, v2 FROM x JOIN small USING (id1);
-- Run Time (s): real 233.101 user 1226.698236 sys 272.034250
copy ans_1_1 TO 'ans_1_1.csv' (HEADER, FORMAT CSV);
-- internal format is probably a lot smaller, but the result size it 50G. Same as x (or big);
-- let's say the internal format is something like 38G? that's the size of parquet. 
CREATE TABLE ans_1_2 AS SELECT x.*, small.id4 AS small_id4, v2 FROM x JOIN small USING (id1);
-- 72G G

CREATE TABLE ans_2_1 AS SELECT x.*, medium.id1 AS medium_id1, medium.id4 AS medium_id4, medium.id5 AS medium_id5, v2 FROM x JOIN medium USING (id2)
CREATE TABLE ans_2_2 AS SELECT x.*, medium.id1 AS medium_id1, medium.id4 AS medium_id4, medium.id5 AS medium_id5, v2 FROM x JOIN medium USING (id2)
-- 50G

CREATE TABLE ans_3_1 AS SELECT x.*, medium.id1 AS medium_id1, medium.id4 AS medium_id4, medium.id5 AS medium_id5, v2 FROM x LEFT JOIN medium USING (id2)
CREATE TABLE ans_3_2 AS SELECT x.*, medium.id1 AS medium_id1, medium.id4 AS medium_id4, medium.id5 AS medium_id5, v2 FROM x LEFT JOIN medium USING (id2)
-- 50G

CREATE TABLE ans_4_1 AS SELECT x.*, medium.id1 AS medium_id1, medium.id2 AS medium_id2, medium.id4 AS medium_id4, v2 FROM x JOIN medium USING (id5)
CREATE TABLE ans_4_2 AS SELECT x.*, medium.id1 AS medium_id1, medium.id2 AS medium_id2, medium.id4 AS medium_id4, v2 FROM x JOIN medium USING (id5)
-- 50G

CREATE TEMP TABLE ans_5_1 AS SELECT x.*, big.id1 AS big_id1, big.id2 AS big_id2, big.id4 AS big_id4, big.id5 AS big_id5, big.id6 AS big_id6, v2 FROM x JOIN big USING (id3);
CREATE TEMP TABLE ans_5_2 AS SELECT x.*, big.id1 AS big_id1, big.id2 AS big_id2, big.id4 AS big_id4, big.id5 AS big_id5, big.id6 AS big_id6, v2 FROM x JOIN big USING (id3)
-- 50G