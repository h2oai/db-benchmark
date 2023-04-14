#!/bin/bash
set -e

if [ "$#" -ne 1 ]; then
  echo 'usage: ./clickhouse/exec.sh groupby';
  exit 1
fi;

source ./clickhouse/ch.sh
source ./_helpers/helpers.sh

# start server
ch_start

# confirm server working, wait if it crashed in last run
ch_active || sleep 20
ch_active || echo 'clickhouse-server should be already running, investigate' >&2
ch_active || exit 1


# tail -n+2 data/G1_1e7_1e2_0_0.csv | clickhouse-client --query="INSERT INTO G1_1e7_1e2_0_0 SELECT * FROM input('id1 Nullable(String), id2 Nullable(String), id3 Nullable(String), id4 Nullable(Int32), id5 Nullable(Int32), id6 Nullable(Int32), v1 Nullable(Int32), v2 Nullable(Int32), v3 Nullable(Float64)') FORMAT CSV"

# tune CH settings and load data
CH_MEM=107374182400 # 100GB ## old value 128849018880 # 120GB ## now set to 96GB after cache=1 to in-memory temp tables because there was not enough mem for R to parse timings
clickhouse-client --query 'DROP TABLE IF EXISTS ans'
echo '# clickhouse/exec.sh: creating tables and loading data'
if [ $1 == 'groupby' ]; then
  CH_EXT_GRP_BY=53687091200 # twice less than CH_MEM #96
  CH_EXT_SORT=53687091200
  clickhouse-client --query "DROP TABLE IF EXISTS $SRC_DATANAME"
  clickhouse-client --query "CREATE TABLE $SRC_DATANAME (id1 Nullable(String), id2 Nullable(String), id3 Nullable(String), id4 Nullable(Int32), id5 Nullable(Int32), id6 Nullable(Int32), v1 Nullable(Int32), v2 Nullable(Int32), v3 Nullable(Float64)) ENGINE = MergeTree() ORDER BY tuple();"
  tail -n+2 data/$SRC_DATANAME.csv | clickhouse-client --max_memory_usage $CH_MEM --max_insert_threads 1 --query "INSERT INTO $SRC_DATANAME SELECT * FROM input('id1 Nullable(String), id2 Nullable(String), id3 Nullable(String), id4 Nullable(Int32), id5 Nullable(Int32), id6 Nullable(Int32), v1 Nullable(Int32), v2 Nullable(Int32), v3 Nullable(Float64)') FORMAT CSV"
  # confirm all data loaded yandex/ClickHouse#4463
  echo -e "clickhouse-client --query 'SELECT count(*) FROM $SRC_DATANAME'\n$(echo $SRC_DATANAME | cut -d'_' -f2)" | Rscript -e 'stdin=readLines(file("stdin")); if ((loaded<-as.numeric(system(stdin[1L], intern=TRUE)))!=as.numeric(stdin[2L])) stop("incomplete data load, expected: ", stdin[2L],", loaded: ", loaded)'
elif [ $1 == 'join' ]; then
  # lhs
  clickhouse-client --query "DROP TABLE IF EXISTS $SRC_DATANAME"
  clickhouse-client --query "CREATE TABLE $SRC_DATANAME (id1 Nullable(Int32), id2 Nullable(Int32), id3 Nullable(Int32), id4 Nullable(String), id5 Nullable(String), id6 Nullable(String), v1 Nullable(Float64)) ENGINE = MergeTree() ORDER BY tuple();"
  tail -n+2 data/$SRC_DATANAME.csv | clickhouse-client --max_memory_usage $CH_MEM --max_insert_threads 1 --query "INSERT INTO $SRC_DATANAME SELECT * FROM input('id1 Nullable(Int32), id2 Nullable(Int32), id3 Nullable(Int32), id4 Nullable(String), id5 Nullable(String), id6 Nullable(String), v1 Nullable(Float64)') FORMAT CSV"
  echo -e "clickhouse-client --query 'SELECT count(*) FROM $SRC_DATANAME'\n$(echo $SRC_DATANAME | cut -d'_' -f2)" | Rscript -e 'stdin=readLines(file("stdin")); if ((loaded<-as.numeric(system(stdin[1L], intern=TRUE)))!=as.numeric(stdin[2L])) stop("incomplete data load, expected: ", stdin[2L],", loaded: ", loaded)'
  RHS=$(join_to_tbls $SRC_DATANAME)
  RHS1=$(echo $RHS | cut -d' ' -f1)
  clickhouse-client --query "DROP TABLE IF EXISTS $RHS1"
  clickhouse-client --query "CREATE TABLE $RHS1 (id1 Nullable(Int32), id4 Nullable(String), v2 Nullable(Float64)) ENGINE = MergeTree() ORDER BY tuple();"
  tail -n+2 data/$RHS1.csv | clickhouse-client --max_memory_usage $CH_MEM --max_insert_threads 1 --query "INSERT INTO $RHS1 SELECT * FROM input('id1 Nullable(Int32), id4 Nullable(String), v2 Nullable(Float64)') FORMAT CSV"
  echo -e "clickhouse-client --query 'SELECT count(*) FROM $RHS1'\n$(echo $RHS1 | cut -d'_' -f3)" | Rscript -e 'stdin=readLines(file("stdin")); if ((loaded<-as.numeric(system(stdin[1L], intern=TRUE)))!=as.numeric(stdin[2L])) stop("incomplete data load, expected: ", stdin[2L],", loaded: ", loaded)'
  RHS2=$(echo $RHS | cut -d' ' -f2)
  clickhouse-client --query "DROP TABLE IF EXISTS $RHS2"
  clickhouse-client --query "CREATE TABLE $RHS2 (id1 Nullable(Int32), id2 Nullable(Int32), id4 Nullable(String), id5 Nullable(String), v2 Nullable(Float64)) ENGINE = MergeTree() ORDER BY tuple();"
  tail -n+2 data/$RHS2.csv | clickhouse-client --max_memory_usage $CH_MEM --max_insert_threads 1 --query "INSERT INTO $RHS2 SELECT * FROM input('id1 Nullable(Int32), id2 Nullable(Int32), id4 Nullable(String), id5 Nullable(String), v2 Nullable(Float64)') FORMAT CSV"
  echo -e "clickhouse-client --query 'SELECT count(*) FROM $RHS2'\n$(echo $RHS2 | cut -d'_' -f3)" | Rscript -e 'stdin=readLines(file("stdin")); if ((loaded<-as.numeric(system(stdin[1L], intern=TRUE)))!=as.numeric(stdin[2L])) stop("incomplete data load, expected: ", stdin[2L],", loaded: ", loaded)'
  RHS3=$(echo $RHS | cut -d' ' -f3)
  clickhouse-client --query "DROP TABLE IF EXISTS $RHS3"
  clickhouse-client --query "CREATE TABLE $RHS3 (id1 Nullable(Int32), id2 Nullable(Int32), id3 Nullable(Int32), id4 Nullable(String), id5 Nullable(String), id6 Nullable(String), v2 Nullable(Float64)) ENGINE = MergeTree() ORDER BY tuple();"
  tail -n+2 data/$RHS3.csv | clickhouse-client --max_memory_usage $CH_MEM --max_insert_threads 1 --query "INSERT INTO $RHS3 SELECT * FROM input('id1 Nullable(Int32), id2 Nullable(Int32), id3 Nullable(Int32), id4 Nullable(String), id5 Nullable(String), id6 Nullable(String), v2 Nullable(Float64)') FORMAT CSV"
  echo -e "clickhouse-client --query 'SELECT count(*) FROM $RHS3'\n$(echo $RHS3 | cut -d'_' -f3)" | Rscript -e 'stdin=readLines(file("stdin")); if ((loaded<-as.numeric(system(stdin[1L], intern=TRUE)))!=as.numeric(stdin[2L])) stop("incomplete data load, expected: ", stdin[2L],", loaded: ", loaded)'
else
  echo "clickhouse task $1 not implemented" >&2 && exit 1
fi

# cleanup timings from last run if they have not been cleaned up after parsing
mkdir -p clickhouse/log
rm -f clickhouse/log/$1_${SRC_DATANAME}_q*.csv

# execute sql script on clickhouse
clickhouse-client --query 'TRUNCATE TABLE system.query_log'
echo "# clickhouse/exec.sh: data loaded, logs truncated, preparing $1-$SRC_DATANAME benchmark sql script and sending it clickhouse"
if [ $1 == 'groupby' ]; then
  # for each data_name produce sql script
  sed "s/DATA_NAME/$SRC_DATANAME/g" < "clickhouse/$1-clickhouse.sql.in" > "clickhouse/$1-clickhouse.sql"
  cat "clickhouse/$1-clickhouse.sql" | clickhouse-client -mn --max_memory_usage $CH_MEM --max_bytes_before_external_group_by $CH_EXT_GRP_BY --max_bytes_before_external_sort $CH_EXT_SORT --receive_timeout 10800 --format Pretty && echo '# clickhouse/exec.sh: benchmark sql script finished' || echo "# clickhouse/exec.sh: benchmark sql script for $SRC_DATANAME terminated with error"
elif [ $1 == 'join' ]; then
  sed "s/DATA_NAME/$SRC_DATANAME/g; s/RHS_SMALL/$RHS1/g; s/RHS_MEDIUM/$RHS2/g; s/RHS_BIG/$RHS3/g" < "clickhouse/join-clickhouse.sql.in" > "clickhouse/join-clickhouse.sql"
  cat "clickhouse/$1-clickhouse.sql" | clickhouse-client -mn --max_memory_usage $CH_MEM --receive_timeout 10800 --format Pretty && echo '# clickhouse/exec.sh: benchmark sql script finished' || echo "# clickhouse/exec.sh: benchmark sql script for $SRC_DATANAME terminated with error"
else
  echo "clickhouse task $1 benchmark script launching not defined" >&2 && exit 1
fi

# need to wait in case if server crashed to release memory
sleep 90

# cleanup data
ch_active && echo '# clickhouse/exec.sh: finishing, cleaning up' && clickhouse-client --query "DROP TABLE IF EXISTS ans" && clickhouse-client --query "DROP TABLE IF EXISTS $SRC_DATANAME" || echo '# clickhouse/exec.sh: finishing, clickhouse server down, possibly crashed, could not clean up'

# stop server
ch_stop && echo '# clickhouse/exec.sh: stopping server finished' || echo '# clickhouse/exec.sh: stopping server failed'

# wait for memory
sleep 30

# parse timings from clickhouse/log/[task]_[data_name]_q[i]_r[j].csv
Rscript clickhouse/clickhouse-parse-log.R $1 $SRC_DATANAME
