#!/bin/bash
set -e

if [ "$#" -ne 2 ]; then
    echo "usage: ./clickhouse/exec.sh groupby G1_1e7_1e2_0_0";
    exit 1
fi;

source ./clickhouse/ch.sh

# start server
ch_start

# confirm server working, wait if it crashed in last run
ch_active || sleep 120
ch_active || echo "clickhouse-server should be already running, investigate" >&2
ch_active || exit 1

# load data
CH_MEM=128849018880 # 120GB; 107374182400 # 100GB
clickhouse-client --query="TRUNCATE TABLE $2"
clickhouse-client --max_memory_usage=$CH_MEM --query="INSERT INTO $2 FORMAT CSVWithNames" < "data/$2.csv"
# confirm all data loaded yandex/ClickHouse#4463
echo -e "clickhouse-client --query=\"SELECT count(*) FROM $2\"\n$2" | Rscript -e 'source("./_helpers/helpers.R"); stdin=readLines(file("stdin")); if ((loaded<-as.numeric(system(stdin[1L], intern=TRUE)))!=get.nrow(data_name=stdin[2L])) stop("incomplete data load for ", stdin[2L],", loaded ", loaded, " rows only")'

# for each data_name produce sql script
sed "s/DATA_NAME/$2/g" < "clickhouse/$1-clickhouse.sql.in" > "clickhouse/$1-clickhouse.sql"

# cleanup timings from last run if they have not been cleaned up after parsing
mkdir -p clickhouse/log
rm -f clickhouse/log/$1_$2_q*.csv

# execute sql script on clickhouse
clickhouse-client --query="TRUNCATE TABLE system.query_log"
cat "clickhouse/$1-clickhouse.sql" | clickhouse-client -mn --max_memory_usage=$CH_MEM --format=Pretty --output_format_pretty_max_rows 1 || echo "# clickhouse-exec.sh: benchmark sql script for $2 terminated with error" >&2

# parse timings from clickhouse/log/[task]_[data_name]_q[i]_r[j].csv
Rscript clickhouse/clickhouse-parse-log.R "$1" "$2"

# cleanup data
ch_active && echo "# clickhouse-exec.sh: finishing, truncating table $2" && clickhouse-client --query="TRUNCATE TABLE $2" || echo "# clickhouse-exec.sh: finishing, clickhouse server down, possibly crashed, could not truncate table $2" >&2

# stop server
ch_stop && echo "# clickhouse-exec.sh: stopping server finished" || echo "# clickhouse-exec.sh: stopping server failed" >&2
