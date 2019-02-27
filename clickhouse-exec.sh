#!/bin/bash
set -e

if [ "$#" -ne 2 ]; then
    echo "usage: ./clickhouse-exec.sh groupby G1_1e7_1e2_0_0";
    exit 1
fi;

# for each data_name produce sql script
sed "s/DATA_NAME/$2/" < "clickhouse/$1-clickhouse.sql.in" > "clickhouse/$1-clickhouse.sql"

# cleanup timings from last run if they have not been cleaned up after parsing
rm -f clickhouse/log_$1_$2_q*.csv

# execute sql script on clickhouse
cat "clickhouse/$1-clickhouse.sql" | clickhouse-client -mn --max_memory_usage=109951162777600 --log_queries 1 --format=Pretty --output_format_pretty_max_rows 1

# parse timings from log_[task]_[data_name]_q[i]_r[j].csv timings log files
Rscript clickhouse/clickhouse-parse-log.R "$1" "$2"
