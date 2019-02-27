#!/bin/bash
set -e

if [ "$#" -ne 2 ]; then
    echo "usage: ./clickhouse-exec.sh groupby G1_1e7_1e2_0_0";
    exit 1
fi;

# for each data_name produce sql script
sed "s/DATA_NAME/$2/" < "clickhouse/$1-clickhouse.sql.in" > "clickhouse/$1-clickhouse.sql"

rm -f clickhouse/log_$1_q*.csv

cat "clickhouse/$1-clickhouse.sql" | clickhouse-client -mn --max_memory_usage=109951162777600 --log_queries 1 --format=Pretty --output_format_pretty_max_rows 1

Rscript clickhouse/clickhouse-parse-log.R "$1"
