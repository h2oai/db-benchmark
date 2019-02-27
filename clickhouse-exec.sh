#!/bin/bash
set -e

rm -f clickhouse/log_$1_q*.csv

cat "clickhouse/$1-clickhouse.sql" | clickhouse-client -mn --max_memory_usage=109951162777600 --log_queries 1 --format=Pretty --output_format_pretty_max_rows 1

Rscript clickhouse/clickhouse-parse-log.R "$1"
