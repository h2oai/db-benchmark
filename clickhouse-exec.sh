#!/bin/bash

cat "clickhouse/$1-clickhouse.sql" | clickhouse-client -mn --max_memory_usage=109951162777600 --log_queries 1 --output_format_pretty_max_rows 1

Rscript clickhouse/clickhouse-parse-log.R "$1"
