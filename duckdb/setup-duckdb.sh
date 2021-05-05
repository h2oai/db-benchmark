#!/bin/bash
set -e

# install stable duckdb
mkdir -p ./duckdb/r-duckdb
Rscript -e 'install.packages("duckdb", repos="https://cloud.r-project.org/", lib="./duckdb/r-duckdb")'
