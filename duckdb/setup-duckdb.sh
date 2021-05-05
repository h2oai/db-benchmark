#!/bin/bash
set -e

# install stable duckdb
mkdir -p ./duckdb
Rscript -e 'install.packages("duckdb", repo="https://cloud.r-project.org/", lib="./duckdb")'
