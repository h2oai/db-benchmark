#!/bin/bash
set -e

# dirs for datasets and output of benchmark, not related to datatable setup
mkdir -p data
mkdir -p out
# packages used in report, sum over int col
Rscript -e 'install.packages(c("bit64","rmarkdown","data.table"))'

# install R

# setup ~/.R/Makevars
mkdir -p ~/.R
echo 'CFLAGS=-O3 -mtune=native' > ~/.R/Makevars
echo 'CXXFLAGS=-O3 -mtune=native' >> ~/.R/Makevars

# install devel data.table
mkdir -p ./datatable/r-datatable
Rscript -e 'install.packages("data.table", repos="https://Rdatatable.gitlab.io/data.table", method="curl", lib="./datatable/r-datatable")'
