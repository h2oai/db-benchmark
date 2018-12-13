#!/bin/bash
set -e

# dirs for datasets and output of benchmark, not related to datatable setup
mkdir -p data
mkdir -p out
# packages used in report, data loading, sum over int col
Rscript -e 'install.packages(c("bit64","fst","rmarkdown"))'

# install R

# setup ~/.R/Makevars
mkdir -p ~/.R
echo 'CFLAGS=-O3 -mtune=native' > ~/.R/Makevars
echo 'CXXFLAGS=-O3 -mtune=native' >> ~/.R/Makevars

# install latest dev
Rscript -e 'install.packages("data.table", repos="https://Rdatatable.github.io/data.table", method="curl")'
