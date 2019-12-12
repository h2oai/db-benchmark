#!/bin/bash
set -e

# dirs for datasets and output of benchmark, not related to datatable setup
mkdir -p data
mkdir -p out
# packages used in launcher and report
Rscript -e 'install.packages(c("bit64","rmarkdown","data.table","rpivotTable"))'

# setup ~/.R/Makevars
mkdir -p ~/.R
echo 'CFLAGS=-O3 -mtune=native' > ~/.R/Makevars
echo 'CXXFLAGS=-O3 -mtune=native' >> ~/.R/Makevars

# after each restart of server
source clickhouse/ch.sh && ch_stop
sudo service docker stop
sudo swapoff -a
