#!/bin/bash
set -e

# dirs for datasets and output of benchmark
mkdir -p data
mkdir -p out

# install R
sudo add-apt-repository "deb https://cloud.r-project.org/bin/linux/ubuntu $(lsb_release -cs)-cran40/"
sudo apt-key adv --keyserver keyserver.ubuntu.com --recv-keys E298A3A825C0D65DFD57CBB651716619E084DAB9
sudo apt-get update -qq
sudo apt-get install -y r-base-dev
echo 'LC_ALL=C' >> ~/.Renviron

# setup ~/.R/Makevars
mkdir -p ~/.R
echo 'CFLAGS=-O3 -mtune=native' > ~/.R/Makevars
echo 'CXXFLAGS=-O3 -mtune=native' >> ~/.R/Makevars

# packages used in launcher and report
Rscript -e 'install.packages(c("bit64","rmarkdown","data.table","rpivotTable","formattable","lattice","arrow"))'
Rscript -e 'sapply(c("bit64","rmarkdown","data.table","rpivotTable","formattable","lattice","arrow"), requireNamespace)'

# after each restart of server
source clickhouse/ch.sh && ch_stop
sudo service docker stop
sudo swapoff -a

# stop and disable
sudo systemctl disable docker
sudo systemctl stop docker
sudo systemctl disable clickhouse-server
sudo systemctl stop clickhouse-server

# generate arrow files from csv
R
library(arrow)
library(data.table)
setDTthreads(0L)
g1 = sprintf("G1_%s_%s_0_%s", rep(c("1e7","1e8","1e9"), each=4L), rep(c("1e2","1e1","2e0","1e2"), times=3L), rep(c("0","0","0","1"), times=3L))
j1 = sprintf("J1_%s_%s_0_0", rep(c("1e7","1e8","1e9"), each=4L), trimws(gsub("e+0","e", format(c(sapply(c(1e7,1e8,1e9), `/`, c(NA,1e6,1e3,1e0))), digits=1), fixed=TRUE)))
csv2feather = function(dn) {
  cat("csv2feather:", dn, "\n")
  df = fread(sprintf("data/%s.csv", dn), showProgress=FALSE, stringsAsFactors=TRUE, data.table=FALSE)
  write_feather(df, sprintf("data/%s.feather", dn))
  rm(df)
  TRUE
}
sapply(c(g1, j1), csv2feather)
