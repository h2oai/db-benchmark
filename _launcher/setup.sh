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
Rscript -e 'install.packages(c("bit64","rmarkdown","data.table","rpivotTable","formattable","arrow"))'
Rscript -e 'sapply(c("bit64","rmarkdown","data.table","rpivotTable","formattable","arrow"), requireNamespace)'

# after each restart of server
source clickhouse/ch.sh && ch_stop
sudo service docker stop
sudo swapoff -a

# stop and disable
sudo systemctl disable docker
sudo systemctl stop docker
sudo systemctl disable clickhouse-server
sudo systemctl stop clickhouse-server
