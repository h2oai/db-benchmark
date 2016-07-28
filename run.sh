#!/bin/bash
set -e

# get config
source run.conf

# set batch
export BATCH=$(date +%s)

# produce iteration dictionaries
./init-setup-iteration.R

# spark
./spark/spark.sh

# impala
./impala/impala.sh

# datatable
./datatable/datatable.sh

# h2o
./h2o/h2o.sh

# pandas
./pandas/pandas.sh

# dask
./dask/dask.sh

# publish timing
Rscript -e 'knitr::knit2html("time.Rmd")'
