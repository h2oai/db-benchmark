#!/bin/bash
set -e

# get config
source run.conf

# produce iteration dictionaries from data.csv
./init-setup-iteration.R

# set batch
export BATCH=$(date +%s)

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

# dplyr
./dplyr/dplyr.sh

# presto
./presto/presto.sh

# publish timing locally
#Rscript -e 'rmarkdown::render("index.Rmd")'

# completed
echo "# Benchmark run $BATCH has been completed in $(($(date +%s)-$BATCH))s"
