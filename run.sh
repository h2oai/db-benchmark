#!/bin/bash
set -e

# get config
source run.conf

# set batch
export BATCH=$(date +%s)

# produce iteration dictionaries from data.csv
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

# dplyr
./dplyr/dplyr.sh

# publish timing locally
# Rscript -e 'rmarkdown::render("index.Rmd")'
# push timing data to shiny
# rsync -aq time.csv $USER@$SHINY:.
