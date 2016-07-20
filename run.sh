#!/bin/bash
set -e

# get config
source run.conf

# set batch
export BATCH=$(date +%s)

# spark
./spark/spark.sh

# impala
./impala/impala.sh

# datatable
./datatable/datatable.sh # 15min+

# h2o
./h2o/h2o.sh

# pandas
./pandas/pandas.sh # 60min+

# publish timing
Rscript -e 'knitr::knit2html("time.Rmd")'
