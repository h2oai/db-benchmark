#!/bin/bash
set -e

# get config
source run.conf

# spark
./spark/spark.sh

# impala
./impala/impala.sh

# datatable
./datatable/datatable.sh

# h2o
./h2o/h2o.sh

# pandas
./pandas/pandas.sh # not run for 1e9

# publish timing
Rscript -e 'knitr::knit2html("time.Rmd")'
