#!/bin/bash
set -e

# get config
source run.conf

# ensure all presto shutdown
./presto/shutdown-presto.sh || true

# test network speed
./servertest.sh
Rscript -e 'data.table::fread("servertest.log")[substr(V3,1,2)!="Gb" | V2 < 9, if (!.N) q("no") else if(any(V2<5)) stop("slow (< 5 Gb) network interface on nodes: ",paste(V1[V2<5],collapse=", "), call.=FALSE) else warning("network interface below 9 Gb on nodes: ",paste(V1,collapse=", "), call.=FALSE)]'

# produce iteration dictionaries from data.csv
./init-setup-iteration.R

# set batch
export BATCH=$(date +%s)

echo "# Benchmark run $BATCH started"

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

# test network speed
./servertest.sh
Rscript -e 'data.table::fread("servertest.log")[substr(V3,1,2)!="Gb" | V2 < 9.5, if(.N) stop("slow (< 9.5 Gb) network interface on nodes: ",paste(V1,collapse=","), call.=FALSE) else q("no")]'
