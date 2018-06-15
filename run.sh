#!/bin/bash
set -e

# get config
source run.conf

# produce iteration dictionaries from data.csv
./init-setup-iteration.R

# set batch
export BATCH=$(date +%s)

echo "# Benchmark run $BATCH started"

# datatable
./datatable/datatable.sh

# dplyr
./dplyr/dplyr.sh

# pandas
./pandas/pandas.sh

# pydatatable
./pydatatable/pydatatable.sh

# publish report for all tasks
Rscript -e 'rmarkdown::render("index.Rmd")'

# groupby benchplot
Rscript -e 'source("benchplot.R"); nr<-fread("data.csv")[task=="groupby" & active==TRUE, unique(rows)]; sapply(nr, benchplot)'

# publish benchmark, only if token file exists
[ -f ./token ] && ./publish.sh && echo "# Benchmark results has been published"

# completed
echo "# Benchmark run $BATCH has been completed in $(($(date +%s)-$BATCH))s"
