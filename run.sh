#!/bin/bash
set -e

# get config
source run.conf

# produce iteration dictionaries from data.csv
./init-setup-iteration.R

# set batch
export BATCH=$(date +%s)

echo "# Benchmark run $BATCH started"

# upgrade tools
$DO_UPGRADE && ./datatable/init-datatable.sh
$DO_UPGRADE && ./dplyr/init-dplyr.sh
$DO_UPGRADE && ./pydatatable/init-pydatatable.sh

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

# publish benchmark, only if token file exists
$DO_PUBLISH && [ -f ./token ] && ./publish.sh && echo "# Benchmark results has been published"

# completed
echo "# Benchmark run $BATCH has been completed in $(($(date +%s)-$BATCH))s"
