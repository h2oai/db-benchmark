#!/bin/bash
set -e

# get config
source run.conf

# cleanup
rm -f *.png
rm -f *.env
rm -f rmarkdown.out
rm -rf db-benchmark.gh-pages

# set batch
export BATCH=$(date +%s)

echo "# Benchmark run $BATCH started"

# upgrade tools
$DO_UPGRADE && ./dask/init-dask.sh
$DO_UPGRADE && ./datatable/init-datatable.sh
$DO_UPGRADE && ./dplyr/init-dplyr.sh
$DO_UPGRADE && ./juliadf/init-juliadf.sh
$DO_UPGRADE && ./pandas/init-pandas.sh
$DO_UPGRADE && ./pydatatable/init-pydatatable.sh
$DO_UPGRADE && ./spark/init-spark.sh

# produce iteration dictionaries from data.csv and solutions that should be run
./init-setup-iteration.R

#source do-solutions.env
DO_ALL=true
DO_DASK=$DO_ALL
DO_DATATABLE=$DO_ALL
DO_DPLYR=$DO_ALL
DO_JULIADF=$DO_ALL
DO_MODIN=false
DO_PANDAS=$DO_ALL
DO_PYDATATABLE=$DO_ALL
DO_SPARK=$DO_ALL

# dask
$DO_DASK && ./dask/dask.sh

# datatable
$DO_DATATABLE && ./datatable/datatable.sh

# dplyr
$DO_DPLYR && ./dplyr/dplyr.sh

# juliadf
$DO_JULIADF && ./juliadf/juliadf.sh

# modin
$DO_MODIN && ./modin/modin.sh

# pandas
$DO_PANDAS && ./pandas/pandas.sh

# pydatatable
$DO_PYDATATABLE && ./pydatatable/pydatatable.sh

# spark
$DO_SPARK && ./spark/spark.sh

# publish report for all tasks
rm -rf public && Rscript -e 'rmarkdown::render("index.Rmd", output_dir="public")' > ./rmarkdown.out 2>&1 && echo "# Benchmark report produced"

# publish benchmark, only if token file exists
$DO_PUBLISH && [ -f ./token ] && ((./publish.sh && echo "# Benchmark results has been published") || echo "# Benchmark publish script failed")

# completed
echo "# Benchmark run $BATCH has been completed in $(($(date +%s)-$BATCH))s"
