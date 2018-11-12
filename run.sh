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

# get solutions to run env vars
source do-solutions.env

# dask
if [ "$DO_DASK" == true ]; then ./dask/dask.sh; fi;

# datatable
if [ "$DO_DATATABLE" == true ]; then ./datatable/datatable.sh; fi;

# dplyr
if [ "$DO_DPLYR" == true ]; then ./dplyr/dplyr.sh; fi;

# juliadf
if [ "$DO_JULIADF" == true ]; then ./juliadf/juliadf.sh; fi;

# modin
if [ "$DO_MODIN" == true ]; then ./modin/modin.sh; fi;

# pandas
if [ "$DO_PANDAS" == true ]; then ./pandas/pandas.sh; fi;

# pydatatable
if [ "$DO_PYDATATABLE" == true ]; then ./pydatatable/pydatatable.sh; fi;

# spark
if [ "$DO_SPARK" == true ]; then ./spark/spark.sh; fi;

# publish report for all tasks
rm -rf public && Rscript -e 'rmarkdown::render("index.Rmd", output_dir="public")' > ./rmarkdown.out 2>&1 && echo "# Benchmark report produced"

# publish benchmark, only if token file exists
$DO_PUBLISH && [ -f ./token ] && ((./publish.sh && echo "# Benchmark results has been published") || echo "# Benchmark publish script failed")

# completed
echo "# Benchmark run $BATCH has been completed in $(($(date +%s)-$BATCH))s"
