#!/bin/bash
set -e

# get config
source run.conf

# cleanup
rm -f *.png
rm -f *.env
rm -f rmarkdown.out
rm -rf db-benchmark.gh-pages

# produce iteration dictionaries from data.csv
./init-setup-iteration.R

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

# dask
./dask/dask.sh

# datatable
./datatable/datatable.sh

# dplyr
./dplyr/dplyr.sh

# juliadf
./juliadf/juliadf.sh

# pandas
./pandas/pandas.sh

# pydatatable
./pydatatable/pydatatable.sh

## modin
#./modin/modin.sh

# spark
./spark/spark.sh

# publish report for all tasks
Rscript -e 'rmarkdown::render("index.Rmd")' > ./rmarkdown.out 2>&1 && echo "# Benchmark report produced"

# publish benchmark, only if token file exists
$DO_PUBLISH && [ -f ./token ] && ./publish.sh && echo "# Benchmark results has been published"

# completed
echo "# Benchmark run $BATCH has been completed in $(($(date +%s)-$BATCH))s"
