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
if [[ "$DO_UPGRADE" == true && "$RUN_SOLUTIONS" =~ "dask" ]]; then ./dask/init-dask.sh; fi;
if [[ "$DO_UPGRADE" == true && "$RUN_SOLUTIONS" =~ "data.table" ]]; then ./datatable/init-datatable.sh; fi;
if [[ "$DO_UPGRADE" == true && "$RUN_SOLUTIONS" =~ "dplyr" ]]; then ./dplyr/init-dplyr.sh; fi;
if [[ "$DO_UPGRADE" == true && "$RUN_SOLUTIONS" =~ "juliadf" ]]; then ./juliadf/init-juliadf.sh; fi;
if [[ "$DO_UPGRADE" == true && "$RUN_SOLUTIONS" =~ "pandas" ]]; then ./pandas/init-pandas.sh; fi;
if [[ "$DO_UPGRADE" == true && "$RUN_SOLUTIONS" =~ "pydatatable" ]]; then ./pydatatable/init-pydatatable.sh; fi;
if [[ "$DO_UPGRADE" == true && "$RUN_SOLUTIONS" =~ "spark" ]]; then ./spark/init-spark.sh; fi;

# run
Rscript ./launcher.R

# publish report for all tasks
#rm -rf public && Rscript -e 'rmarkdown::render("index.Rmd", output_dir="public")' > ./rmarkdown.out 2>&1 && echo "# Benchmark report produced"

# publish benchmark, only if token file exists
$DO_PUBLISH && [ -f ./token ] && ((./publish.sh && echo "# Benchmark results has been published") || echo "# Benchmark publish script failed")

# completed
echo "# Benchmark run $BATCH has been completed in $(($(date +%s)-$BATCH))s"
