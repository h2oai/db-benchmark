#!/bin/bash
set -e

## run script the following way to exit if benchmark is already running
#if [[ -f ./run.lock ]]; then echo "# Benchmark run discarded due to previous run $(cat run.lock) still running" > "./run_discarded_at_$(date +%s).out"; else ./run.sh > ./run.out; fi;

# set batch
export BATCH=$(date +%s)

# set lock
if [[ -f ./run.lock ]]; then echo "# Benchmark run $BATCH aborted. 'run.lock' file exists, this should be checked before calling 'run.sh'. Ouput redirection mismatch might have happened if writing output to same file as currently running $(cat ./run.lock) benchmark run" && exit; else echo $BATCH > run.lock; fi;

echo "# Benchmark run $BATCH started"

# get config
source run.conf
source path.env

# upgrade tools
if [[ "$DO_UPGRADE" == true && "$RUN_SOLUTIONS" =~ "dask" ]]; then ./dask/init-dask.sh; fi;
if [[ "$DO_UPGRADE" == true && "$RUN_SOLUTIONS" =~ "data.table" ]]; then ./datatable/init-datatable.sh; fi;
if [[ "$DO_UPGRADE" == true && "$RUN_SOLUTIONS" =~ "dplyr" ]]; then ./dplyr/init-dplyr.sh; fi;
if [[ "$DO_UPGRADE" == true && "$RUN_SOLUTIONS" =~ "juliadf" ]]; then ./juliadf/init-juliadf.sh; fi;
if [[ "$DO_UPGRADE" == true && "$RUN_SOLUTIONS" =~ "modin" ]]; then ./modin/init-modin.sh; fi;
if [[ "$DO_UPGRADE" == true && "$RUN_SOLUTIONS" =~ "pandas" ]]; then ./pandas/init-pandas.sh; fi;
if [[ "$DO_UPGRADE" == true && "$RUN_SOLUTIONS" =~ "pydatatable" ]]; then ./pydatatable/init-pydatatable.sh; fi;
if [[ "$DO_UPGRADE" == true && "$RUN_SOLUTIONS" =~ "spark" ]]; then ./spark/init-spark.sh; fi;

# produce VERSION, REVISION files for each solution
./versions.sh

# run
Rscript ./launcher.R

# publish report for all tasks
rm -f rmarkdown-index.out rmarkdown-tech.out rmarkdown-history.out
rm -rf public
rm -f report-done
Rscript -e 'rmarkdown::render("index.Rmd", output_dir="public")' > ./rmarkdown-index.out 2>&1 && echo "# Benchmark index report produced"
Rscript -e 'rmarkdown::render("history.Rmd", output_dir="public")' > ./rmarkdown-history.out 2>&1 && echo "# Benchmark history report produced"
Rscript -e 'rmarkdown::render("tech.Rmd", output_dir="public")' > ./rmarkdown-tech.out 2>&1 && echo "# Benchmark tech report produced"

# publish benchmark, only if reports successfully generated (groupby, tech), token file exists
rm -rf db-benchmark.gh-pages
$DO_PUBLISH \
  && [ -f ./report-done ] \
  && [ $(wc -l report-done | awk '{print $1}') -eq 3 ] \
  && [ -f ./token ] \
  && ((./publish.sh && echo "# Benchmark results has been published") || echo "# Benchmark publish script failed")

# remove run lock file
rm -f run.lock

# completed
echo "# Benchmark run $BATCH has been completed in $(($(date +%s)-$BATCH))s"
