#!/bin/bash
set -e

## run script the following way to exit if benchmark is already running
#if [[ -f ./run.lock ]]; then echo "# Benchmark run discarded due to previous run $(cat run.lock) still running" > "./run_discarded_at_$(date +%s).out"; else ./run.sh > ./run.out; fi;

# set batch
export BATCH=$(date +%s)

# confirm stop flag disabled
if [[ -f ./stop ]]; then echo "# Benchmark run $BATCH aborted. 'stop' file exists, should be removed before calling 'run.sh'" && exit; fi;

# confirm clickhouse is not running
source ./ch.sh
ch_installed && ch_active && echo "# Benchmark run $BATCH aborted. clickhouse-server is running, shut it down before calling 'run.sh'" && exit;

# confirm swap disabled
Rscript -e 'swap_all<-data.table::fread("free -h | grep Swap", header=FALSE)[, -1L][, as.numeric(gsub("[^0-9.]", "", unlist(.SD)))]; swap_off<-!is.na(s<-sum(swap_all)) && s==0; q("no", status=as.numeric(swap_off))' && echo "# Benchmark run $BATCH aborted. swap is enabled, 'free -h' has to report only 0s for Swap, run 'swapoff -a' before calling 'run.sh'" && exit;

# ensure directories exists
mkdir -p ./out
if [[ ! -d ./data ]]; then echo "# Benchmark run $BATCH aborted. './data' directory does not exists" && exit; fi;

# set lock
if [[ -f ./run.lock ]]; then echo "# Benchmark run $BATCH aborted. 'run.lock' file exists, this should be checked before calling 'run.sh'. Ouput redirection mismatch might have happened if writing output to same file as currently running $(cat ./run.lock) benchmark run" && exit; else echo $BATCH > run.lock; fi;

echo "# Benchmark run $BATCH started"

# get config
source ./run.conf
source ./path.env

# upgrade tools
if [[ "$DO_UPGRADE" == true && "$RUN_SOLUTIONS" =~ "dask" ]]; then ./dask/init-dask.sh; fi;
if [[ "$DO_UPGRADE" == true && "$RUN_SOLUTIONS" =~ "data.table" ]]; then ./datatable/init-datatable.sh; fi;
if [[ "$DO_UPGRADE" == true && "$RUN_SOLUTIONS" =~ "dplyr" ]]; then ./dplyr/init-dplyr.sh; fi;
if [[ "$DO_UPGRADE" == true && "$RUN_SOLUTIONS" =~ "juliadf" ]]; then ./juliadf/init-juliadf.sh; fi;
if [[ "$DO_UPGRADE" == true && "$RUN_SOLUTIONS" =~ "modin" ]]; then ./modin/init-modin.sh; fi;
if [[ "$DO_UPGRADE" == true && "$RUN_SOLUTIONS" =~ "pandas" ]]; then ./pandas/init-pandas.sh; fi;
if [[ "$DO_UPGRADE" == true && "$RUN_SOLUTIONS" =~ "pydatatable" ]]; then ./pydatatable/init-pydatatable.sh; fi;
if [[ "$DO_UPGRADE" == true && "$RUN_SOLUTIONS" =~ "spark" ]]; then ./spark/init-spark.sh; fi;
#if [[ "$DO_UPGRADE" == true && "$RUN_SOLUTIONS" =~ "clickhouse" ]]; then ./clickhouse/init-clickhouse.sh; fi; # manual as requires sudo: apt-get install --only-upgrade clickhouse-server clickhouse-client

# produce VERSION, REVISION files for each solution
set +e
./versions.sh
if [[ $? -ne 0 ]]; then echo "# Benchmark run $BATCH failed to check versions of currently installed solutions" && rm -f ./run.lock && exit; fi;
set -e

# run
Rscript ./launcher.R
if [[ -f ./stop ]]; then echo "# Benchmark run $BATCH has been interrupted after $(($(date +%s)-$BATCH))s due to 'stop' file" && rm -f ./stop && rm -f ./run.lock && exit; fi;

# publish report for all tasks
rm -rf ./public
rm -f ./report-done
$DO_REPORT && Rscript -e 'rmarkdown::render("index.Rmd", output_dir="public")' > ./out/rmarkdown_index.out 2>&1 && echo "# Benchmark index report produced"
$DO_REPORT && Rscript -e 'rmarkdown::render("groupby.Rmd", output_dir="public")' > ./out/rmarkdown_groupby.out 2>&1 && echo "# Benchmark groupby report produced"
$DO_REPORT && Rscript -e 'rmarkdown::render("history.Rmd", output_dir="public")' > ./out/rmarkdown_history.out 2>&1 && echo "# Benchmark history report produced"
$DO_REPORT && Rscript -e 'rmarkdown::render("tech.Rmd", output_dir="public")' > ./out/rmarkdown_tech.out 2>&1 && echo "# Benchmark tech report produced"

# publish benchmark, only if all reports successfully generated (logged in ./report-done file), and token file exists
rm -rf ./db-benchmark.gh-pages
$DO_REPORT && $DO_PUBLISH \
  && [ -f ./report-done ] \
  && [ $(wc -l report-done | awk '{print $1}') -eq 4 ] \
  && [ -f ./token ] \
  && ((./publish.sh && echo "# Benchmark results has been published") || echo "# Benchmark publish script failed")

# remove run lock file
rm -f ./run.lock

# completed
echo "# Benchmark run $BATCH has been completed in $(($(date +%s)-$BATCH))s"
