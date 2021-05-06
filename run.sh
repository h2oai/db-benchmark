#!/bin/bash
set -e

## run script the following way to exit if benchmark is already running
#if [[ -f ./run.lock ]]; then echo "# Benchmark run discarded due to previous run $(cat run.lock) still running" > "./run_discarded_at_$(date +%s).out"; else ./run.sh > ./run.out; fi;

# set batch
export BATCH=$(date +%s)

# low level log of every script run
echo $BATCH >> ./batch.log

# confirm stop flag disabled
if [[ -f ./stop ]]; then echo "# Benchmark run $BATCH aborted. 'stop' file exists, should be removed before calling 'run.sh'" && exit; fi;

# confirm java is not running
pidof java > /dev/null 2>&1 && echo "# Benchmark run $BATCH aborted. java is running, shut it down before calling 'run.sh'" && exit;

# confirm clickhouse is not running
source ./clickhouse/ch.sh
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

# upgrade tools and VERSION, REVISION metadata files
$DO_UPGRADE && echo "# Upgrading solutions"
if [[ "$DO_UPGRADE" == true && "$RUN_SOLUTIONS" =~ "dask" ]]; then ./dask/upg-dask.sh; fi;
if [[ "$RUN_SOLUTIONS" =~ "dask" ]]; then ./dask/ver-dask.sh; fi;
if [[ "$DO_UPGRADE" == true && "$RUN_SOLUTIONS" =~ "data.table" ]]; then ./datatable/upg-datatable.sh; fi;
if [[ "$RUN_SOLUTIONS" =~ "data.table" ]]; then ./datatable/ver-datatable.sh; fi;
if [[ "$DO_UPGRADE" == true && "$RUN_SOLUTIONS" =~ "dplyr" ]]; then ./dplyr/upg-dplyr.sh; fi;
if [[ "$RUN_SOLUTIONS" =~ "dplyr" ]]; then ./dplyr/ver-dplyr.sh; fi;
if [[ "$DO_UPGRADE" == true && "$RUN_SOLUTIONS" =~ "juliadf" ]]; then ./juliadf/upg-juliadf.sh; fi;
if [[ "$RUN_SOLUTIONS" =~ "juliadf" ]]; then ./juliadf/ver-juliadf.sh; fi;
if [[ "$DO_UPGRADE" == true && "$RUN_SOLUTIONS" =~ "modin" ]]; then ./modin/upg-modin.sh; fi;
if [[ "$RUN_SOLUTIONS" =~ "modin" ]]; then ./modin/ver-modin.sh; fi;
if [[ "$DO_UPGRADE" == true && "$RUN_SOLUTIONS" =~ "pandas" ]]; then ./pandas/upg-pandas.sh; fi;
if [[ "$RUN_SOLUTIONS" =~ "pandas" ]]; then ./pandas/ver-pandas.sh; fi;
if [[ "$DO_UPGRADE" == true && "$RUN_SOLUTIONS" =~ "pydatatable" ]]; then ./pydatatable/upg-pydatatable.sh; fi;
if [[ "$RUN_SOLUTIONS" =~ "pydatatable" ]]; then ./pydatatable/ver-pydatatable.sh; fi;
if [[ "$DO_UPGRADE" == true && "$RUN_SOLUTIONS" =~ "spark" ]]; then ./spark/upg-spark.sh; fi;
if [[ "$RUN_SOLUTIONS" =~ "spark" ]]; then ./spark/ver-spark.sh; fi;
#if [[ "$DO_UPGRADE" == true && "$RUN_SOLUTIONS" =~ "cudf" ]]; then ./cudf/upg-cudf.sh; fi; # upgrade manually as full re-install
if [[ "$RUN_SOLUTIONS" =~ "cudf" ]]; then ./cudf/ver-cudf.sh; fi;
#if [[ "$DO_UPGRADE" == true && "$RUN_SOLUTIONS" =~ "clickhouse" ]]; then ./clickhouse/upg-clickhouse.sh; fi; # manual as requires sudo: apt-get install --only-upgrade clickhouse-server clickhouse-client
if [[ "$RUN_SOLUTIONS" =~ "clickhouse" ]]; then ./clickhouse/ver-clickhouse.sh; fi;
if [[ "$DO_UPGRADE" == true && "$RUN_SOLUTIONS" =~ "h2o" ]]; then ./h2o/upg-h2o.sh; fi;
if [[ "$RUN_SOLUTIONS" =~ "h2o" ]]; then ./h2o/ver-h2o.sh; fi;
if [[ "$DO_UPGRADE" == true && "$RUN_SOLUTIONS" =~ "polars" ]]; then ./polars/upg-polars.sh; fi;
if [[ "$RUN_SOLUTIONS" =~ "polars" ]]; then ./polars/ver-polars.sh; fi;
if [[ "$DO_UPGRADE" == true && "$RUN_SOLUTIONS" =~ "arrow" ]]; then ./arrow/upg-arrow.sh; fi;
if [[ "$RUN_SOLUTIONS" =~ "arrow" ]]; then ./arrow/ver-arrow.sh; fi;
if [[ "$DO_UPGRADE" == true && "$RUN_SOLUTIONS" =~ "duckdb" ]]; then ./duckdb/upg-duckdb.sh; fi;
if [[ "$RUN_SOLUTIONS" =~ "duckdb" ]]; then ./duckdb/ver-duckdb.sh; fi;

# run
if [[ -f ./stop ]]; then echo "# Benchmark run $BATCH has been interrupted after $(($(date +%s)-$BATCH))s due to 'stop' file" && rm -f ./stop && rm -f ./run.lock && exit; fi;
echo "# Running benchmark scripts launcher"
Rscript ./_launcher/launch.R
if [[ -f ./stop ]]; then echo "# Benchmark run $BATCH has been interrupted after $(($(date +%s)-$BATCH))s due to 'stop' file" && rm -f ./stop && rm -f ./run.lock && exit; fi;

# publish report for all tasks
rm -rf ./public
rm -f ./report-done
$DO_REPORT && echo "# Rendering report"
$DO_REPORT && Rscript -e 'rmarkdown::render("./_report/index.Rmd", output_dir="public")' > ./out/rmarkdown_index.out 2>&1 && echo "# Benchmark index report produced"
$DO_REPORT && Rscript -e 'rmarkdown::render("./_report/history.Rmd", output_dir="public")' > ./out/rmarkdown_history.out 2>&1 && echo "# Benchmark history report produced"
$DO_REPORT && Rscript -e 'rmarkdown::render("./_report/tech.Rmd", output_dir="public")' > ./out/rmarkdown_tech.out 2>&1 && echo "# Benchmark tech report produced"

# publish benchmark, only if all reports successfully generated (logged in ./report-done file), and token file exists
if [[ -f ./stop ]]; then echo "# Benchmark run $BATCH has been interrupted after $(($(date +%s)-$BATCH))s due to 'stop' file" && rm -f ./stop && rm -f ./run.lock && exit; fi;
rm -rf ./db-benchmark.gh-pages
$DO_REPORT && $DO_PUBLISH \
  && [ -f ./report-done ] \
  && [ $(wc -l report-done | awk '{print $1}') -eq 3 ] \
  && [ -f ./token ] \
  && echo "# Publishing report" \
  && ((./_report/publish.sh && echo "# Benchmark results has been published") || echo "# Benchmark publish script failed")

# remove run lock file
rm -f ./run.lock

# completed
echo "# Benchmark run $BATCH has been completed in $(($(date +%s)-$BATCH))s"
