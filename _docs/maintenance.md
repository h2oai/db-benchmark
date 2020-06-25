
# db-benchmark maintenance manual

For initial setup of benchmark project see [README.md](../README.md).

## initial steps after (re)boot

Ensure that `clickhouse-sever` and `docker` are not running. Also check that swap is disabled.

```sh
## should return "Cannot connect... Is the docker daemon running?"
sudo docker ps
#sudo systemctl stop docker

## should return code 210
clickhouse-client --query="SELECT 0;"
#sudo systemctl stop clickhouse-server

## should report 0B Swap memory
free -h
#sudo swapoff -a
```

You should additionally ensure no other unneeded processes are running, as they may consume resources and interfere benchmark scripts.

## configuration

### `run.conf` file

`RUN_TASKS`, `RUN_SOLUTIONS` controls which tasks and solutions should be run.

`DO_UPGRADE` - if for some reason you want to repeat last benchmark using exactly the same version of software you should set this to `false` no upgrades of software will be made.
`FORCE_RUN` - force run benchmark tells the launcher script to ignore the fact that solution and its version might have been already run on particular task and data, in such a case it would normally be skipped.
`MOCKUP` - this can be used to just print to console benchmarks scripts that would run. Note that when using `MOCKUP` run logs are not written.

`DO_REPORT` - should Rmarkdown benchmark reports be rendered into html.
`DO_PUBLISH` - should benchmark report html files be published, setting `DO_REPORT=false` disables publish as well.

### `_control` directory

#### `data.csv` file

Defines a list of tasks and data cases to run, use `active` field to disable any if needed. `nrow`, `k`, `na`, `sort` are only descriptions derived from `data` field.

#### `nodenames.csv`

If moving benchmark project to new machine, hostname entry of that machine should be added to this file so machine specification can be included in the report.

#### `questions.csv`

File defines dictionary of questions, additionally grouping them into `question_group`. When implementing new questions, entries should be added to this file.

#### `solutions.csv`

File defines dictionary of solutions and tasks they can compute. When implementing new solutions or tasks, entries should be added to this file.

#### `timeout.csv`

File defines timeout values in minutes for tasks and data sizes. When implementing new task or new data size, entries should be added to this file. Benchmark script that exceeds the timeout values defined in this file are interrupted.

## running

### single solution, task and data

See ["Single solution benchmark" section in `README.md`](../README.md#single-solution-benchmark). Note that running single solution should only be used for debugging. It will not populate entries into `logs.csv` and `time.csv` files. Additionally it will not switch to required virtual environments, do upgrades of software, or check if current version was already tested.

### full benchmarks

Command to run full benchmark script can be found at the top `run.sh` file (`head run.sh`).

```sh
if [[ -f ./run.lock ]]; then echo "# Benchmark run discarded due to previous run $(cat run.lock) still running" > "./run_discarded_at_$(date +%s).out"; else ./run.sh > ./run.out; fi;
```

The command looks complex because we want to escape running benchmark if one is already running. `run.lock` file is created at the start of the benchmark and removed at the end, so we first check if it exists, and then if it doesn't we start `run.sh` script.

#### schedule benchmark

Because of `run.lock` machanism benchmark script can be easily scheduled in crontab. To schedule to run every 10 days the following entry can be used.

```cron
0 */240 * * * cd ~/git/db-benchmark && if [ -f ./run.lock ]; then echo "# Benchmark run discarded due to previous run $(cat run.lock) still running" > "./run_discarded_at_$(date +\%s).out"; else ./run.sh > ./run.out 2>&1; fi;
```

Command used in cron has to be posix shell, not bash, and percent signs has to be escaped with backslashes.

Note that the user that executes the job has to have privileges defined, extra care is needed to setup sudo-less access for starting and shutting down ClickHouse server, see [`clickhouse/setup-clickhouse.sh`](../clickhouse/setup-clickhouse.sh) for details.

#### exceptions

In case of unlikely event of an exception of the main process (`CTRL+C` to interrupt, etc.) the `run.lock` file will not be removed, thus manual removal of that file is needed. Additionally if exception occured during Spark or ClickHouse scripts, those platforms may require to shutdown them manually `spark.stop()` or `sudo /usr/sbin/service clickhouse-server stop`.

If you want to gracefully terminate benchmark, rather than `CTRL+C`, you can just create `stop` file in db-benchmark directory.

```sh
touch stop
```

In such case current benchmark script will continue to be executed, but before starting the next script, the check for `stop` file will recognize you want to terminate benchmark and it will exit, not executing all remaining scripts. `stop` file will be automatically removed when benchmark launcher will exit, so you don't have to manually remove it afterwards.

If you want to just pause benchmark, then create `pause` file.

```sh
touch pause
```

Similarly as for `stop` file, the execution of current script will continue, but any further scripts will be waiting till the `pause` file be removed.

## reading console output logs

### out files

#### `run.out` main benchmark process output

Assuming that above instruction on running full benchmark run has been followed, the `run.sh` process output will be in `run.out` file.

```sh
more run.out
tail -f run.out
```

#### `out/run_*.(out|err)` solution-task scripts output

Output of all benchmark scripts is stored in `out` directory, and prefixed with `run_*`. Logs are stored in `out` files and `err` files corresponding to data streams.

```sh
ls out/run_*.out
ls out/run_*.err
ls -l out/run_*.err | awk '{if ($5 != 0) print $9}' ## non-empty err files

more out/run_datatable_groupby_G1_1e9_2e0_0_0.out
more out/run_datatable_groupby_G1_1e9_2e0_0_0.err
```

#### `out/rmarkdown_*.out` reports rendering output

```sh
ls out/rmarkdown_*.out
more out/rmarkdown_index.out
```

## reading csv logs and timings

Some common fields

`batch` - integer timestamp of the benchmark run starting time
`data` - file name, with no extension, of the source data for benchmark scripts; encodes information like data size, cardinality factor, NAs fraction and sortedness

### `logs.csv`

File is used to record attempts of running script. If it happens that script fails already during data loading step, then there will be no timing measures written to `time.csv`, yet still we will see entry in `logs.csv`.

- `action` field
  - `start` - script start event
  - `finish` - script finish event
  - `skip` - indicates that there was a attempt to run script, but due to the fact that same version of solution has been already tested on particular task and data, the run attempt has been skipped.
- `stderr` counts the number of lines of `run_*.err` STDERR log file.
- `ret` field is a value returned from benchmark script, some commonly observed:
  - `0` - successfully completed
  - `1` - (virtually any) error, including memory errors as well
  - `134` - `SIGABRT`, happened for `cudf` OOM (GPU mem)
  - `137` - `SIGKILL`, happened due to OOM or timeout

New exit codes can be examined using and [this reference doc](http://tldp.org/LDP/abs/html/exitcodes.html):
```sh
R -q -e 'data.table::fread("logs.csv")[ret>1L, .N, ,.(ret,solution,task,data)]'
```

### `time.csv`

- fields used for validation for results from resolved questions:
  - `out_rows` - number of rows of the answer
  - `out_cols` - number of columns of the answer
  - `chk` - check sum of all measures, pasted together
  - `chk_time_sec` - time taken to compute the `chk` field, used to track lazyness of evaluation
- `fun` - function name from a solution that was used to compute the answer, haven't yet used multiple functions for same question
- `run` - indicates the query run, as of now we run each query twice
- `time_sec` - query time in seconds
- `mem_gb` - memory used in GB, not reliable as of now, should not be used for reporting
- `cache` - some solutions (as of now `clickhouse`) doesn't have ability to keep query result in cache/variable (thus `cache=FALSE`) - to workaround this limitation `CREATE TABLE AS SELECT` could be used instead of just `SELECT`, in such case `cache=TRUE` should be set
- `on_disk` - indicates if data storage was _in-memory_ or _on-disk_, note that for `cudf` the default in-memory storage is actually and in-gpu-memory, and on-disk could be in-memory and on_disk (but not in-gpu-memory)

## reports

Reports consumes `logs.csv` and `time.csv` to produce meaningful insight into timings measures. To prepare data for reports there are multiple functions defined in `_report/report.R`. They also do data validation, like checking numbers rows and columns on output matches between solutions, etc. In case of issues at this stage one can be easily run those functions directly in R.
```r
source("_report/report.R")
d = time_logs()
```
Those validation checks, as well as some other exceptions that could happen during benchmark scripts, may cause rendring reports to fail. If rendering a reports fails, it will most likely require manual intervention.
There are currently 3 reports:

- `index.Rmd` - main page report - [index.html](https://h2oai.github.io/db-benchmark)
- `history.Rmd` - performance regression tracking - [history.html](https://h2oai.github.io/db-benchmark/history.html)
- `tech.Rmd` - technical stats about whole scripts execution times, failures - [tech.html](https://h2oai.github.io/db-benchmark/tech.html)

## publish

We use GitHub pages for publishing static html pages. `token` file is expected to be present in project root directory. Publish process is handled by `_report/publish.sh` script. It `git reset --hard` the `gh-pages` branch to initial state, put new files there, commit and push to publish. Note that `logs.csv` and `time.csv` files (and their md5sum) are published as well, so users can access full history of benchmark runs.

## support

```
jan.gorecki
@
h2o.ai
```
