Repository for reproducible benchmarking of database-like operations in single-node environment.  
Benchmark report is available at [h2oai.github.io/db-benchmark](https://h2oai.github.io/db-benchmark).  
We focused mainly on portability and reproducibility. Benchmark is routinely re-run to present up-to-date timings. Most of solutions used are automatically upgraded to their stable or development versions.  
This benchmark is meant to compare scalability both in data volume and data complexity.  
Contribution and feedback are very welcome!  

# Tasks

  - [x] groupby
  - [x] join
  - [ ] sort
  - [ ] read

# Solutions

  - [x] [dask](https://github.com/dask/dask)
  - [x] [data.table](https://github.com/Rdatatable/data.table)
  - [x] [dplyr](https://github.com/tidyverse/dplyr)
  - [x] [DataFrames.jl](https://github.com/JuliaData/DataFrames.jl)
  - [x] [pandas](https://github.com/pandas-dev/pandas)
  - [x] [(py)datatable](https://github.com/h2oai/datatable)
  - [x] [spark](https://github.com/apache/spark)
  - [x] [cuDF](https://github.com/rapidsai/cudf)
  - [x] [ClickHouse](https://github.com/yandex/ClickHouse) (`join` not yet added)

More solutions has been proposed. Some of them are not yet mature enough to address benchmark questions well enough (e.g. [modin](https://github.com/h2oai/db-benchmark/issues/38)). Others haven't been yet evaluated or implemented. Status of all can be tracked in dedicated [issues labelled as _new solution_](https://github.com/h2oai/db-benchmark/issues?q=is%3Aissue+is%3Aopen+label%3A%22new+solution%22) in project repository.

# Reproduce

## Batch benchmark run

- edit `path.env` and set `julia` and `java` paths
- if solution uses python create new `virtualenv` as `$solution/py-$solution`, example for `pandas` use `virtualenv pandas/py-pandas --python=/usr/bin/python3.6`
- install every solution (if needed activate each `virtualenv`)
- edit `run.conf` to define solutions and tasks to benchmark
- generate data, for `groupby` use `Rscript _data/groupby-datagen.R 1e7 1e2 0 0` to create `G1_1e7_1e2_0_0.csv`, re-save to binary data where needed, create `data` directory and keep all data files there
- edit `_control/data.csv` to define data sizes to benchmark using `active` flag
- start benchmark with `./run.sh`

## Single solution benchmark interactively

- generate data using `_data/*-datagen.R` scripts and put data files in `data` directory
- if solution uses python activate `virtualenv` or conda environment
- if solution uses R ensure that library is installed in a solution subdirectory, so that `library("dplyr", lib.loc="./dplyr/r-dplyr")` or `library("data.table", lib.loc="./datatable/r-datatable")` works
- run benchmark for a single solution using `./_launcher/solution.R --solution=data.table --task=groupby --nrow=1e7`
- run other data cases by passing extra parameters `--k=1e2 --na=0 --sort=0`
- use `--quiet=true` to suppress script's output and prints timings only, using `--print=question,run,time_sec` specify column to be printed to console, to print all use `--print=*`
- use `--out=time.csv` to write timings to file rather than console

## Extra care needed

- `cuDF`
  - use `conda` instead of `virtualenv`
- `ClickHouse`
  - generate data having extra primary key column according to `clickhouse/setup-clickhouse.sh`
  - follow "reproduce interactive environment" section from `clickhouse/setup-clickhouse.sh`

# Example environment

- setting up r3-8xlarge: 244GB RAM, 32 cores: [Amazon EC2 for beginners](https://github.com/Rdatatable/data.table/wiki/Amazon-EC2-for-beginners)  
- (slightly outdated) full reproduce script on clean Ubuntu 16.04: [_utils/repro.sh](https://github.com/h2oai/db-benchmark/blob/master/_utils/repro.sh)

# Acknowledgment

Timings for some solutions might be missing for particular data sizes or questions. Some functions are not yet implemented in all solutions so we were unable to answer all questions in all solutions. Some solutions might also run out of memory when running benchmark script which results the process to be killed by OS. Lastly we also added timeout for single benchmark script to run, once timeout value is reached script is terminated.
Please check [issues labelled as _exceptions_](https://github.com/h2oai/db-benchmark/issues?q=is%3Aissue+is%3Aopen+label%3Aexceptions) in our repository for a list of issues/defects in solutions, that makes us unable to provide all timings.
