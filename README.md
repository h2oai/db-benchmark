Repository for reproducible benchmarking of database-like operations.  
Benchmark is mainly focused on portability and reproducibility. This benchmark is meant to compare scalability both in data volume and data complexity.  

# Tasks

  - [x] groupby
  - [ ] join
  - [ ] sort
  - [ ] read

# Solutions

  - [x] [data.table](https://github.com/Rdatatable/data.table)
  - [x] [dplyr](https://github.com/tidyverse/dplyr)
  - [x] [pandas](https://github.com/pandas-dev/pandas)
  - [x] [(py)datatable](https://github.com/h2oai/datatable)
  - [x] [spark](https://github.com/apache/spark)
  - [x] [dask](https://github.com/dask/dask)
  - [ ] [modin](https://github.com/modin-project/modin) (not capable to _groupby_ yet)

# Reproduce

## all tasks and all solutions

- if solution uses python create new `virtualenv` as `[$solution]/py-$solution`, example for `pandas` use `virtualenv pandas/py-pandas --python=/usr/bin/python3.6`
- install every solution (if needed activate `virtualenv` each)
- edit `run.conf` to define tasks to benchmark
- generate data, for `groupby` use `Rscript groupby-datagen.R 1e7 1e2` to create `G1_1e7_1e2.csv`
- edit `data.csv` to define data sizes to benchmark
- start benchmark with `./run.sh`

## single task and single solution

- if solution uses python create new `virtualenv` as `[$solution]/py-$solution`, example for `pandas` use `virtualenv pandas/py-pandas --python=/usr/bin/python3.6`
- install solution (if needed activate `virtualenv`)
- generate data, for `groupby` use `Rscript groupby-datagen.R 1e7 1e2` to create `G1_1e7_1e2.csv`
- start single task and solution by `SRC_GRP_LOCAL=G1_1e7_1e2.csv ./pandas/groupby-pandas.py`

# Example environment

- setting up r3-8xlarge: 244GB RAM, 32 cores: [Amazon EC2 for beginners](https://github.com/Rdatatable/data.table/wiki/Amazon-EC2-for-beginners)  
- full reproduce script on clean Ubuntu 16.04: [repro.sh](https://github.com/h2oai/db-benchmark/blob/master/repro.sh)  

# Acknowledgment

- Solution [`modin`](https://github.com/modin-project/modin) is not capable to perform `groupby` task yet.  
- It might eventually happens that on the report `spark` will not have a date for its corresponding version. It is because of [SPARK-16864](https://issues.apache.org/jira/browse/SPARK-16864) "resolved" as "Won't Fix", thus we are unable to lookup that information from GitHub repo.  
- Solution [`dask`](https://github.com/dask/dask) is currently not presented on plot due to ["groupby aggregation does not scale well with amount of groups"](https://github.com/dask/dask/issues/4001), `groupby` scritpt is in place so anyone interested can run it already.
