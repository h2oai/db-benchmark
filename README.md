Repository for reproducible benchmarking of database-like operations.  
Benchmark is mainly focused on portability and reproducibility, there was no *production tuning* made. Initial benchmark setup is meant to compare scalability both in data volume and data complexity.  

Tasks:
  - [x] join
  - [x] groupby
  - [x] sort
  - [x] read

Tools:
  - [x] data.table
  - [x] pandas
  - [x] dplyr
  - [x] pydatatable

Reproduce:  
- edit `run.conf` to define tasks to benchmark
- generate data: use `tableGen.c` for join/sort and `groupby-datagen.R` for groupby
- edit `data.csv` to define data sizes to benchmark
- start benchmark with `./run.sh`

Example environment setup:
- setting up r3-8xlarge: 244GB RAM, 32 cores: [Amazon EC2 for beginners](https://github.com/Rdatatable/data.table/wiki/Amazon-EC2-for-beginners)  
- full reproduce script on clean Ubuntu 16.04: [repro.sh](https://github.com/h2oai/db-benchmark/blob/master/repro.sh)  

---

Multi-node applications has been disabled, to re-enable them check [2ae9815](https://github.com/h2oai/db-benchmark/commit/2ae98156532641f57c092f7b9b74cf4a8d7e2ef8). Some of their scripts might require updates for recent changes.  

Currently disabled multinode tools:
  - [x] spark
  - [x] impala
  - [x] h2o
  - [x] dask
  - [x] presto

To reproduce:  
- setup tools on cluster nodes: use `[tool]/setup-[tool].sh` scripts
- edit `run.conf` to define tasks to benchmark and configure cluster
- generate data: use `tableGen.c` for join/sort and `groupby-datagen.R` for groupby
- edit `data.csv` to define data sizes to benchmark
- start benchmark with `./run.sh`
