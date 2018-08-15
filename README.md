Repository for reproducible benchmarking of database-like operations.  
Benchmark is mainly focused on portability and reproducibility. Initial benchmark setup is meant to compare scalability both in data volume and data complexity.  

Tasks:
  - [x] join (modin, pydatatable not yet implemented)
  - [x] groupby (modin not yet implemented)
  - [x] sort
  - [x] read

Tools:
  - [x] data.table
  - [x] pandas
  - [x] dplyr
  - [x] pydatatable
  - [ ] modin (Pandas on Ray)

Reproduce:  
- edit `run.conf` to define tasks to benchmark
- generate data: use `tableGen.c` for join/sort and `groupby-datagen.R` for groupby
- edit `data.csv` to define data sizes to benchmark
- start benchmark with `./run.sh`

Example environment setup:
- setting up r3-8xlarge: 244GB RAM, 32 cores: [Amazon EC2 for beginners](https://github.com/Rdatatable/data.table/wiki/Amazon-EC2-for-beginners)  
- full reproduce script on clean Ubuntu 16.04: [repro.sh](https://github.com/h2oai/db-benchmark/blob/master/repro.sh)  

---

Multi-node applications has been disabled, to re-enable them revert to [2ae9815](https://github.com/h2oai/db-benchmark/commit/2ae98156532641f57c092f7b9b74cf4a8d7e2ef8).  

Currently disabled multinode tools:
  - [x] spark
  - [x] impala
  - [x] h2o
  - [x] dask
  - [x] presto
