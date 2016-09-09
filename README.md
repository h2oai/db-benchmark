Repository for reproducible benchmarking of database-like operations.  
Benchmark is mainly focused on portability and reproducibility, there was no *production tuning* made. Initial benchmark setup is meant to compare scalability both in data volume and data complexity.  

Tasks:
  - [x] join
  - [ ] groupby
  - [x] sort

Tools:
  - [x] spark
  - [x] impala
  - [x] data.table
  - [x] h2o
  - [x] pandas
  - [x] dask
  - [x] dplyr
  - [x] presto

To reproduce:  
- setup tools on cluster nodes: use `[tool]/setup-[tool].sh` scripts
- generate data: use `tableGen.c` for join and `groupby-datagen.R` for groupby
- edit `run.conf`
- edit `data.csv`
- start benchmark with `./run.sh`
