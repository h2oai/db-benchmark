Repository for reproducible benchmarking of database-like operations.  
Benchmark is mainly focused on portability and reproducibility, there was no *production tuning* made. Initial benchmark setup is meant to compare scalability both in data volume and data complexity.
Multi-node applications has been disabled, to re-enable them check git history for child commit of `21b1b367a587e6281ca86d3ccba5348a4a11213f`.  

Tasks:
  - [x] join
  - [x] groupby
  - [x] sort

Tools:
  - [_] spark
  - [_] impala
  - [x] data.table
  - [_] h2o
  - [x] pandas
  - [_] dask
  - [x] dplyr
  - [_] presto

To reproduce:  
- setup tools on cluster nodes: use `[tool]/setup-[tool].sh` scripts
- generate data: use `tableGen.c` for join and `groupby-datagen.R` for groupby
- edit `run.conf`
- edit `data.csv`
- start benchmark with `./run.sh`
