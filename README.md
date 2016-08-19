Tasks:
  - [x] join
  - [ ] groupby
  - [ ] sort
  - [ ] import
  - [ ] subset

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
