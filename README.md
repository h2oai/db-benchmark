Tasks:
  - [x] join
  - [ ] groupby
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

A good way to run benchmark is run it in background so it can continue even if we disconnect: `nohup ./run.sh > run.log 2>&1 &`.  
Preview logs with `tail -f run.log` or `tail -f run.log | grep "^#"`for pretty print of processing and csv outcome.  
