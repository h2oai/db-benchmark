#!/bin/bash
set -e

## Join benchmark:
# spark
# impala
# data.table
# h2o

## Requirements:
# 1. passwordless ssh from mr-0xd6 to mr-0xd1:10
# 2. ensure you are not running h2o or spark cluster already, those will be started during script
# 3. run one-time setup-spark.sh to copy spark binaries to all nodes and create ~/tmp
# 4. run one-time setup-h2o.sh to copy h2o jar to all nodes
# 5. run one-time setup-impala.sh to copy impala client to mr-0xd6
# 6. execute from mr-0xd6 with ./join.sh
# 7. installed R pkgs: h2o, data.table, knitr
# 8. impala cluster should be already running, connection available on mr-0xd2-precise1
# 9. optionally adjust memory limits in join-spark.sh and join-h2o.sh
# 10. optionally set ports join-spark.sh and join-h2o.sh
# 11. chmod a+x join.sh join-h2o.sh join-impala.sh join-spark.sh join-data.table.R join-h2o.R
# 12. source filenames X1e7_2c.csv should start with single letter followed by 1e7 notation to catch row count correctly on which data.table is conditionally processed

export CSV_TIME_FILE="time.csv"
export SRC_X="hdfs://mr-0xd6/datasets/mattd/X1e8_2c.csv" # see req.12
export SRC_Y="hdfs://mr-0xd6/datasets/mattd/Y1e8_2c.csv"

# - [ ] Spark

./join-spark.sh
sleep 5

# - [ ] Impala

./join-impala.sh
sleep 5

# - [ ] data.table

./join-data.table.R
sleep 5

# - [ ] h2o

./join-h2o.sh

# - [ ] publish
Rscript -e 'knitr::knit2html("join.Rmd")'
