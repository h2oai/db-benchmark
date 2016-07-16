#!/bin/bash
set -e

## Requirements:
# 1. run one-time setup-impala.sh to copy impala-shell binaries to mr-0xd6
# 2. SRC_X and SRC_Y env vars pointing to hdfs://mr-0xd6/

SRC_X_LOCAL=$(echo "$SRC_X" | cut -c 15-) # extract path for impala: /datasets/mattd
SRC_Y_LOCAL=$(echo "$SRC_Y" | cut -c 15-)
echo $SRC_X_LOCAL
echo $SRC_Y_LOCAL

# run
echo "Running impala benchmark..."
$HOME/impala/bin/impala-shell -i mr-0xd2-precise1 --var=SRC_X_DIR=${SRC_X_LOCAL%.csv} --var=SRC_Y_DIR=${SRC_Y_LOCAL%.csv} -B --output_delimiter ',' -V -f join-impala.sql &> join-impala.log
cat join-impala.log
# parse sql log and write timing
./join-impala-write.log.R "join-impala.log"
