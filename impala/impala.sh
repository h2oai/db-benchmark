#!/bin/bash
set -e

# join
echo "Running impala join benchmark..."
$IMPALA_HOME/bin/impala-shell -i $IMPALA_MASTER --var=SRC_X_DIR=${SRC_X_LOCAL%.csv} --var=SRC_Y_DIR=${SRC_Y_LOCAL%.csv} -B --output_delimiter ',' -V -f ./impala/join-impala.sql &> ./impala/join-impala.log
cat ./impala/join-impala.log
./impala/join-impala-write.log.R "./impala/join-impala.log"
