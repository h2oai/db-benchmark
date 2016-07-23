#!/bin/bash
set -e

# join
echo "Running impala join benchmark..."
$IMPALA_HOME/bin/impala-shell -i $IMPALA_MASTER --var=SRC_X_DIR=${SRC_X_LOCAL%.csv} --var=SRC_Y_DIR=${SRC_Y_LOCAL%.csv} -B --output_delimiter ',' -V -f ./impala/join-impala.sql &> ./impala/join-impala.log
cat ./impala/join-impala.log
./impala/impala-write.log.R "./impala/join-impala.log"

# groupby
echo "Running impala groupby benchmark..."
$IMPALA_HOME/bin/impala-shell -i $IMPALA_MASTER --var=SRC_GRP_DIR=${SRC_GRP_LOCAL%.csv} -B --output_delimiter ',' -V -f ./impala/groupby-impala.sql &> ./impala/groupby-impala.log
cat ./impala/groupby-impala.log
./impala/impala-write.log.R "./impala/groupby-impala.log"
