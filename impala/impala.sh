#!/bin/bash
set -e

# join
if [[ "$RUN_TASKS" =~ "join" ]]; then
  echo "Running impala join benchmark..."
  while read line
  do 
    rm -f ./impala/join-impala.log
    eval $line
    $IMPALA_HOME/bin/impala-shell -i $IMPALA_MASTER --var=SRC_X_DIR=${SRC_X_LOCAL%.csv} --var=SRC_Y_DIR=${SRC_Y_LOCAL%.csv} -B --output_delimiter ',' -V -f ./impala/join-impala.sql &> ./impala/join-impala.log
    cat ./impala/join-impala.log
    ./impala/impala-write.log.R "./impala/join-impala.log"
  done < ./loop-join-data.env
fi

# groupby
if [[ "$RUN_TASKS" =~ "groupby" ]]; then
  echo "Running impala groupby benchmark..."
  while read line
  do 
    rm -f ./impala/groupby-impala.log
    eval $line
    $IMPALA_HOME/bin/impala-shell -i $IMPALA_MASTER --var=SRC_GRP_DIR=${SRC_GRP_LOCAL%.csv} -B --output_delimiter ',' -V -f ./impala/groupby-impala.sql &> ./impala/groupby-impala.log
    cat ./impala/groupby-impala.log
    ./impala/impala-write.log.R "./impala/groupby-impala.log"
  done < ./loop-groupby-data.env
fi

# sort - ORDER BY is ignored for cli unless you print all results: https://issues.cloudera.org/browse/IMPALA-1052 reproduce with .sort-impala.sql
if [[ "$RUN_TASKS" =~ "sort" ]]; then
  echo "Running impala sort benchmark..."
  while read line
  do
    rm -f ./impala/sort-impala.log
    eval $line
    $IMPALA_HOME/bin/impala-shell -i $IMPALA_MASTER --var=SRC_X_DIR=${SRC_X_LOCAL%.csv} -B --output_delimiter ',' -V -f ./impala/sort-impala.sql &> ./impala/sort-impala.log
    cat ./impala/sort-impala.log
    ./impala/impala-write.log.R "./impala/sort-impala.log"
  done < ./loop-sort-data.env
fi
