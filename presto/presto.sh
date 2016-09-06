#!/bin/bash
set -e

# init
./presto/init-presto.sh
sleep 30

rm -f ./presto/join-presto-setup-hive.log ./presto/join-presto.log #./presto/sort-presto.log
    
# join - temporarly also sort
if [[ "$RUN_TASKS" =~ "join" ]]; then
  echo "Running presto join benchmark..."
  while read line
  do
    eval $line
    # load data using hive
    echo "loading datasets..."
    hive --define SRC_X_DIR=${SRC_X_LOCAL%.csv} --define SRC_Y_DIR=${SRC_Y_LOCAL%.csv} -f ./presto/join-presto-setup-hive.sql >> ./presto/join-presto-setup-hive.log 2>&1;
    # test presto, using <, not -f because -f wont print timing
    echo "joining..."
    PATH=/usr/lib/jvm/java-8-oracle/bin:$PATH $PRESTO_CLI --catalog hive --schema benchmark --server $MASTER:$PRESTO_PORT --output-format CSV < ./presto/join-presto.sql >> ./presto/join-presto.log 2>&1;
    # parse log, waiting until presto prints the timing
    #./presto/presto-write.log.R "./presto/join-presto.log"
    #echo "sorting..." # temporary start in join to re-use existing data http://stackoverflow.com/questions/39309984/csv-loading-from-hadoop-to-hive-scalability-issue-20gb-h
    #PATH=/usr/lib/jvm/java-8-oracle/bin:$PATH $PRESTO_CLI --catalog hive --schema benchmark --server $MASTER:$PRESTO_PORT --output-format CSV < ./presto/sort-presto.sql >> ./presto/sort-presto.log 2>&1;
    
  done < ./loop-join-data.env
fi
cat ./presto/join-presto.log

# shutdown - not shutdown to allow handscrapping
#./presto/shutdown-presto.sh
