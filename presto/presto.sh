#!/bin/bash
set -e

# init
./presto/init-presto.sh
sleep 30

rm -f ./presto/join-presto.log ./presto/join-presto-setup-hive.log
    
# join
if [[ "$RUN_TASKS" =~ "join" ]]; then
  echo "Running presto join benchmark..."
  while read line
  do 
    eval $line
    # load data using hive
    hive --define SRC_X_DIR=${SRC_X_LOCAL%.csv} --define SRC_Y_DIR=${SRC_Y_LOCAL%.csv} -f ./presto/join-presto-setup-hive.sql >> ./presto/join-presto-setup-hive.log 2>&1;
    # test presto, using <, not -f because -f wont print timing
    PATH=/usr/lib/jvm/java-8-oracle/bin:$PATH $PRESTO_CLI --catalog hive --schema benchmark --server $MASTER:$PRESTO_PORT --output-format CSV < ./presto/join-presto.sql >> ./presto/join-presto.log 2>&1;
    # workaround to keep logs from all iterations because they are not yet parsed
    #LOG_NUM=$(($(find ./presto/ -maxdepth 1 -name "join-presto_*log" | wc -l)+1))
    #mv ./presto/join-presto.log ./presto/join-presto_$LOG_NUM.log
    echo "# tail 150 ======"
    tail -n 150 ./presto/join-presto.log
    #./presto/presto-write.log.R "./presto/join-presto.log"
  done < ./loop-join-data.env
fi

# shutdown
./presto/shutdown-presto.sh
