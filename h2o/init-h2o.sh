#!/bin/bash
set -e

[ -z "$CLUSTER" ] && echo "Need to set CLUSTER" && exit 1;

# kcl: always kill all hosts to not leave lying around when changing configs
for i in $CLUSTER; do (ssh $USER@$i "killall -9 java 2>&1 > /dev/null" &) 2>&1 > /dev/null; done && sleep 5

# start cluster
for i in $CLUSTER; do (ssh $USER@$i "nohup java $MEM -cp h2o.jar water.H2OApp -name $H2O_NAME -port $H2O_PORT 2>&1 >> ~/cluster.log" & ); done && sleep 2 && echo Started h2o cluster ok on $CLUSTER
sleep 15
