#!/bin/bash
set -e

[ -z "$CLUSTER" ] && echo "Need to set CLUSTER" && exit 1;

# stop cluster
echo "Stopping presto cluster..."
for i in $CLUSTER; do cmd="ssh $USER@$i 'export PRESTO_HOME=$PRESTO_HOME PATH=/usr/lib/jvm/java-8-oracle/bin:\$PATH; $PRESTO_HOME/bin/launcher kill'"; eval $cmd; done;
sleep 5
for i in $CLUSTER; do (ssh $USER@$i "killall -9 presto-server 2>&1 > /dev/null" &) 2>&1 > /dev/null; done && sleep 5
for i in $CLUSTER; do (ssh $USER@$i "killall -9 java 2>&1 > /dev/null" &) 2>&1 > /dev/null; done && sleep 5
sleep 5

echo "Checking presto cluster stopped correctly: presto-server, java. No pid below means ok." # echo \$HOSTNAME; 
for i in $CLUSTER; do cmd="ssh $USER@$i 'pgrep -u $USER presto-server'"; eval $cmd; done;
for i in $CLUSTER; do cmd="ssh $USER@$i 'pgrep -u $USER java'"; eval $cmd; done;
