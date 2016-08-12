#!/bin/bash
set -e

[ -z "$CLUSTER" ] && echo "Need to set CLUSTER" && exit 1;

# stop cluster
echo "Stopping presto cluster..."
for i in $CLUSTER; do cmd="ssh $USER@$i 'export PRESTO_HOME=$PRESTO_HOME PATH=/usr/lib/jvm/java-8-oracle/bin:\$PATH; $PRESTO_HOME/bin/launcher kill'"; eval $cmd; done;
sleep 5

echo "Checking presto cluster stopped correctly: presto-server, java. No pid means ok." # echo \$HOSTNAME; 
for i in $CLUSTER; do cmd="ssh $USER@$i 'pgrep -u jan presto-server'"; eval $cmd; done;
for i in $CLUSTER; do cmd="ssh $USER@$i 'pgrep -u jan java'"; eval $cmd; done;
