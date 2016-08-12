#!/bin/bash
set -e

[ -z "$CLUSTER" ] && echo "Need to set CLUSTER" && exit 1;

# stop cluster
echo "Stopping presto cluster..."
for i in $CLUSTER; do cmd="ssh $USER@$i 'export PRESTO_HOME=$PRESTO_HOME PATH=/usr/lib/jvm/java-8-oracle/bin:\$PATH; $PRESTO_HOME/bin/launcher kill --verbose > ~/tmp/presto/launcher-kill.log 2>&1; killall -9 presto-server 2>&1 > /dev/null'"; eval $cmd; done;
sleep 5
