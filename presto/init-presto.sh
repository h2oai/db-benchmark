#!/bin/bash
set -e

[ -z "$CLUSTER" ] && echo "Need to set CLUSTER" && exit 1;

# start cluster
echo "Starting presto cluster..."
for i in $CLUSTER; do cmd="ssh $USER@$i 'rm -rf ~/tmp/presto; mkdir -p ~/tmp/presto; export PRESTO_HOME=$PRESTO_HOME PATH=/usr/lib/jvm/java-8-oracle/bin:\$PATH; $PRESTO_HOME/bin/launcher start --verbose > ~/tmp/presto/launcher-start.log 2>&1'"; eval $cmd; done;
sleep 30
