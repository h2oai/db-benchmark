#!/bin/bash
set -e

## Requirements:
# 1. on mr-0xd6 you should have ~/h2o.jar, file originally from h2o-hadoop/h2o-hdp2.2-assembly/build/libs/h2odriver.jar

# copy h2o.jar to nodes
for i in $CLUSTER; do cmd="rsync -aq $HOME/h2o.jar $USER@$i:h2o.jar"; echo $cmd; eval $cmd; done
