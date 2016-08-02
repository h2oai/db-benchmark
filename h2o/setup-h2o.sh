#!/bin/bash
set -e

## Requirements:
# 1. on mr-0xd6 you should have ~/h2o.jar, file originally from h2o-hadoop/h2o-hdp2.2-assembly/build/libs/h2odriver.jar

# build h2o
BUILD_HADOOP=YeAh ./gradlew :h2o-hadoop:h2o-hdp2.2-assembly:assemble
mv ./h2o-hadoop/h2o-hdp2.2-assembly/build/libs/h2odriver.jar ~/h2o.jar
rsync -aq $HOME/h2o.jar $USER@mr-0xd6:h2o.jar

# copy h2o.jar to all nodes
for i in $CLUSTER; do cmd="rsync -aq $HOME/h2o.jar $USER@$i:h2o.jar"; echo $cmd; eval $cmd; done
