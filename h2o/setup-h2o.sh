#!/bin/bash
set -e

# build h2o locally
rm ./h2o-hadoop/h2o-hdp2.2-assembly/build/libs/h2odriver-*
rm h2o-r/R/src/contrib/*

./gradlew clean
./gradlew build -x test
./gradlew dist
mv ./h2o-hadoop/h2o-hdp2.2-assembly/build/libs/h2odriver.jar ~/h2o.jar

# send to mr-0xd6
rsync -aq $HOME/h2o.jar $USER@mr-0xd6:h2o.jar

# copy h2o.jar to all nodes
for i in $CLUSTER; do cmd="rsync -aq $HOME/h2o.jar $USER@$i:h2o.jar"; echo $cmd; eval $cmd; done
