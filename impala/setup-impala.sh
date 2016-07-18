#!/bin/bash
set -e

## Requirements:
# 1. passwordless ssh to mr-0xd2-precise1, or just run below rsync commands in shell
# 2. hdfs source datasets syncronized between hdsf and hdfs-precise1
# 3. source files must be placed in own dirs /file1.csv /file1/file1.csv

# copy impala-shell client
mkdir -p $HOME/impala/bin $HOME/impala/lib
rsync -e ssh $USER@mr-0xd2-precise1:/opt/cloudera/parcels/CDH-5.7.1-1.cdh5.7.1.p0.11/bin/impala-shell $HOME/impala/bin/impala-shell
rsync -r -e ssh $USER@mr-0xd2-precise1:/opt/cloudera/parcels/CDH-5.7.1-1.cdh5.7.1.p0.11/lib/impala-shell/ $HOME/impala/lib/impala-shell

# copy files to own subdirs on hdfs in mr-0xd2-precise1
SRC_X_LOCAL=$(echo "$SRC_X" | cut -c 15-) # extract path for impala: /datasets/mattd
SRC_Y_LOCAL=$(echo "$SRC_Y" | cut -c 15-)
echo $SRC_X_LOCAL
echo $SRC_Y_LOCAL
ssh mr-0xd2-precise1 << EOF
hadoop fs -rm -r -skipTrash ${SRC_X_LOCAL%.csv} ${SRC_Y_LOCAL%.csv}
hadoop fs -mkdir -p ${SRC_X_LOCAL%.csv} ${SRC_Y_LOCAL%.csv}
hadoop fs -cp $SRC_X_LOCAL ${SRC_X_LOCAL%.csv}
hadoop fs -cp $SRC_Y_LOCAL ${SRC_Y_LOCAL%.csv}
EOF

# test client
$HOME/impala/bin/impala-shell -i mr-0xd2-precise1 -q "select version();"
