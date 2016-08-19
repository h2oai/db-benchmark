#!/bin/bash
set -e

[ -z "$CLUSTER" ] && echo "Need to set CLUSTER" && exit 1;

# kcl: always kill all hosts to not leave lying around when changing configs
for i in $CLUSTER; do (ssh $USER@$i "killall -9 java 2>&1 > /dev/null" &) 2>&1 > /dev/null; done && sleep 5

# update h2o on all nodes and R pkg in client
echo "# Updating h2o to latest devel..."
rm -rf ~/tmp/h2o/r-pkg ~/h2o.jar
mkdir -p ~/tmp/h2o/r-pkg
scp jenkins@mr-0xc1:/var/lib/jenkins/jobs/h2o_master_gradle_buildFlow/lastSuccessful/archive/h2o-r/R/src/contrib/h2o*.tar.gz ~/tmp/h2o/r-pkg/.
scp jenkins@mr-0xc1:/var/lib/jenkins/jobs/h2o_master_gradle_buildFlow/lastSuccessful/archive/h2o-hadoop/h2o-hdp2.2-assembly/build/libs/h2odriver.jar ~/h2o.jar
Rscript -e 'install.packages(tail(list.files("~/tmp/h2o/r-pkg", pattern="\\.tar\\.gz$", full.names=TRUE), 1), repos=NULL, type="source", quiet=TRUE)'
for i in $CLUSTER; do cmd="rsync -aq $HOME/h2o.jar $USER@$i:h2o.jar"; eval $cmd; done

# start cluster
for i in $CLUSTER; do (ssh $USER@$i "nohup java $MEM -XX:-PrintGCDetails -cp h2o.jar water.H2OApp -name $H2O_NAME -baseport $H2O_PORT 2>&1 >> ~/cluster.log" & ); done && sleep 2 && echo Started h2o cluster ok on $CLUSTER
sleep 15
