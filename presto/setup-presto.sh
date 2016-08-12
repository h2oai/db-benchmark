#https://prestodb.io/docs/current/installation/deployment.html

# cd ~
# wget https://repo1.maven.org/maven2/com/facebook/presto/presto-server/0.150/presto-server-0.150.tar.gz
# curl -O https://repo1.maven.org/maven2/com/facebook/presto/presto-server/0.150/presto-server-0.150.tar.gz

# extract in client
# tar -xf presto-server-0.150.tar.gz
export PRESTO_HOME=$HOME/presto-server-0.150
# mkdir -p ~/tmp/presto

# merge config from previous version, etc.
#cp -n $PRESTO_HOME_OLD/etc/config.properties.master $PRESTO_HOME/etc/config.properties.master
#cp -n $PRESTO_HOME_OLD/etc/config.properties.worker $PRESTO_HOME/etc/config.properties.worker

# sync directory to cluster
#for i in $CLUSTER; do cmd="ssh $USER@$i 'rm -rf $PRESTO_HOME'"; echo $cmd; eval $cmd; done
#for i in $CLUSTER; do cmd="ssh $USER@$i 'mkdir -p $PRESTO_HOME'"; echo $cmd; eval $cmd; done
for i in $CLUSTER; do cmd="rsync -aq $PRESTO_HOME/ $USER@$i:$PRESTO_HOME"; echo $cmd; eval $cmd; done
# for i in $CLUSTER; do cmd="ssh $USER@$i 'mkdir -p ~/tmp/presto'"; echo $cmd; eval $cmd; done

# NEW: master and slaves
for i in $CLUSTER; do cmd="ssh $USER@$i 'export PRESTO_NODE_ID=\$HOSTNAME; echo \$PRESTO_NODE_ID; envsubst < $PRESTO_HOME/etc/node.properties.in > $PRESTO_HOME/etc/node.properties'"; eval $cmd; done;
for i in $SLAVES; do cmd="ssh $USER@$i 'mv $PRESTO_HOME/etc/config.properties.worker $PRESTO_HOME/etc/config.properties'"; eval $cmd; done;
for i in $MASTER; do cmd="ssh $USER@$i 'mv $PRESTO_HOME/etc/config.properties.master $PRESTO_HOME/etc/config.properties'"; eval $cmd; done;

# # client setup
# curl -O https://repo1.maven.org/maven2/com/facebook/presto/presto-cli/0.150/presto-cli-0.150-executable.jar
# cp presto-cli-0.150-executable.jar presto.jar
# chmod a+x presto.jar
# 
# PATH=/usr/lib/jvm/java-8-oracle/bin:$PATH ./presto.jar --version
# # Presto CLI 0.150
# 
# PATH=/usr/lib/jvm/java-8-oracle/bin:$PATH ./presto.jar --help
# 
# export PRESTO_PORT=18880
# export PRESTO_CLI="$HOME/presto.jar"
# PATH=/usr/lib/jvm/java-8-oracle/bin:$PATH $PRESTO_CLI --server $MASTER:$PRESTO_PORT --execute "select * from system.runtime.nodes;"
# 
# # https://prestodb.io/docs/current/installation/cli.html
# PATH=/usr/lib/jvm/java-8-oracle/bin:$PATH ./presto.jar --server $MASTER:$PRESTO_PORT 
# > select * from system.runtime.nodes;
