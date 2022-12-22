
# install
sudo apt-get install -y apt-transport-https ca-certificates dirmngr
sudo apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv 8919F6BD2B48D754

echo "deb https://packages.clickhouse.com/deb stable main" | sudo tee /etc/apt/sources.list.d/clickhouse.list
sudo apt-get update

sudo apt-get install -y clickhouse-server clickhouse-client

# start server

sudo rm /var/log/clickhouse-server/clickhouse-server.err.log /var/log/clickhouse-server/clickhouse-server.log
sudo service clickhouse-server start

# stop server
#sudo service clickhouse-server stop

# let file table function access csv -- NO LONGER NECESSARY
# grep '<user_files_path>/var/lib/clickhouse/user_files/</user_files_path>' /etc/clickhouse-server/config.xml
# sudo sed -i -e "s|<user_files_path>/home/ubuntu/</user_files_path>|<user_files_path>/var/lib/clickhouse/user_files/</user_files_path>|" /etc/clickhouse-server/config.xml
# sudo grep 'user_files_path' /etc/clickhouse-server/config.xml

# server start/stop without sudo: use visudo to edit sudoers
#sudo cp /etc/sudoers etc_sudoers.bak
#sudo EDITOR=vim visudo
# add two lines for your user at the end of section: Members of the admin group may gain root privileges
#user     ALL=NOPASSWD: /usr/sbin/service clickhouse-server start
#user     ALL=NOPASSWD: /usr/sbin/service clickhouse-server stop

# interactive debugging
# copy exec.sh body and substitute $1 for groupby and $2 for G1_1e7_1e2_0_0, avoid exit calls