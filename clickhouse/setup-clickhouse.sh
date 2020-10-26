# install

sudo apt-key adv --keyserver keyserver.ubuntu.com --recv E0C56BD4

echo "deb https://repo.yandex.ru/clickhouse/deb/stable/ main/" | sudo tee /etc/apt/sources.list.d/clickhouse.list
sudo apt-get update

sudo apt-get install -y clickhouse-server clickhouse-client

# start server

sudo rm /var/log/clickhouse-server/clickhouse-server.err.log /var/log/clickhouse-server/clickhouse-server.log
sudo service clickhouse-server start

# stop server
#sudo service clickhouse-server stop

# server start/stop without sudo: use visudo to edit sudoers
#sudo cp /etc/sudoers ~/etc_sudoers.bak
#sudo EDITOR=vim visudo
#user     ALL=NOPASSWD: /usr/sbin/service clickhouse-server start
#user     ALL=NOPASSWD: /usr/sbin/service clickhouse-server stop
