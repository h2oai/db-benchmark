
# install

sudo apt-key adv --keyserver keyserver.ubuntu.com --recv E0C56BD4

echo "deb https://repo.yandex.ru/clickhouse/deb/stable/ main/" | sudo tee /etc/apt/sources.list.d/clickhouse.list
sudo apt-get update

sudo apt-get install -y clickhouse-server clickhouse-client

# start server

sudo rm /var/log/clickhouse-server/clickhouse-server.err.log /var/log/clickhouse-server/clickhouse-server.log
sudo service clickhouse-server start

# create table for groupby
## in memory engine
#clickhouse-client --query="CREATE TABLE IF NOT EXISTS G1_1e6_1e2_0_0 (id1 String, id2 String, id3 String, id4 Int32, id5 Int32, id6 Int32, v1 Int32, v2 Int32, v3 Float64) ENGINE = Memory()"
Rscript -e 'all_data=data.table::fread("data.csv")[task=="groupby"][substr(data, 1L, 2L)=="G1", data]; setNames(sapply(FUN=system, sprintf("clickhouse-client --query=\"CREATE TABLE IF NOT EXISTS %s (id1 String, id2 String, id3 String, id4 Int32, id5 Int32, id6 Int32, v1 Int32, v2 Int32, v3 Float64) ENGINE = Memory()\"", all_data)), all_data)'
## mergeetree engine
Rscript -e 'all_data=data.table::fread("data.csv")[task=="groupby"][substr(data, 1L, 2L)=="G2", data]; setNames(sapply(FUN=system, sprintf("clickhouse-client --query=\"CREATE TABLE IF NOT EXISTS %s (id0 Int32, id1 String, id2 String, id3 String, id4 Int32, id5 Int32, id6 Int32, v1 Int32, v2 Int32, v3 Float64) ENGINE = MergeTree() ORDER BY (id0)\"", all_data)), all_data)'

# prepare primary key for mergetree table engine
awk -F',' -v OFS=',' 'NR == 1 {print "id0", $0; next} {print (NR-1), $0}' data/G1_1e6_1e2_0_0.csv > data/G2_1e6_1e2_0_0.csv
awk -F',' -v OFS=',' 'NR == 1 {print "id0", $0; next} {print (NR-1), $0}' data/G1_1e6_1e1_0_0.csv > data/G2_1e6_1e1_0_0.csv
awk -F',' -v OFS=',' 'NR == 1 {print "id0", $0; next} {print (NR-1), $0}' data/G1_1e6_2e0_0_0.csv > data/G2_1e6_2e0_0_0.csv
awk -F',' -v OFS=',' 'NR == 1 {print "id0", $0; next} {print (NR-1), $0}' data/G1_1e6_1e2_0_1.csv > data/G2_1e6_1e2_0_1.csv
awk -F',' -v OFS=',' 'NR == 1 {print "id0", $0; next} {print (NR-1), $0}' data/G1_1e7_1e2_0_0.csv > data/G2_1e7_1e2_0_0.csv
awk -F',' -v OFS=',' 'NR == 1 {print "id0", $0; next} {print (NR-1), $0}' data/G1_1e7_1e1_0_0.csv > data/G2_1e7_1e1_0_0.csv
awk -F',' -v OFS=',' 'NR == 1 {print "id0", $0; next} {print (NR-1), $0}' data/G1_1e7_2e0_0_0.csv > data/G2_1e7_2e0_0_0.csv
awk -F',' -v OFS=',' 'NR == 1 {print "id0", $0; next} {print (NR-1), $0}' data/G1_1e7_1e2_0_1.csv > data/G2_1e7_1e2_0_1.csv
awk -F',' -v OFS=',' 'NR == 1 {print "id0", $0; next} {print (NR-1), $0}' data/G1_1e8_1e2_0_0.csv > data/G2_1e8_1e2_0_0.csv
awk -F',' -v OFS=',' 'NR == 1 {print "id0", $0; next} {print (NR-1), $0}' data/G1_1e8_1e1_0_0.csv > data/G2_1e8_1e1_0_0.csv
awk -F',' -v OFS=',' 'NR == 1 {print "id0", $0; next} {print (NR-1), $0}' data/G1_1e8_2e0_0_0.csv > data/G2_1e8_2e0_0_0.csv
awk -F',' -v OFS=',' 'NR == 1 {print "id0", $0; next} {print (NR-1), $0}' data/G1_1e8_1e2_0_1.csv > data/G2_1e8_1e2_0_1.csv
awk -F',' -v OFS=',' 'NR == 1 {print "id0", $0; next} {print (NR-1), $0}' data/G1_1e9_1e2_0_0.csv > data/G2_1e9_1e2_0_0.csv
awk -F',' -v OFS=',' 'NR == 1 {print "id0", $0; next} {print (NR-1), $0}' data/G1_1e9_1e1_0_0.csv > data/G2_1e9_1e1_0_0.csv
awk -F',' -v OFS=',' 'NR == 1 {print "id0", $0; next} {print (NR-1), $0}' data/G1_1e9_2e0_0_0.csv > data/G2_1e9_2e0_0_0.csv
awk -F',' -v OFS=',' 'NR == 1 {print "id0", $0; next} {print (NR-1), $0}' data/G1_1e9_1e2_0_1.csv > data/G2_1e9_1e2_0_1.csv

# testing
# load data - as a part of exec script
#clickhouse-client --max_memory_usage=109951162777600 --query="INSERT INTO G1_1e6_1e2_0_0 FORMAT CSVWithNames" < data/G1_1e6_1e2_0_0.csv
#clickhouse-client --query="SELECT count(*) FROM G1_1e6_1e2_0_0"
#clickhouse-client --max_memory_usage=109951162777600 --query="INSERT INTO G1_1e7_1e2_0_0 FORMAT CSVWithNames" < data/G1_1e7_1e2_0_0.csv
#clickhouse-client --max_memory_usage=109951162777600 --query="SELECT count(*) FROM G1_1e7_1e2_0_0"
# try some query
#clickhouse-client --max_memory_usage=109951162777600 --output_format_pretty_max_rows 10
#SELECT id1, sum(v1) AS v1 FROM G1_1e7_1e2_0_0 GROUP BY id1;

# stop server
#sudo service clickhouse-server stop

# server start/stop without sudo: use visudo to edit sudoers
#sudo cp /etc/sudoers ~/etc_sudoers.bak
#sudo EDITOR=vim visudo
#user     ALL=NOPASSWD: /usr/sbin/service clickhouse-server start
#user     ALL=NOPASSWD: /usr/sbin/service clickhouse-server stop
