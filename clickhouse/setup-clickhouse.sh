
# install

sudo apt-get install dirmngr    # optional
sudo apt-key adv --keyserver keyserver.ubuntu.com --recv E0C56BD4    # optional

echo "deb https://repo.yandex.ru/clickhouse/deb/stable/ main/" | sudo tee /etc/apt/sources.list.d/clickhouse.list
sudo apt-get update

sudo apt-get install -y clickhouse-server clickhouse-client

# start server

sudo rm /var/log/clickhouse-server/clickhouse-server.err.log /var/log/clickhouse-server/clickhouse-server.log
sudo service clickhouse-server start

# create table for groupby

clickhouse-client --query="CREATE TABLE IF NOT EXISTS G1_1e7_1e2_0_0 (id1 String, id2 String, id3 String, id4 Int32, id5 Int32, id6 Int32, v1 Int32, v2 Int32, v3 Float64) ENGINE = Memory()"
clickhouse-client --query="TRUNCATE TABLE G1_1e7_1e2_0_0"
clickhouse-client --query="INSERT INTO G1_1e7_1e2_0_0 FORMAT CSVWithNames" < data/G1_1e7_1e2_0_0.csv
clickhouse-client --query="SELECT count(*) FROM G1_1e7_1e2_0_0"

## https://github.com/yandex/ClickHouse/issues/4463

#clickhouse-client --query="CREATE TABLE IF NOT EXISTS G1_1e8_1e2_0_0 (id1 String, id2 String, id3 String, id4 Int32, id5 Int32, id6 Int32, v1 Int32, v2 Int32, v3 Float64) ENGINE = Memory()"
#clickhouse-client --query="TRUNCATE TABLE G1_1e8_1e2_0_0"
#clickhouse-client --query="INSERT INTO G1_1e8_1e2_0_0 FORMAT CSVWithNames" < data/G1_1e8_1e2_0_0.csv
#clickhouse-client --query="SELECT count(*) FROM G1_1e8_1e2_0_0"

#clickhouse-client --query="CREATE TABLE IF NOT EXISTS G1_1e9_1e2_0_0 (id1 String, id2 String, id3 String, id4 Int32, id5 Int32, id6 Int32, v1 Int32, v2 Int32, v3 Float64) ENGINE = Memory()"
#clickhouse-client --query="TRUNCATE TABLE G1_1e9_1e2_0_0"
#clickhouse-client --query="INSERT INTO G1_1e9_1e2_0_0 FORMAT CSVWithNames" < data/G1_1e9_1e2_0_0.csv
#clickhouse-client --query="SELECT count(*) FROM G1_1e9_1e2_0_0"

# try some query

clickhouse-client

SET log_queries=1;
SELECT id1, sum(v1) AS v1 FROM G1_1e7_1e2_0_0 GROUP BY id1;
SELECT id1, sum(v1) AS v1 FROM G1_1e8_1e2_0_0 GROUP BY id1;
SELECT query, read_rows, result_rows, memory_usage, query_duration_ms FROM system.query_log WHERE type=2 AND query='SELECT id1, sum(v1) AS v1 FROM G1_1e8_1e2_0_0 GROUP BY id1';

# stop server

sudo service clickhouse-server stop
