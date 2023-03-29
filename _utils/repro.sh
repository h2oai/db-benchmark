# full repro on Ubuntu 22.04

cd ~/h2oai-db-benchmark

sudo apt-get -qq update
sudo apt upgrade

sudo apt-get -qq install -y lsb-release software-properties-common wget curl vim htop git byobu libcurl4-openssl-dev libssl-dev
sudo apt-get -qq install -y libfreetype6-dev
sudo apt-get -qq install -y libfribidi-dev
sudo apt-get -qq install -y libharfbuzz-dev
sudo apt-get -qq install -y git
sudo apt-get -qq install -y libxml2-dev
sudo apt-get -qq install -y make
sudo apt-get -qq install -y libfontconfig1-dev
sudo apt-get -qq install -y libicu-dev pandoc zlib1g-dev libgit2-dev libcurl4-openssl-dev libssl-dev libjpeg-dev libpng-dev libtiff-dev
# sudo apt-key adv --keyserver keyserver.ubuntu.com --recv-keys E298A3A825C0D65DFD57CBB651716619E084DAB9
sudo add-apt-repository "deb [arch=amd64,i386] https://cloud.r-project.org/bin/linux/ubuntu $(lsb_release -cs)-cran40/"
sudo apt-get -qq update
sudo apt-get -qq install -y r-base-dev virtualenv

cd /usr/local/lib/R
sudo chmod o+w site-library

cd ~
mkdir -p .R
echo 'CFLAGS=-O3 -mtune=native' >> ~/.R/Makevars
echo 'CXXFLAGS=-O3 -mtune=native' >> ~/.R/Makevars

cd pydatatable
virtualenv py-pydatatable --python=/usr/bin/python3.10
cd ../pandas
virtualenv py-pandas --python=/usr/bin/python3.10
cd ../modin
virtualenv py-modin --python=/usr/bin/python3.10
cd ..


Rscript -e 'install.packages(c("jsonlite","bit64","devtools","rmarkdown"), dependecies=TRUE, repos="https://cloud.r-project.org")'


source ./pandas/py-pandas/bin/activate
python3 -m pip install --upgrade psutil
python3 -m pip install --upgrade pandas
deactivate

source ./modin/py-modin/bin/activate
python3 -m pip install --upgrade modin
deactivate

source ./pydatatable/py-pydatatable/bin/activate
python3 -m pip install --upgrade git+https://github.com/h2oai/datatable
deactivate

# install dplyr
Rscript -e 'devtools::install_github(c("tidyverse/readr","tidyverse/dplyr"))'

# install data.table
Rscript -e 'install.packages("data.table", repos="https://rdatatable.gitlab.io/data.table/")'


# generate data for groupby 0.5GB
Rscript _data/groupby-datagen.R 1e7 1e2 0 0

mkdir data
mv G1_1e7_1e2_0_0.csv data/

# Rscript _data/groupby-datagen.R 1e8 1e2 0 0
# Rscript _data/groupby-datagen.R 1e9 1e2 0 0

# set only groupby task
echo "Changing run.conf and _control/data.csv to run only groupby at 0.5GB"
cp run.conf run.conf.original
sed -i 's/groupby join groupby2014/groupby/g' run.conf
sed -i 's/data.table dplyr pandas pydatatable spark dask clickhouse polars arrow duckdb/data.table dplyr duckdb/g' run.conf 
sed -i 's/DO_PUBLISH=true/DO_PUBLISH=false/g' run.conf

# set sizes
mv _control/data.csv _control/data.csv.original

echo "task,data,nrow,k,na,sort,active" > _control/data.csv
echo "groupby,G1_1e7_1e2_0_0,1e7,1e2,0,0,1" >> _control/data.csv

./dplyr/setup-dplyr.sh
./datatable/setup-datatable.sh
./duckdb/setup-duckdb.sh

# running db-benchmark
echo "running benchmark"
./run.sh

echo "Returning run.conf and _control/data.csv to original state"
mv run.cong.original run.conf
mv _control/data.csv.original _control/data.csv
