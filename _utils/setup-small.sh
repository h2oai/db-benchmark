# full repro on Ubuntu 22.04

## Install libraries
sudo apt-get -qq update
sudo apt upgrade

sudo apt-get -qq install -y lsb-release software-properties-common wget curl vim htop git byobu libcurl4-openssl-dev libssl-dev
sudo apt-get -qq install -y libfreetype6-dev
sudo apt-get -qq install -y libfribidi-dev
sudo apt-get -qq install -y libharfbuzz-dev
sudo apt-get -qq install -y libxml2-dev
sudo apt-get -qq install -y make
sudo apt-get -qq install -y libfontconfig1-dev
sudo apt-get -qq install -y libicu-dev pandoc zlib1g-dev libgit2-dev libcurl4-openssl-dev libssl-dev libjpeg-dev libpng-dev libtiff-dev
# sudo apt-key adv --keyserver keyserver.ubuntu.com --recv-keys E298A3A825C0D65DFD57CBB651716619E084DAB9
sudo add-apt-repository "deb [arch=amd64,i386] https://cloud.r-project.org/bin/linux/ubuntu $(lsb_release -cs)-cran40/"
sudo apt-get -qq install -y r-base-dev virtualenv

sudo chmod o+w /usr/local/lib/R/site-library


Rscript -e 'install.packages(c("jsonlite","bit64","devtools","rmarkdown", "data.table"), dependecies=TRUE, repos="https://cloud.r-project.org")'


mkdir -p ~/.R
echo 'CFLAGS=-O3 -mtune=native' >> ~/.R/Makevars
echo 'CXXFLAGS=-O3 -mtune=native' >> ~/.R/Makevars


# Data generation data for groupby 0.5GB

mkdir data
cd data/
Rscript ../_data/groupby-datagen.R 1e7 1e2 0 0
Rscript ../_data/join-datagen.R 1e7 0 0 1
cd ..

# don't publish, we dont even have the keys
sed -i 's/DO_PUBLISH=true/DO_PUBLISH=false/g' run.conf

# set sizes
mv _control/data.csv _control/data.csv.original

echo "task,data,nrow,k,na,sort,active" > _control/data.csv
echo "groupby,G1_1e7_1e2_0_0,1e7,1e2,0,0,1" >> _control/data.csv
echo "join,G1_1e7_NA_0_0,1e7,NA,0,0,1" >> _control/data.csv


