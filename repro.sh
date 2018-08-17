# full repro on Ubuntu 16.04

sudo apt-get -qq update
sudo apt-get -qq install -y lsb-release software-properties-common wget curl vim htop git byobu libcurl4-openssl-dev libssl-dev
sudo apt-key adv --keyserver keyserver.ubuntu.com --recv-keys E298A3A825C0D65DFD57CBB651716619E084DAB9
sudo add-apt-repository "deb [arch=amd64,i386] https://cran.r-project.org/bin/linux/ubuntu `lsb_release -sc`/"
sudo apt-get -qq update
sudo apt-get -qq install -y r-base-dev virtualenv python3.5-dev

wget https://releases.llvm.org/6.0.0/clang+llvm-6.0.0-x86_64-linux-gnu-ubuntu-16.04.tar.xz
sudo mv clang+llvm-6.0.0-x86_64-linux-gnu-ubuntu-16.04.tar.xz /opt
cd /opt
sudo tar xvf clang+llvm-6.0.0-x86_64-linux-gnu-ubuntu-16.04.tar.xz

cd /usr/local/lib/R
sudo chmod o+w site-library

cd ~
mkdir -p .R
echo 'CFLAGS=-O3 -mtune=native' >> ~/.R/Makevars
echo 'CXXFLAGS=-O3 -mtune=native' >> ~/.R/Makevars

git clone http://github.com/h2oai/datatable
git clone http://github.com/h2oai/db-benchmark

cd db-benchmark
cd pydatatable
virtualenv py-pydatatable --python=/usr/bin/python3.6
cd ../pandas
virtualenv py-pandas --python=/usr/bin/python3.6
cd ../modin
virtualenv py-modin --python=/usr/bin/python3.6
cd ..

Rscript -e 'install.packages(c("jsonlite","bit64","devtools","rmarkdown"), repos="https://cloud.r-project.org")'

byobu
source ./pandas/py-pandas/bin/activate
python -m pip install --upgrade psutil
python -m pip install --upgrade pandas
deactivate

source ./modin/py-modin/bin/activate
python -m pip install --upgrade modin
deactivate

# install pydatatable
source ./pydatatable/py-pydatatable/bin/activate
export LLVM6=/opt/clang+llvm-6.0.0-x86_64-linux-gnu-ubuntu-16.04
cd datatable
make build
make install
deactivate

# install dplyr
Rscript -e 'devtools::install_github(c("tidyverse/readr","tidyverse/dplyr"))'

# install data.table
Rscript -e 'install.packages("data.table", repos="https://Rdatatable.github.io/data.table")'

# benchmark
cd db-benchmark

# generate data for groupby
Rscript groupby-datagen.R 1e7 1e2
Rscript groupby-datagen.R 1e8 1e2
Rscript groupby-datagen.R 1e9 1e2
#Rscript groupby-datagen.R 2e9 1e2 # https://github.com/Rdatatable/data.table/issues/2956

# set only groupby task
vim run.conf

# set data sizes
[ ! -f ./orgdata.csv ] && cp data.csv orgdata.csv
vim data.csv

# running db-benchmark
./run.sh
