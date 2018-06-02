
# For fresh Ubuntu 14.04 LTS Trusty ...

# r3-8xlarge ( 240GB RAM / 32 cores )  - data.table benchmarking
# r3-xlarge ( 30GB RAM / 4 cores )   - h2o

# Always ...
sudo apt-key adv --keyserver keyserver.ubuntu.com --recv-keys E084DAB9
sudo add-apt-repository -y 'deb  http://cran.stat.ucla.edu/bin/linux/ubuntu trusty/'
sudo apt-get update
sudo apt-get -y install r-base-core htop libcurl4-openssl-dev  # needed by devtools and h2o

sudo R
options(repos = "http://cran.stat.ucla.edu")
install.packages(c("RCurl","rjson","statmod",   # dependencies of h2o not done by step below
                   "devtools","roxygen2"))      # for h2o-dev
q(save="no")


# H2O dev ...
sudo apt-get -y install python-pip
sudo pip install grip
sudo easy_install -U pip
sudo pip install tabulate
sudo pip install wheel
sudo apt-get -y install npm
sudo ln -s /usr/bin/nodejs /usr/bin/node
sudo npm install -g bower

# for java for h2o and h2o-dev
# To remove openJDK on my laptop first : sudo apt-get purge openjdk-\*
sudo add-apt-repository -y ppa:webupd8team/java    # on my debian laptop then look at sources.list file and change 'testing' to 'trusty' in the sources.list file.
sudo apt-get update
sudo apt-get -y install oracle-java7-installer
sudo apt-get -y install oracle-java7-set-default
java -version
# end for java
# for h2o-dev
sudo apt-get -y install git
git clone https://github.com/h2oai/h2o-dev
cd h2o-dev
./gradlew syncSmalldata
./gradlew build -x test       # 16mins from scratch.  11 min when rebuilt (mainly testMultiNode)
sudo R CMD INSTALL ~/h2o-dev/h2o-r/R/src/contrib/h2o_0.3.0.99999.tar.gz
java -Xmx2g -jar build/h2o.jar &

./gradlew idea
Intellij:  open project
open  *.iml.  Has Intellij icon.


ssh -i mdowle.pem -L 55555:localhost:54321 ubuntu@54.67.82.235
htop

http://localhost:55555/



mkdir .R
echo -e 'CFLAGS=-O3 -mtune=native\nCXXFLAGS=-O3 -mtune=native' > ~/.R/Makevars
# R's default is '-g -O2'

sudo R
options(repos = "http://cran.stat.ucla.edu")
install.packages("data.table")
install.packages("h2o", repos="http://h2o-release.s3.amazonaws.com/h2o/rel-maxwell/8/R")

q(save="no")


install.packages("dplyr")


require(devtools)
install_github("hadley/dplyr", build_vignettes=FALSE)

q(save="no")
R
require(data.table)
require(dplyr)
sessionInfo()
q(save="no")
lsb_release -a
uname -a
lscpu
free -h

# To copy to EC2 (final colon needed!):
scp -i ~/mdowle.pem bigbench.R ubuntu@54.183.161.72:

# To copy from EC2 (without copying bigbench.R back):
scp -i ~/mdowle.pem ubuntu@54.183.161.72:~/*E* .


####
ubuntu@ip-172-31-33-222:~$ R

R version 3.1.1 (2014-07-10) -- "Sock it to Me"
Copyright (C) 2014 The R Foundation for Statistical Computing
Platform: x86_64-pc-linux-gnu (64-bit)

> require(data.table)
Loading required package: data.table
data.table 1.9.2  For help type: help("data.table")
> require(dplyr)
Loading required package: dplyr

Attaching package: ‘dplyr’

The following object is masked from ‘package:data.table’:

    last

The following objects are masked from ‘package:stats’:

    filter, lag

The following objects are masked from ‘package:base’:

    intersect, setdiff, setequal, union

> sessionInfo()
R version 3.1.1 (2014-07-10)
Platform: x86_64-pc-linux-gnu (64-bit)

locale:
 [1] LC_CTYPE=en_US.UTF-8       LC_NUMERIC=C              
 [3] LC_TIME=en_US.UTF-8        LC_COLLATE=en_US.UTF-8    
 [5] LC_MONETARY=en_US.UTF-8    LC_MESSAGES=en_US.UTF-8   
 [7] LC_PAPER=en_US.UTF-8       LC_NAME=C                 
 [9] LC_ADDRESS=C               LC_TELEPHONE=C            
[11] LC_MEASUREMENT=en_US.UTF-8 LC_IDENTIFICATION=C       

attached base packages:
[1] stats     graphics  grDevices utils     datasets  methods   base     

other attached packages:
[1] dplyr_0.2        data.table_1.9.2

loaded via a namespace (and not attached):
[1] assertthat_0.1 parallel_3.1.1 plyr_1.8.1     Rcpp_0.11.2    reshape2_1.4  
[6] stringr_0.6.2  tools_3.1.1   
> q(save="no")
ubuntu@ip-172-31-33-222:~$ lsb_release -a
No LSB modules are available.
Distributor ID:	Ubuntu
Description:	Ubuntu 14.04 LTS
Release:	14.04
Codename:	trusty
ubuntu@ip-172-31-33-222:~$ uname -a
Linux ip-172-31-33-222 3.13.0-29-generic #53-Ubuntu SMP Wed Jun 4 21:00:20 UTC 2014 x86_64 x86_64 x86_64 GNU/Linux
ubuntu@ip-172-31-33-222:~$ lscpu
Architecture:          x86_64
CPU op-mode(s):        32-bit, 64-bit
Byte Order:            Little Endian
CPU(s):                32
On-line CPU(s) list:   0-31
Thread(s) per core:    2
Core(s) per socket:    8
Socket(s):             2
NUMA node(s):          2
Vendor ID:             GenuineIntel
CPU family:            6
Model:                 62
Stepping:              4
CPU MHz:               2494.090
BogoMIPS:              5049.01
Hypervisor vendor:     Xen
Virtualization type:   full
L1d cache:             32K
L1i cache:             32K
L2 cache:              256K
L3 cache:              25600K
NUMA node0 CPU(s):     0-7,16-23
NUMA node1 CPU(s):     8-15,24-31
ubuntu@ip-172-31-33-222:~$ free -h
             total       used       free     shared    buffers     cached
Mem:          240G       2.4G       237G       364K        60M       780M
-/+ buffers/cache:       1.6G       238G
Swap:           0B         0B         0B
ubuntu@ip-172-31-33-222:~$ 


