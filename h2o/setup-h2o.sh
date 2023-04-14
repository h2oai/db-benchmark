mkdir -p ./h2o/log
# install h2o
mkdir -p ./h2o/r-h2o
Rscript -e 'install.packages(c("RCurl","jsonlite"), repos="https://cloud.r-project.org", lib="./h2o/r-h2o"); install.packages("h2o", repos="http://h2o-release.s3.amazonaws.com/h2o/latest_stable_R", method="curl", lib="./h2o/r-h2o")'

