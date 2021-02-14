#!/bin/bash
set -e

# install stable arrow
mkdir -p ./arrow/r-arrow
Rscript -e 'install.packages(c("arrow","dplyr"), lib="./arrow/r-arrow")'
