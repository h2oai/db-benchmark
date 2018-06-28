#!/bin/bash
set -e

# upgrade to latest devel
echo 'upgrading dplyr...'
Rscript -e 'devtools::install_github(c("tidyverse/dplyr","tidyverse/readr"), quiet=TRUE, method="curl")'
