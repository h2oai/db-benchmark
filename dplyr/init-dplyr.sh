#!/bin/bash
set -e

# upgrade to latest devel
Rscript -e 'devtools::install_github("tidyverse/dplyr", quiet=TRUE, method="curl")'
