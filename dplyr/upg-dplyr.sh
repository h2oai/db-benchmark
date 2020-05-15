#!/bin/bash
set -e

# upgrade all packages in dplyr library only if new dplyr is out
echo 'upgrading dplyr...'
Rscript -e 'ap=available.packages(); if (ap["dplyr","Version"]!=packageVersion("dplyr", lib.loc="./dplyr/r-dplyr")) update.packages(lib.loc="./dplyr/r-dplyr", ask=FALSE, checkBuilt=TRUE, quiet=TRUE)'
