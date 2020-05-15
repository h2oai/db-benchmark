#!/bin/bash
set -e

# upgrade to latest stable from h2o repo
echo 'upgrading h2o...'
Rscript -e 'ap=available.packages(repos="http://h2o-release.s3.amazonaws.com/h2o/latest_stable_R", method="curl"); if (ap["h2o","Version"]!=packageVersion("h2o", lib.loc="./h2o/r-h2o")) update.packages(lib.loc="./h2o/r-h2o", repos="http://h2o-release.s3.amazonaws.com/h2o/latest_stable_R", method="curl", ask=FALSE, checkBuilt=TRUE, quiet=TRUE)'
