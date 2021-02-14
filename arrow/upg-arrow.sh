#!/bin/bash
set -e

# upgrade all packages in arrow library only if new arrow is out
echo 'upgrading arrow...'
Rscript -e 'ap=available.packages(); if (ap["arrow","Version"]!=packageVersion("arrow", lib.loc="./arrow/r-arrow")) update.packages(lib.loc="./arrow/r-arrow", ask=FALSE, checkBuilt=TRUE, quiet=TRUE)'
