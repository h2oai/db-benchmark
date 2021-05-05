#!/bin/bash
set -e

# upgrade all packages in duckdb library only if new arrow is out
echo 'upgrading duckdb...'
Rscript -e 'ap=available.packages(repo="https://cloud.r-project.org/"); if (ap["duckdb","Version"]!=packageVersion("duckdb", lib.loc="./duckdb")) update.packages(lib.loc="./duckdb", ask=FALSE, checkBuilt=TRUE, quiet=TRUE, repo="https://cloud.r-project.org/")'
