#!/bin/bash
set -e

# upgrade all packages in duckdb library only if new arrow is out
echo 'upgrading duckdb...'
Rscript -e 'ap=available.packages(repos="https://cloud.r-project.org/"); if (ap["duckdb","Version"]!=packageVersion("duckdb", lib.loc="./duckdb/r-duckdb")) update.packages(lib.loc="./duckdb/r-duckdb", ask=FALSE, checkBuilt=TRUE, quiet=TRUE, repos="https://cloud.r-project.org/")'
