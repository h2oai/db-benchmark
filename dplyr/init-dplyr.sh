#!/bin/bash
set -e

# upgrade to latest devel
echo 'upgrading dplyr...'
Rscript -e 'devtools::install_github("tidyverse/dplyr", quiet=TRUE, method="curl"); v=read.dcf(system.file(package="dplyr", "DESCRIPTION"), fields=c("Version","RemoteSha")); colnames(v)[colnames(v)=="RemoteSha"]="Revision"; invisible(mapply(function(f, v) writeLines(v, file.path("dplyr", f)), toupper(colnames(v)), c(v)))'
