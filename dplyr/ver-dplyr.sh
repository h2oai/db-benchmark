#!/bin/bash
set -e

Rscript -e 'v=read.dcf(system.file(package="dplyr", lib.loc="./dplyr/r-dplyr", "DESCRIPTION"), fields=c("Version","RemoteSha")); colnames(v)[colnames(v)=="RemoteSha"]="Revision"; cnafill=function(x) {x=c(x); x[is.na(x)]=""; x}; fw=function(f, v) writeLines(v, file.path("dplyr", f)); invisible(mapply(fw, toupper(colnames(v)), cnafill(v)))'
