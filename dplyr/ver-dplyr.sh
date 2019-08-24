#!/bin/bash
set -e

Rscript -e 'v=read.dcf(system.file(package="dplyr", "DESCRIPTION"), fields=c("Version","RemoteSha")); colnames(v)[colnames(v)=="RemoteSha"]="Revision"; invisible(mapply(function(f, v) writeLines(v, file.path("dplyr", f)), toupper(colnames(v)), c(v)))'
