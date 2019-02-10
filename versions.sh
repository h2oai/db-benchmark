#!/bin/bash
set -e

Rscript -e 'v=read.dcf(system.file(package="data.table", "DESCRIPTION"), fields=c("Version","Revision")); invisible(mapply(function(f, v) writeLines(v, file.path("datatable", f)), toupper(colnames(v)), c(v)))'
Rscript -e 'v=read.dcf(system.file(package="dplyr", "DESCRIPTION"), fields=c("Version","RemoteSha")); colnames(v)[colnames(v)=="RemoteSha"]="Revision"; invisible(mapply(function(f, v) writeLines(v, file.path("dplyr", f)), toupper(colnames(v)), c(v)))'

julia -q -e 'using Pkg; include("$(pwd())/helpers.jl"); pkgmeta = getpkgmeta("DataFrames"); f=open("juliadf/VERSION","w"); write(f, string(pkgmeta["version"])); f=open("juliadf/REVISION","w"); write(f, string(pkgmeta["git-tree-sha1"]));' > /dev/null

source ./dask/py-dask/bin/activate
python -c 'import dask as dk; open("dask/VERSION","w").write(dk.__version__); open("dask/REVISION","w").write(dk.__git_revision__);' > /dev/null
source ./modin/py-modin/bin/activate
python -c 'import modin as modin; open("modin/VERSION","w").write(modin.__version__); open("modin/REVISION","w").write("");' > /dev/null
source ./pandas/py-pandas/bin/activate
python -c 'import pandas as pd; open("pandas/VERSION","w").write(pd.__version__); open("pandas/REVISION","w").write(pd.__git_version__);' > /dev/null 2>1&
source ./pydatatable/py-pydatatable/bin/activate
python -c 'import datatable as dt; open("pydatatable/VERSION","w").write(dt.__version__); open("pydatatable/REVISION","w").write(dt.__git_revision__);' > /dev/null
source ./spark/py-spark/bin/activate
python -c 'import pyspark; open("spark/VERSION","w").write(pyspark.__version__); open("spark/REVISION","w").write("");' > /dev/null
