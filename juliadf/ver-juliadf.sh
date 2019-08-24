#!/bin/bash
set -e

julia -q -e 'using Pkg; include("$(pwd())/helpers.jl"); pkgmeta = getpkgmeta("DataFrames"); f=open("juliadf/VERSION","w"); write(f, string(pkgmeta["version"])); f=open("juliadf/REVISION","w"); write(f, string(pkgmeta["git-tree-sha1"]));' > /dev/null
