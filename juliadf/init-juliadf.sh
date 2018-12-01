#!/bin/bash
set -e

# upgrade to latest devel
echo 'upgrading juliadf...'
julia -q -e 'using Pkg; Pkg.update();' > /dev/null

julia -q -e 'using Pkg; f=open("juliadf/VERSION","w"); write(f, string(Pkg.installed()["DataFrames"]));' > /dev/null
