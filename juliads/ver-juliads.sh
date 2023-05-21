#!/bin/bash
set -e

julia -q -e 'include("$(pwd())/_helpers/helpersds.jl"); pkgmeta = getpkgmeta("InMemoryDatasets"); f=open("juliads/VERSION","w"); write(f, string(pkgmeta["version"])); f=open("juliads/REVISION","w"); write(f, string(" "));' > /dev/null
