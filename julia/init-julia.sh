#!/bin/bash
set -e

# upgrade to latest devel
echo 'upgrading Julia DataFrames...'
julia -q -e 'using Pkg; Pkg.update();' > /dev/null
