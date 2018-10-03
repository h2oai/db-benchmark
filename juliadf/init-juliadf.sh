#!/bin/bash
set -e

# upgrade to latest devel
echo 'upgrading juliadf...'
julia -q -e 'using Pkg; Pkg.update();' > /dev/null
