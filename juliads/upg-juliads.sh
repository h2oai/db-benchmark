#!/bin/bash
set -e

# upgrade to latest devel
echo 'upgrading juliads...'
julia -q -e 'using Pkg; Pkg.update();' > /dev/null 2>&1

