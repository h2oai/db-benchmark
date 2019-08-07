#!/bin/bash
set -e

echo 'NOT upgrading cudf...' # fixed to 0.8.0 as of now, probably better to remove conda env and install newer version in clean environment

# https://stackoverflow.com/questions/52779016/conda-command-working-in-command-prompt-but-not-in-bash-script
source ~/anaconda3/etc/profile.d/conda.sh && conda activate cudf

conda install --name cudf -c nvidia -c rapidsai -c numba -c conda-forge -c pytorch -c defaults cudf=0.8 cuml=0.8 cugraph=0.8 python=3.6 cudatoolkit=9.2

conda deactivate
