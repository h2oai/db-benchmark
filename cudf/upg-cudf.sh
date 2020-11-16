#!/bin/bash
set -e

echo 'NOT upgrading cudf...' # fixed version as of now, we manully remove conda env and install newer version in new clean environment

# https://stackoverflow.com/questions/52779016/conda-command-working-in-command-prompt-but-not-in-bash-script
#source ~/anaconda3/etc/profile.d/conda.sh && conda activate cudf

# install command for current stable release from https://rapids.ai/start.html

conda env remove -y --name cudf
conda create -y --name cudf
conda activate cudf
#conda install --name cudf -c rapidsai -c nvidia -c conda-forge -c defaults cudf=0.11 python=3.6 cudatoolkit=10.0
#conda install --name cudf -c rapidsai -c nvidia -c conda-forge -c defaults dask-cudf=0.11 #116
#conda install --name cudf -c rapidsai -c nvidia -c conda-forge -c defaults cudf=0.12 python=3.6 cudatoolkit=10.0
#conda install -y --name cudf -c rapidsai -c nvidia -c conda-forge -c defaults cudf=0.13 python=3.6 cudatoolkit=10.0
conda install -y --name cudf -c rapidsai -c nvidia -c conda-forge -c defaults cudf=0.16 python=3.8 cudatoolkit=11.0
conda install -y --name cudf psutil
conda deactivate
