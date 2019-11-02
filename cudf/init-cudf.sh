#!/bin/bash
set -e

echo 'NOT upgrading cudf...' # fixed to 0.9.0 as of now, probably better to remove conda env and install newer version in clean environment

# https://stackoverflow.com/questions/52779016/conda-command-working-in-command-prompt-but-not-in-bash-script
#source ~/anaconda3/etc/profile.d/conda.sh && conda activate cudf

# install command for current stable release from https://rapids.ai/start.html

conda env remove --name cudf
conda create --name cudf
conda activate cudf
conda install --name cudf -c rapidsai -c nvidia -c conda-forge -c defaults cudf=0.10 python=3.6 cudatoolkit=9.2
conda install --name cudf psutil
conda deactivate
