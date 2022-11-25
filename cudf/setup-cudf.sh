# install gcc-7 g++-7 required for CUDA toolkit 9.2
# setup gcc-7 and g++-7 to be default gcc and g++
# download and install cuda 9.2 toolkit
sudo apt install nvidia-cuda-toolkit

# setup paths
export LD_LIBRARY_PATH=/usr/local/cuda-9.2/bin:/usr/local/cuda-9.2/lib64:$LD_LIBRARY_PATH
export PATH=/usr/local/cuda-9.2/bin:$PATH

# confirm 9.2 used
nvcc --version

# setup conda env based on cudf/upg-cudf.py

# test
conda activate cudf
python3 -m pip install cudf
python3 cudf/setup-cuda.py
conda deactivate
