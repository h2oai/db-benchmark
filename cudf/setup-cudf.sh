# install gcc-7 g++-7 required for CUDA toolkit 9.2
# setup gcc-7 and g++-7 to be default gcc and g++
# download and install cuda 9.2 toolkit

# setup paths
export LD_LIBRARY_PATH=/usr/local/cuda-9.2/bin:/usr/local/cuda-9.2/lib64:$LD_LIBRARY_PATH
export PATH=/usr/local/cuda-9.2/bin:$PATH

# confirm 9.2 used
nvcc --version

# conda env
conda install --name cudf -c nvidia -c rapidsai -c numba -c conda-forge -c pytorch -c defaults cudf=0.8 cuml=0.8 cugraph=0.8 python=3.6 cudatoolkit=9.2
# test
conda activate cudf
python -c 'import cudf'
conda install psutil

x = cudf.read_csv("data/G1_1e7_1e2_0_0.csv", skiprows=1, # header arg not there yet as of cudf 0.4
                  names=['id1','id2','id3','id4','id5','id6','v1','v2','v3'],
                  dtype=['str','str','str','int','int','int','int','int','float']) # category type not supported
# factor type does not yet work
#x = cudf.read_csv("data/G1_1e7_1e2_0_0.csv", skiprows=1, # header arg not there yet as of cudf 0.4
#                  names=['id1','id2','id3','id4','id5','id6','v1','v2','v3'],
#                  dtype=['category','category','category','int','int','int','int','int','float']) # category type not supported

print(x.head(3))
