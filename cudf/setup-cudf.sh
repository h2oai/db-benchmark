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
python
import cudf as cu
cu.__version__.split("+", 1)[0]
x = cu.read_csv("data/G1_1e7_1e2_0_0.csv", skiprows=1,
                names=['id1','id2','id3','id4','id5','id6','v1','v2','v3'],
                dtype=['str','str','str','int32','int32','int32','int32','int32','float64'])
x['id1'] = x['id1'].astype('category') # not yet implemented rapidsai/cudf#2644
x['id2'] = x['id2'].astype('category')
x['id3'] = x['id3'].astype('category')
ans = x.groupby(['id1'],as_index=False).agg({'v1':'sum'}).reset_index(drop=True)
print(len(x.index), flush=True)
print(x.head(3), flush=True)
print(x.dtypes, flush=True)
exit()
conda deactivate
