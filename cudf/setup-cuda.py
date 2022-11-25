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

