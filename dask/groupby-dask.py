#!/usr/bin/env python

print("# groupby-dask.py", flush=True)

import os
import gc
import timeit
import pandas as pd
import dask as dk
import dask.dataframe as dd

exec(open("./helpers.py").read())

ver = dk.__version__
git = dk.__git_revision__
task = "groupby"
solution = "dask"
fun = ".groupby"
cache = "TRUE"

data_name = os.environ['SRC_GRP_LOCAL']
src_grp = os.path.join("data", data_name+".csv")
print("loading dataset %s" % data_name, flush=True)

# loading from feather dask/dask#1277
#import feather # temp fix for pandas-dev/pandas#16359
#x = pd.DataFrame(feather.read_dataframe(os.path.join("data", src_grp)))
##x = pd.read_feather(os.path.join("data", src_grp), use_threads=True)

# try parquet according to suggestions in https://github.com/dask/dask/issues/4001
# parq created with fastparquet for 1e7, 1e8, and spark for 1e9 due to failure to read 1e9 data in
# x.write.option("compression","uncompressed").parquet("G1_1e9_1e2.parq") # full path to file was used
#data_name = "G1_1e9_1e2.csv"
#data_name = data_name[:-3]+"parq" # csv to parq
#x = dd.read_parquet(data_name, engine="fastparquet")
# parquet timings slower, 1e9 not possible to read due to parquet format portability issue of spark-fastparquet

x = dd.read_csv(src_grp, na_filter=False, dtype={'id1':'category', 'id2':'category', 'id3':'category'}).persist()
in_rows = len(x)
print(in_rows, flush=True)

print("grouping...", flush=True)

question = "sum v1 by id1" # q1
gc.collect()
t_start = timeit.default_timer()
ans = x.groupby(['id1']).agg({'v1':'sum'}).compute()
ans.reset_index(inplace=True) # #68
print(ans.shape, flush=True)
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = [ans.v1.sum()]
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=in_rows, question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=1, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt)
del ans
gc.collect()
t_start = timeit.default_timer()
ans = x.groupby(['id1']).agg({'v1':'sum'}).compute()
ans.reset_index(inplace=True)
print(ans.shape, flush=True)
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = [ans.v1.sum()]
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=in_rows, question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=2, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt)
print(ans.head(3), flush=True)
print(ans.tail(3), flush=True)
del ans

question = "sum v1 by id1:id2" # q2
gc.collect()
t_start = timeit.default_timer()
ans = x.groupby(['id1','id2']).agg({'v1':'sum'}).compute()
ans.reset_index(inplace=True)
print(ans.shape, flush=True)
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = [ans.v1.sum()]
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=in_rows, question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=1, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt)
del ans
gc.collect()
t_start = timeit.default_timer()
ans = x.groupby(['id1','id2']).agg({'v1':'sum'}).compute()
ans.reset_index(inplace=True)
print(ans.shape, flush=True)
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = [ans.v1.sum()]
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=in_rows, question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=2, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt)
print(ans.head(3), flush=True)
print(ans.tail(3), flush=True)
del ans

question = "sum v1 mean v3 by id3" # q3
gc.collect()
t_start = timeit.default_timer()
ans = x.groupby(['id3']).agg({'v1':'sum', 'v3':'mean'}).compute()
ans.reset_index(inplace=True)
print(ans.shape, flush=True)
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = [ans.v1.sum(), ans.v3.sum()]
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=in_rows, question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=1, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt)
del ans
gc.collect()
t_start = timeit.default_timer()
ans = x.groupby(['id3']).agg({'v1':'sum', 'v3':'mean'}).compute()
ans.reset_index(inplace=True)
print(ans.shape, flush=True)
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = [ans.v1.sum(), ans.v3.sum()]
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=in_rows, question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=2, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt)
print(ans.head(3), flush=True)
print(ans.tail(3), flush=True)
del ans

question = "mean v1:v3 by id4" # q4
gc.collect()
t_start = timeit.default_timer()
ans = x.groupby(['id4']).agg({'v1':'mean', 'v2':'mean', 'v3':'mean'}).compute()
ans.reset_index(inplace=True)
print(ans.shape, flush=True)
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = [ans.v1.sum(), ans.v2.sum(), ans.v3.sum()]
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=in_rows, question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=1, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt)
del ans
gc.collect()
t_start = timeit.default_timer()
ans = x.groupby(['id4']).agg({'v1':'mean', 'v2':'mean', 'v3':'mean'}).compute()
ans.reset_index(inplace=True)
print(ans.shape, flush=True)
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = [ans.v1.sum(), ans.v2.sum(), ans.v3.sum()]
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=in_rows, question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=2, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt)
print(ans.head(3), flush=True)
print(ans.tail(3), flush=True)
del ans

question = "sum v1:v3 by id6" # q5
gc.collect()
t_start = timeit.default_timer()
ans = x.groupby(['id6']).agg({'v1':'sum', 'v2':'sum', 'v3':'sum'}).compute()
ans.reset_index(inplace=True)
print(ans.shape, flush=True)
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = [ans.v1.sum(), ans.v2.sum(), ans.v3.sum()]
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=in_rows, question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=1, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt)
del ans
gc.collect()
t_start = timeit.default_timer()
ans = x.groupby(['id6']).agg({'v1':'sum', 'v2':'sum', 'v3':'sum'}).compute()
ans.reset_index(inplace=True)
print(ans.shape, flush=True)
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = [ans.v1.sum(), ans.v2.sum(), ans.v3.sum()]
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=in_rows, question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=2, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt)
print(ans.head(3), flush=True)
print(ans.tail(3), flush=True)
del ans

#question = "median v3 sd v3 by id2 id4" # q6 # median function not yet implemented: https://github.com/dask/dask/issues/4362
#gc.collect()
#t_start = timeit.default_timer()
#ans = x.groupby(['id2','id4']).agg({'v3': ['median','std']}).compute()
#ans.reset_index(inplace=True)
#print(ans.shape, flush=True)
#t = timeit.default_timer() - t_start
#m = memory_usage()
#t_start = timeit.default_timer()
#chk = [ans['v3']['median'].sum(), ans['v3']['std'].sum()]
#chkt = timeit.default_timer() - t_start
#write_log(task=task, data=data_name, in_rows=in_rows, question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=1, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt)
#del ans
#gc.collect()
#t_start = timeit.default_timer()
#ans = x.groupby(['id2','id4']).agg({'v3': ['median','std']}).compute()
#ans.reset_index(inplace=True)
#print(ans.shape, flush=True)
#t = timeit.default_timer() - t_start
#m = memory_usage()
#t_start = timeit.default_timer()
#chk = [ans['v3']['median'].sum(), ans['v3']['std'].sum()]
#chkt = timeit.default_timer() - t_start
#write_log(task=task, data=data_name, in_rows=in_rows, question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=2, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt)
#print(ans.head(3), flush=True)
#print(ans.tail(3), flush=True)
#del ans

question = "max v1 - min v2 by id2 id4" # q7
gc.collect()
t_start = timeit.default_timer()
ans = x.groupby(['id2','id4']).agg({'v1': 'max', 'v2': 'min'}).assign(range_v1_v2=lambda x: x['v1'] - x['v2'])[['range_v1_v2']].compute()
ans.reset_index(inplace=True)
print(ans.shape, flush=True)
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = [ans['range_v1_v2'].sum()]
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=in_rows, question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=1, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt)
del ans
gc.collect()
t_start = timeit.default_timer()
ans = x.groupby(['id2','id4']).agg({'v1': 'max', 'v2': 'min'}).assign(range_v1_v2=lambda x: x['v1'] - x['v2'])[['range_v1_v2']].compute()
ans.reset_index(inplace=True)
print(ans.shape, flush=True)
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = [ans['range_v1_v2'].sum()]
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=in_rows, question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=2, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt)
print(ans.head(3), flush=True)
print(ans.tail(3), flush=True)
del ans

question = "largest two v3 by id2 id4" # q8
gc.collect()
t_start = timeit.default_timer()
ans = x[['id2','id4','v3']].groupby(['id2','id4']).apply(lambda x: x.nlargest(2, columns='v3'), meta={'id2': 'category', 'id4': 'int64', 'v3': 'float64'})[['v3']].compute()
ans.reset_index(level=("id2","id4"), inplace=True)
ans.reset_index(drop=True, inplace=True) # drop because nlargest creates some extra new index field
print(ans.shape, flush=True)
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = [ans['v3'].sum()]
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=in_rows, question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=1, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt)
del ans
gc.collect()
t_start = timeit.default_timer()
ans = x[['id2','id4','v3']].groupby(['id2','id4']).apply(lambda x: x.nlargest(2, columns='v3'), meta={'id2': 'category', 'id4': 'int64', 'v3': 'float64'})[['v3']].compute()
ans.reset_index(level=("id2","id4"), inplace=True)
ans.reset_index(drop=True, inplace=True)
print(ans.shape, flush=True)
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = [ans['v3'].sum()]
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=in_rows, question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=2, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt)
print(ans.head(3), flush=True)
print(ans.tail(3), flush=True)
del ans

#question = "regression v1 v2 by id2 id4" # q9 - rewrite using dd.Aggregation https://github.com/dask/dask/issues/4372#issuecomment-493491021
#gc.collect()
#t_start = timeit.default_timer()
#ans = x[['id2','id4','v1','v2']].groupby(['id2','id4']).apply(lambda x: pd.Series({'r2': x.corr()['v1']['v2']**2}), meta={'r2': 'float64'}).compute()
#ans.reset_index(inplace=True)
#print(ans.shape, flush=True)
#t = timeit.default_timer() - t_start
#m = memory_usage()
#t_start = timeit.default_timer()
#chk = [ans['r2'].sum()]
#chkt = timeit.default_timer() - t_start
#write_log(task=task, data=data_name, in_rows=in_rows, question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=1, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt)
#del ans
#gc.collect()
#t_start = timeit.default_timer()
#ans = x[['id2','id4','v1','v2']].groupby(['id2','id4']).apply(lambda x: pd.Series({'r2': x.corr()['v1']['v2']**2}), meta={'r2': 'float64'}).compute()
#ans.reset_index(inplace=True)
#print(ans.shape, flush=True)
#t = timeit.default_timer() - t_start
#m = memory_usage()
#t_start = timeit.default_timer()
#chk = [ans['r2'].sum()]
#chkt = timeit.default_timer() - t_start
#write_log(task=task, data=data_name, in_rows=in_rows, question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=2, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt)
#print(ans.head(3), flush=True)
#print(ans.tail(3), flush=True)
#del ans

question = "sum v3 count by id1:id6" # q10
gc.collect()
t_start = timeit.default_timer()
ans = x.groupby(['id1','id2','id3','id4','id5','id6']).agg({'v3':'sum', 'v1':'count'}).compute() # column name different than expected, ignore it because: ValueError: Metadata inference failed in `rename`: Original error is below: ValueError('Level values must be unique: [nan, nan] on level 0',)
ans.reset_index(inplace=True)
print(ans.shape, flush=True)
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = [ans.v3.sum(), ans.v1.sum()]
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=in_rows, question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=1, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt)
del ans
gc.collect()
t_start = timeit.default_timer()
ans = x.groupby(['id1','id2','id3','id4','id5','id6']).agg({'v3':'sum', 'v1':'count'}).compute()
ans.reset_index(inplace=True)
print(ans.shape, flush=True)
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = [ans.v3.sum(), ans.v1.sum()]
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=in_rows, question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=2, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt)
print(ans.head(3), flush=True)
print(ans.tail(3), flush=True)
del ans

exit(0)
