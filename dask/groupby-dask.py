#!/usr/bin/env python

print("# groupby-dask.py")

import os
import gc
import timeit
import pandas as pd
import dask as dk
import dask.dataframe as dd

exec(open("./helpers.py").read())

src_grp = os.environ['SRC_GRP_LOCAL']

ver = dk.__version__
git = dk.__git_revision__
task = "groupby"
data_name = os.path.basename(src_grp)
solution = "dask"
fun = ".groupby"
cache = "TRUE"

print("loading dataset...")

# try parquet according to suggestions in https://github.com/dask/dask/issues/4001
# parq created with fastparquet for 1e7, 1e8, and spark for 1e9 due to failure to read 1e9 data in
# x.write.option("compression","uncompressed").parquet("G1_1e9_1e2.parq") # full path to file was used
#data_name = "G1_1e9_1e2.csv"
#data_name = data_name[:-3]+"parq" # csv to parq
#x = dd.read_parquet(data_name, engine="fastparquet")
# parquet timings slower, 1e9 not possible to read due to parquet format portability issue of spark-fastparquet

if os.path.isfile(data_name):
  x = dd.read_csv(data_name, na_filter=False, dtype={'id1':'category', 'id2':'category', 'id3':'category'}).persist()
else:
  x = dd.read_csv(src_grp, na_filter=False, dtype={'id1':'category', 'id2':'category', 'id3':'category'}).persist()

in_rows = len(x)
print(in_rows)

print("grouping...")

question = "sum v1 by id1" #1
gc.collect()
t_start = timeit.default_timer()
ans = x.groupby(['id1']).agg({'v1':'sum'}).compute()
print((len(ans), len(ans.columns)))
t = timeit.default_timer() - t_start
out_rows = len(ans)
m = memory_usage()
t_start = timeit.default_timer()
chk = [ans.v1.sum()]
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=in_rows, question=question, out_rows=out_rows, out_cols=len(ans.columns), solution=solution, version=ver, git=git, fun=fun, run=1, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt)
del ans
gc.collect()
t_start = timeit.default_timer()
ans = x.groupby(['id1']).agg({'v1':'sum'}).compute()
print((len(ans), len(ans.columns)))
t = timeit.default_timer() - t_start
out_rows = len(ans)
m = memory_usage()
t_start = timeit.default_timer()
chk = [ans.v1.sum()]
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=in_rows, question=question, out_rows=out_rows, out_cols=len(ans.columns), solution=solution, version=ver, git=git, fun=fun, run=2, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt)
del ans

question = "sum v1 by id1:id2" #2
gc.collect()
t_start = timeit.default_timer()
ans = x.groupby(['id1','id2']).agg({'v1':'sum'}).compute()
print((len(ans), len(ans.columns)))
t = timeit.default_timer() - t_start
out_rows = len(ans)
m = memory_usage()
t_start = timeit.default_timer()
chk = [ans.v1.sum()]
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=in_rows, question=question, out_rows=out_rows, out_cols=len(ans.columns), solution=solution, version=ver, git=git, fun=fun, run=1, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt)
del ans
gc.collect()
t_start = timeit.default_timer()
ans = x.groupby(['id1','id2']).agg({'v1':'sum'}).compute()
print((len(ans), len(ans.columns)))
t = timeit.default_timer() - t_start
out_rows = len(ans)
m = memory_usage()
t_start = timeit.default_timer()
chk = [ans.v1.sum()]
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=in_rows, question=question, out_rows=out_rows, out_cols=len(ans.columns), solution=solution, version=ver, git=git, fun=fun, run=2, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt)
del ans

question = "sum v1 mean v3 by id3" #3
gc.collect()
t_start = timeit.default_timer()
ans = x.groupby(['id3']).agg({'v1':'sum', 'v3':'mean'}).compute()
print((len(ans), len(ans.columns)))
t = timeit.default_timer() - t_start
out_rows = len(ans)
m = memory_usage()
t_start = timeit.default_timer()
chk = [ans.v1.sum(), ans.v3.sum()]
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=in_rows, question=question, out_rows=out_rows, out_cols=len(ans.columns), solution=solution, version=ver, git=git, fun=fun, run=1, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt)
del ans
gc.collect()
t_start = timeit.default_timer()
ans = x.groupby(['id3']).agg({'v1':'sum', 'v3':'mean'}).compute()
print((len(ans), len(ans.columns)))
t = timeit.default_timer() - t_start
out_rows = len(ans)
m = memory_usage()
t_start = timeit.default_timer()
chk = [ans.v1.sum(), ans.v3.sum()]
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=in_rows, question=question, out_rows=out_rows, out_cols=len(ans.columns), solution=solution, version=ver, git=git, fun=fun, run=2, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt)
del ans

question = "mean v1:v3 by id4" #4
gc.collect()
t_start = timeit.default_timer()
ans = x.groupby(['id4']).agg({'v1':'mean', 'v2':'mean', 'v3':'mean'}).compute()
print((len(ans), len(ans.columns)))
t = timeit.default_timer() - t_start
out_rows = len(ans)
m = memory_usage()
t_start = timeit.default_timer()
chk = [ans.v1.sum(), ans.v2.sum(), ans.v3.sum()]
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=in_rows, question=question, out_rows=out_rows, out_cols=len(ans.columns), solution=solution, version=ver, git=git, fun=fun, run=1, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt)
del ans
gc.collect()
t_start = timeit.default_timer()
ans = x.groupby(['id4']).agg({'v1':'mean', 'v2':'mean', 'v3':'mean'}).compute()
print((len(ans), len(ans.columns)))
t = timeit.default_timer() - t_start
out_rows = len(ans)
m = memory_usage()
t_start = timeit.default_timer()
chk = [ans.v1.sum(), ans.v2.sum(), ans.v3.sum()]
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=in_rows, question=question, out_rows=out_rows, out_cols=len(ans.columns), solution=solution, version=ver, git=git, fun=fun, run=2, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt)
del ans

question = "sum v1:v3 by id6" #5
gc.collect()
t_start = timeit.default_timer()
ans = x.groupby(['id6']).agg({'v1':'sum', 'v2':'sum', 'v3':'sum'}).compute()
print((len(ans), len(ans.columns)))
t = timeit.default_timer() - t_start
out_rows = len(ans)
m = memory_usage()
t_start = timeit.default_timer()
chk = [ans.v1.sum(), ans.v2.sum(), ans.v3.sum()]
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=in_rows, question=question, out_rows=out_rows, out_cols=len(ans.columns), solution=solution, version=ver, git=git, fun=fun, run=1, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt)
del ans
gc.collect()
t_start = timeit.default_timer()
ans = x.groupby(['id6']).agg({'v1':'sum', 'v2':'sum', 'v3':'sum'}).compute()
print((len(ans), len(ans.columns)))
t = timeit.default_timer() - t_start
out_rows = len(ans)
m = memory_usage()
t_start = timeit.default_timer()
chk = [ans.v1.sum(), ans.v2.sum(), ans.v3.sum()]
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=in_rows, question=question, out_rows=out_rows, out_cols=len(ans.columns), solution=solution, version=ver, git=git, fun=fun, run=2, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt)
del ans

exit(0)
