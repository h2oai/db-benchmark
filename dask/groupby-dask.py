#!/usr/bin/env python

print("# groupby-dask.py")

import os
import gc
import timeit
import pandas as pd
import dask as dk
import dask.dataframe as dd
from dask.distributed import Client

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

if os.path.isfile(data_name):
  x = dd.read_csv(data_name, na_filter=False)
else:
  x = dd.read_csv(src_grp, na_filter=False)

client = Client(processes=False)
x = client.persist(x)
in_rows = len(x)
print(in_rows)

print("grouping...")

question = "sum v1 by id1" #1
gc.collect()
t_start = timeit.default_timer()
ans = x.groupby(['id1']).agg({'v1':'sum'})
ans = client.persist(ans)
print((len(ans), len(ans.columns)))
t = timeit.default_timer() - t_start
out_rows = len(ans)
m = memory_usage()
t_start = timeit.default_timer()
chk = ans.sum().compute()
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=in_rows, question=question, out_rows=out_rows, out_cols=len(ans.columns), solution=solution, version=ver, git=git, fun=fun, run=1, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt)
del ans
gc.collect()
t_start = timeit.default_timer()
ans = x.groupby(['id1']).agg({'v1':'sum'})
ans = client.persist(ans)
print((len(ans), len(ans.columns)))
t = timeit.default_timer() - t_start
out_rows = len(ans)
m = memory_usage()
t_start = timeit.default_timer()
chk = ans.sum().compute()
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=in_rows, question=question, out_rows=out_rows, out_cols=len(ans.columns), solution=solution, version=ver, git=git, fun=fun, run=2, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt)
del ans
gc.collect()
t_start = timeit.default_timer()
ans = x.groupby(['id1']).agg({'v1':'sum'})
ans = client.persist(ans)
print((len(ans), len(ans.columns)))
t = timeit.default_timer() - t_start
out_rows = len(ans)
m = memory_usage()
t_start = timeit.default_timer()
chk = ans.sum().compute()
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=in_rows, question=question, out_rows=out_rows, out_cols=len(ans.columns), solution=solution, version=ver, git=git, fun=fun, run=3, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt)
del ans

question = "sum v1 by id1:id2" #2
gc.collect()
t_start = timeit.default_timer()
ans = x.groupby(['id1','id2']).agg({'v1':'sum'})
ans = client.persist(ans)
print((len(ans), len(ans.columns)))
t = timeit.default_timer() - t_start
out_rows = len(ans)
m = memory_usage()
t_start = timeit.default_timer()
chk = ans.sum().compute()
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=in_rows, question=question, out_rows=out_rows, out_cols=len(ans.columns), solution=solution, version=ver, git=git, fun=fun, run=1, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt)
del ans
gc.collect()
t_start = timeit.default_timer()
ans = x.groupby(['id1','id2']).agg({'v1':'sum'})
ans = client.persist(ans)
print((len(ans), len(ans.columns)))
t = timeit.default_timer() - t_start
out_rows = len(ans)
m = memory_usage()
t_start = timeit.default_timer()
chk = ans.sum().compute()
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=in_rows, question=question, out_rows=out_rows, out_cols=len(ans.columns), solution=solution, version=ver, git=git, fun=fun, run=2, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt)
del ans
gc.collect()
t_start = timeit.default_timer()
ans = x.groupby(['id1','id2']).agg({'v1':'sum'})
ans = client.persist(ans)
print((len(ans), len(ans.columns)))
t = timeit.default_timer() - t_start
out_rows = len(ans)
m = memory_usage()
t_start = timeit.default_timer()
chk = ans.sum().compute()
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=in_rows, question=question, out_rows=out_rows, out_cols=len(ans.columns), solution=solution, version=ver, git=git, fun=fun, run=3, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt)
del ans

question = "sum v1 mean v3 by id3" #3
gc.collect()
t_start = timeit.default_timer()
ans = x.groupby(['id3']).agg({'v1':'sum', 'v3':'mean'})
ans = client.persist(ans)
print((len(ans), len(ans.columns)))
t = timeit.default_timer() - t_start
out_rows = len(ans)
m = memory_usage()
t_start = timeit.default_timer()
chk = [ans['v1'].sum().compute(), ans['v3'].sum().compute()]
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=in_rows, question=question, out_rows=out_rows, out_cols=len(ans.columns), solution=solution, version=ver, git=git, fun=fun, run=1, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt)
del ans
gc.collect()
t_start = timeit.default_timer()
ans = x.groupby(['id3']).agg({'v1':'sum', 'v3':'mean'})
ans = client.persist(ans)
print((len(ans), len(ans.columns)))
t = timeit.default_timer() - t_start
out_rows = len(ans)
m = memory_usage()
t_start = timeit.default_timer()
chk = [ans['v1'].sum().compute(), ans['v3'].sum().compute()]
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=in_rows, question=question, out_rows=out_rows, out_cols=len(ans.columns), solution=solution, version=ver, git=git, fun=fun, run=2, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt)
del ans
gc.collect()
t_start = timeit.default_timer()
ans = x.groupby(['id3']).agg({'v1':'sum', 'v3':'mean'})
ans = client.persist(ans)
print((len(ans), len(ans.columns)))
t = timeit.default_timer() - t_start
out_rows = len(ans)
m = memory_usage()
t_start = timeit.default_timer()
chk = [ans['v1'].sum().compute(), ans['v3'].sum().compute()]
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=in_rows, question=question, out_rows=out_rows, out_cols=len(ans.columns), solution=solution, version=ver, git=git, fun=fun, run=3, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt)
del ans

question = "mean v1:v3 by id4" #4
gc.collect()
t_start = timeit.default_timer()
ans = x.groupby(['id4']).agg({'v1':'mean', 'v2':'mean', 'v3':'mean'})
ans = client.persist(ans)
print((len(ans), len(ans.columns)))
t = timeit.default_timer() - t_start
out_rows = len(ans)
m = memory_usage()
t_start = timeit.default_timer()
chk = [ans['v1'].sum().compute(), ans['v2'].sum().compute(), ans['v3'].sum().compute()]
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=in_rows, question=question, out_rows=out_rows, out_cols=len(ans.columns), solution=solution, version=ver, git=git, fun=fun, run=1, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt)
del ans
gc.collect()
t_start = timeit.default_timer()
ans = x.groupby(['id4']).agg({'v1':'mean', 'v2':'mean', 'v3':'mean'})
ans = client.persist(ans)
print((len(ans), len(ans.columns)))
t = timeit.default_timer() - t_start
out_rows = len(ans)
m = memory_usage()
t_start = timeit.default_timer()
chk = [ans['v1'].sum().compute(), ans['v2'].sum().compute(), ans['v3'].sum().compute()]
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=in_rows, question=question, out_rows=out_rows, out_cols=len(ans.columns), solution=solution, version=ver, git=git, fun=fun, run=2, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt)
del ans
gc.collect()
t_start = timeit.default_timer()
ans = x.groupby(['id4']).agg({'v1':'mean', 'v2':'mean', 'v3':'mean'})
ans = client.persist(ans)
print((len(ans), len(ans.columns)))
t = timeit.default_timer() - t_start
out_rows = len(ans)
m = memory_usage()
t_start = timeit.default_timer()
chk = [ans['v1'].sum().compute(), ans['v2'].sum().compute(), ans['v3'].sum().compute()]
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=in_rows, question=question, out_rows=out_rows, out_cols=len(ans.columns), solution=solution, version=ver, git=git, fun=fun, run=3, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt)
del ans

question = "sum v1:v3 by id6" #5
gc.collect()
t_start = timeit.default_timer()
ans = x.groupby(['id6']).agg({'v1':'sum', 'v2':'sum', 'v3':'sum'})
ans = client.persist(ans)
print((len(ans), len(ans.columns)))
t = timeit.default_timer() - t_start
out_rows = len(ans)
m = memory_usage()
t_start = timeit.default_timer()
chk = [ans['v1'].sum().compute(), ans['v2'].sum().compute(), ans['v3'].sum().compute()]
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=in_rows, question=question, out_rows=out_rows, out_cols=len(ans.columns), solution=solution, version=ver, git=git, fun=fun, run=1, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt)
del ans
gc.collect()
t_start = timeit.default_timer()
ans = x.groupby(['id6']).agg({'v1':'sum', 'v2':'sum', 'v3':'sum'})
ans = client.persist(ans)
print((len(ans), len(ans.columns)))
t = timeit.default_timer() - t_start
out_rows = len(ans)
m = memory_usage()
t_start = timeit.default_timer()
chk = [ans['v1'].sum().compute(), ans['v2'].sum().compute(), ans['v3'].sum().compute()]
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=in_rows, question=question, out_rows=out_rows, out_cols=len(ans.columns), solution=solution, version=ver, git=git, fun=fun, run=2, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt)
del ans
gc.collect()
t_start = timeit.default_timer()
ans = x.groupby(['id6']).agg({'v1':'sum', 'v2':'sum', 'v3':'sum'})
ans = client.persist(ans)
print((len(ans), len(ans.columns)))
t = timeit.default_timer() - t_start
out_rows = len(ans)
m = memory_usage()
t_start = timeit.default_timer()
chk = [ans['v1'].sum().compute(), ans['v2'].sum().compute(), ans['v3'].sum().compute()]
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=in_rows, question=question, out_rows=out_rows, out_cols=len(ans.columns), solution=solution, version=ver, git=git, fun=fun, run=3, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt)
del ans

exit(0)
