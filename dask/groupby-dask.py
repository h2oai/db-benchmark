#!/usr/bin/env python

print("# groupby-dask.py")

import os
import gc
import timeit
import pandas as pd
import dask as dk
import dask.dataframe as dd
#import pydoop.hdfs as hd
from distributed import Executor

execfile("./helpers.py")

src_grp = os.path.basename(os.environ['SRC_GRP_LOCAL'])

ver = dk.__version__
print(ver)
task = "groupby"
data_name = os.path.basename(src_grp)
solution = "dask"
fun = "groupby"
cache = "TRUE"

e = Executor(os.environ['MASTER'] + ":8786")
e

print("loading dataset...")
#with hd.open(src_grp) as f:
#  x = pd.read_csv(f)
x = pd.read_csv(src_grp)
x = dd.from_pandas(x, npartitions=9)
x = e.persist(x)
in_rows = len(x.index)

print("grouping...")

question = "sum v1 by id1" #1
gc.collect()
t_start = timeit.default_timer()
ans = x.groupby(['id1']).v1.sum()
ans = e.persist(ans)
print len(ans.index)
t = timeit.default_timer() - t_start
out_rows = len(ans.index)
m = float('NaN')
t_start = timeit.default_timer()
chk = [ans.sum().compute()]
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=in_rows, question=question, out_rows=out_rows, solution=solution, version=ver, fun=fun, run=1, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt)
del ans
gc.collect()
t_start = timeit.default_timer()
ans = x.groupby(['id1']).v1.sum()
ans = e.persist(ans)
print len(ans.index)
t = timeit.default_timer() - t_start
out_rows = len(ans.index)
m = float('NaN')
t_start = timeit.default_timer()
chk = [ans.sum().compute()]
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=in_rows, question=question, out_rows=out_rows, solution=solution, version=ver, fun=fun, run=2, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt)
del ans
gc.collect()
t_start = timeit.default_timer()
ans = x.groupby(['id1']).v1.sum()
ans = e.persist(ans)
print len(ans.index)
t = timeit.default_timer() - t_start
out_rows = len(ans.index)
m = float('NaN')
t_start = timeit.default_timer()
chk = [ans.sum().compute()]
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=in_rows, question=question, out_rows=out_rows, solution=solution, version=ver, fun=fun, run=3, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt)
del ans

question = "sum v1 by id1:id2" #2
gc.collect()
t_start = timeit.default_timer()
ans = x.groupby(['id1','id2']).v1.sum()
ans = e.persist(ans)
print len(ans.index)
t = timeit.default_timer() - t_start
out_rows = len(ans.index)
m = float('NaN')
t_start = timeit.default_timer()
chk = [ans.sum().compute()]
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=in_rows, question=question, out_rows=out_rows, solution=solution, version=ver, fun=fun, run=1, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt)
del ans
gc.collect()
t_start = timeit.default_timer()
ans = x.groupby(['id1','id2']).v1.sum()
ans = e.persist(ans)
print len(ans.index)
t = timeit.default_timer() - t_start
out_rows = len(ans.index)
m = float('NaN')
t_start = timeit.default_timer()
chk = [ans.sum().compute()]
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=in_rows, question=question, out_rows=out_rows, solution=solution, version=ver, fun=fun, run=2, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt)
del ans
gc.collect()
t_start = timeit.default_timer()
ans = x.groupby(['id1','id2']).v1.sum()
ans = e.persist(ans)
print len(ans.index)
t = timeit.default_timer() - t_start
out_rows = len(ans.index)
m = float('NaN')
t_start = timeit.default_timer()
chk = [ans.sum().compute()]
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=in_rows, question=question, out_rows=out_rows, solution=solution, version=ver, fun=fun, run=3, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt)
del ans

question = "sum v1 mean v3 by id3" #3 - workaround from https://github.com/dask/dask/issues/1517
gc.collect()
t_start = timeit.default_timer()
ans1 = x.groupby('id3').v1.sum()
ans2 = x.groupby('id3').v3.mean()
ans1 = e.persist(ans1)
ans2 = e.persist(ans2)
print len(ans1.index)
print len(ans2.index)
t = timeit.default_timer() - t_start
out_rows = len(ans1.index)
m = float('NaN')
t_start = timeit.default_timer()
chk = [ans1.sum().compute(), ans2.sum().compute()]
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=in_rows, question=question, out_rows=out_rows, solution=solution, version=ver, fun=fun, run=1, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt)
del ans1, ans2
gc.collect()
t_start = timeit.default_timer()
ans1 = x.groupby('id3').v1.sum()
ans2 = x.groupby('id3').v3.mean()
ans1 = e.persist(ans1)
ans2 = e.persist(ans2)
print len(ans1.index)
print len(ans2.index)
t = timeit.default_timer() - t_start
out_rows = len(ans1.index)
m = float('NaN')
t_start = timeit.default_timer()
chk = [ans1.sum().compute(), ans2.sum().compute()]
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=in_rows, question=question, out_rows=out_rows, solution=solution, version=ver, fun=fun, run=2, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt)
del ans1, ans2
gc.collect()
t_start = timeit.default_timer()
ans1 = x.groupby('id3').v1.sum()
ans2 = x.groupby('id3').v3.mean()
ans1 = e.persist(ans1)
ans2 = e.persist(ans2)
print len(ans1.index)
print len(ans2.index)
t = timeit.default_timer() - t_start
out_rows = len(ans1.index)
m = float('NaN')
t_start = timeit.default_timer()
chk = [ans1.sum().compute(), ans2.sum().compute()]
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=in_rows, question=question, out_rows=out_rows, solution=solution, version=ver, fun=fun, run=3, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt)
del ans1, ans2

question = "mean v1:v3 by id4" #4 - workaround from https://github.com/dask/dask/issues/1517
gc.collect()
t_start = timeit.default_timer()
ans1 = x.groupby('id4').v1.mean()
ans2 = x.groupby('id4').v2.mean()
ans3 = x.groupby('id4').v3.mean()
ans1 = e.persist(ans1)
ans2 = e.persist(ans2)
ans3 = e.persist(ans3)
print len(ans1.index)
print len(ans2.index)
print len(ans3.index)
t = timeit.default_timer() - t_start
out_rows = len(ans1.index)
m = float('NaN')
t_start = timeit.default_timer()
chk = [ans1.sum().compute(), ans2.sum().compute(), ans3.sum().compute()]
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=in_rows, question=question, out_rows=out_rows, solution=solution, version=ver, fun=fun, run=1, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt)
del ans1, ans2, ans3
gc.collect()
t_start = timeit.default_timer()
ans1 = x.groupby('id4').v1.mean()
ans2 = x.groupby('id4').v2.mean()
ans3 = x.groupby('id4').v3.mean()
ans1 = e.persist(ans1)
ans2 = e.persist(ans2)
ans3 = e.persist(ans3)
print len(ans1.index)
print len(ans2.index)
print len(ans3.index)
t = timeit.default_timer() - t_start
out_rows = len(ans1.index)
m = float('NaN')
t_start = timeit.default_timer()
chk = [ans1.sum().compute(), ans2.sum().compute(), ans3.sum().compute()]
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=in_rows, question=question, out_rows=out_rows, solution=solution, version=ver, fun=fun, run=2, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt)
del ans1, ans2, ans3
gc.collect()
t_start = timeit.default_timer()
ans1 = x.groupby('id4').v1.mean()
ans2 = x.groupby('id4').v2.mean()
ans3 = x.groupby('id4').v3.mean()
ans1 = e.persist(ans1)
ans2 = e.persist(ans2)
ans3 = e.persist(ans3)
print len(ans1.index)
print len(ans2.index)
print len(ans3.index)
t = timeit.default_timer() - t_start
out_rows = len(ans1.index)
m = float('NaN')
t_start = timeit.default_timer()
chk = [ans1.sum().compute(), ans2.sum().compute(), ans3.sum().compute()]
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=in_rows, question=question, out_rows=out_rows, solution=solution, version=ver, fun=fun, run=3, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt)
del ans1, ans2, ans3

question = "sum v1:v3 by id6" #5 - workaround from https://github.com/dask/dask/issues/1517
gc.collect()
t_start = timeit.default_timer()
ans1 = x.groupby('id6').v1.sum()
ans2 = x.groupby('id6').v2.sum()
ans3 = x.groupby('id6').v3.sum()
ans1 = e.persist(ans1)
ans2 = e.persist(ans2)
ans3 = e.persist(ans3)
print len(ans1.index)
print len(ans2.index)
print len(ans3.index)
t = timeit.default_timer() - t_start
out_rows = len(ans1.index)
m = float('NaN')
t_start = timeit.default_timer()
chk = [ans1.sum().compute(), ans2.sum().compute(), ans3.sum().compute()]
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=in_rows, question=question, out_rows=out_rows, solution=solution, version=ver, fun=fun, run=1, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt)
del ans1, ans2, ans3
gc.collect()
t_start = timeit.default_timer()
ans1 = x.groupby('id6').v1.sum()
ans2 = x.groupby('id6').v2.sum()
ans3 = x.groupby('id6').v3.sum()
ans1 = e.persist(ans1)
ans2 = e.persist(ans2)
ans3 = e.persist(ans3)
print len(ans1.index)
print len(ans2.index)
print len(ans3.index)
t = timeit.default_timer() - t_start
out_rows = len(ans1.index)
m = float('NaN')
t_start = timeit.default_timer()
chk = [ans1.sum().compute(), ans2.sum().compute(), ans3.sum().compute()]
write_log(task=task, data=data_name, in_rows=in_rows, question=question, out_rows=out_rows, solution=solution, version=ver, fun=fun, run=2, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt)
del ans1, ans2, ans3
gc.collect()
t_start = timeit.default_timer()
ans1 = x.groupby('id6').v1.sum()
ans2 = x.groupby('id6').v2.sum()
ans3 = x.groupby('id6').v3.sum()
ans1 = e.persist(ans1)
ans2 = e.persist(ans2)
ans3 = e.persist(ans3)
print len(ans1.index)
print len(ans2.index)
print len(ans3.index)
t = timeit.default_timer() - t_start
out_rows = len(ans1.index)
m = float('NaN')
t_start = timeit.default_timer()
chk = [ans1.sum().compute(), ans2.sum().compute(), ans3.sum().compute()]
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=in_rows, question=question, out_rows=out_rows, solution=solution, version=ver, fun=fun, run=3, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt)
del ans1, ans2, ans3

exit(0)
