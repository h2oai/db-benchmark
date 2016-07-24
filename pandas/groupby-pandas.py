#!/usr/bin/env python

print("# groupby-pandas.py")

import os
import gc
import timeit
import pandas as pd
# import pydoop.hdfs as hd

execfile("./helpers.py")

src_grp = os.environ['SRC_GRP_LOCAL']
# TODO skip for total row count > 2e9 as data volume cap due to pandas scalability, currently just comment out in run.sh

ver = pd.__version__
print(ver)
task = "groupby"
data_name = os.path.basename(src_grp)
solution = "pandas"
fun = ".groupby"

print("loading datasets...")

# with hd.open(src_grp) as f:
#   x = pd.read_csv(f)
x = pd.read_csv(data_name)

print("grouping...")

question = "sum v1 by id1" #1
gc.collect()
t_start = timeit.default_timer()
ans = x.groupby(['id1']).agg({'v1':'sum'})
ans.shape[0]
t_end = timeit.default_timer()
t = t_end - t_start
m = float('nan')
write_log(task=task, data=data_name, in_rows=x.shape[0], question=question, out_rows=ans.shape[0], solution=solution, version=ver, fun=fun, run=1, time_sec=t, mem_gb=m)
del ans
gc.collect()
t_start = timeit.default_timer()
ans = x.groupby(['id1']).agg({'v1':'sum'})
ans.shape[0]
t_end = timeit.default_timer()
t = t_end - t_start
m = float('nan')
write_log(task=task, data=data_name, in_rows=x.shape[0], question=question, out_rows=ans.shape[0], solution=solution, version=ver, fun=fun, run=2, time_sec=t, mem_gb=m)
del ans
gc.collect()
t_start = timeit.default_timer()
ans = x.groupby(['id1']).agg({'v1':'sum'})
ans.shape[0]
t_end = timeit.default_timer()
t = t_end - t_start
m = float('nan')
write_log(task=task, data=data_name, in_rows=x.shape[0], question=question, out_rows=ans.shape[0], solution=solution, version=ver, fun=fun, run=3, time_sec=t, mem_gb=m)
del ans

question = "sum v1 by id1:id2" #2
gc.collect()
t_start = timeit.default_timer()
ans = x.groupby(['id1','id2']).agg({'v1':'sum'})
ans.shape[0]
t_end = timeit.default_timer()
t = t_end - t_start
m = float('nan')
write_log(task=task, data=data_name, in_rows=x.shape[0], question=question, out_rows=ans.shape[0], solution=solution, version=ver, fun=fun, run=1, time_sec=t, mem_gb=m)
del ans
gc.collect()
t_start = timeit.default_timer()
ans = x.groupby(['id1','id2']).agg({'v1':'sum'})
ans.shape[0]
t_end = timeit.default_timer()
t = t_end - t_start
m = float('nan')
write_log(task=task, data=data_name, in_rows=x.shape[0], question=question, out_rows=ans.shape[0], solution=solution, version=ver, fun=fun, run=2, time_sec=t, mem_gb=m)
del ans
gc.collect()
t_start = timeit.default_timer()
ans = x.groupby(['id1','id2']).agg({'v1':'sum'})
ans.shape[0]
t_end = timeit.default_timer()
t = t_end - t_start
m = float('nan')
write_log(task=task, data=data_name, in_rows=x.shape[0], question=question, out_rows=ans.shape[0], solution=solution, version=ver, fun=fun, run=3, time_sec=t, mem_gb=m)
del ans

question = "sum v1 mean v3 by id3" #3
gc.collect()
t_start = timeit.default_timer()
ans = x.groupby(['id3']).agg({'v1':'sum', 'v3':'mean'})
ans.shape[0]
t_end = timeit.default_timer()
t = t_end - t_start
m = float('nan')
write_log(task=task, data=data_name, in_rows=x.shape[0], question=question, out_rows=ans.shape[0], solution=solution, version=ver, fun=fun, run=1, time_sec=t, mem_gb=m)
del ans
gc.collect()
t_start = timeit.default_timer()
ans = x.groupby(['id3']).agg({'v1':'sum', 'v3':'mean'})
ans.shape[0]
t_end = timeit.default_timer()
t = t_end - t_start
m = float('nan')
write_log(task=task, data=data_name, in_rows=x.shape[0], question=question, out_rows=ans.shape[0], solution=solution, version=ver, fun=fun, run=2, time_sec=t, mem_gb=m)
del ans
gc.collect()
t_start = timeit.default_timer()
ans = x.groupby(['id3']).agg({'v1':'sum', 'v3':'mean'})
ans.shape[0]
t_end = timeit.default_timer()
t = t_end - t_start
m = float('nan')
write_log(task=task, data=data_name, in_rows=x.shape[0], question=question, out_rows=ans.shape[0], solution=solution, version=ver, fun=fun, run=3, time_sec=t, mem_gb=m)
del ans

question = "mean v1:v3 by id4" #4
gc.collect()
t_start = timeit.default_timer()
ans = x.groupby(['id4']).agg({'v1':'mean', 'v2':'mean', 'v3':'mean'})
ans.shape[0]
t_end = timeit.default_timer()
t = t_end - t_start
m = float('nan')
write_log(task=task, data=data_name, in_rows=x.shape[0], question=question, out_rows=ans.shape[0], solution=solution, version=ver, fun=fun, run=1, time_sec=t, mem_gb=m)
del ans
gc.collect()
t_start = timeit.default_timer()
ans = x.groupby(['id4']).agg({'v1':'mean', 'v2':'mean', 'v3':'mean'})
ans.shape[0]
t_end = timeit.default_timer()
t = t_end - t_start
m = float('nan')
write_log(task=task, data=data_name, in_rows=x.shape[0], question=question, out_rows=ans.shape[0], solution=solution, version=ver, fun=fun, run=2, time_sec=t, mem_gb=m)
del ans
gc.collect()
t_start = timeit.default_timer()
ans = x.groupby(['id4']).agg({'v1':'mean', 'v2':'mean', 'v3':'mean'})
ans.shape[0]
t_end = timeit.default_timer()
t = t_end - t_start
m = float('nan')
write_log(task=task, data=data_name, in_rows=x.shape[0], question=question, out_rows=ans.shape[0], solution=solution, version=ver, fun=fun, run=3, time_sec=t, mem_gb=m)
del ans

question = "sum v1:v3 by id6" #5
gc.collect()
t_start = timeit.default_timer()
ans = x.groupby(['id6']).agg({'v1':'sum', 'v2':'sum', 'v3':'sum'})
ans.shape[0]
t_end = timeit.default_timer()
t = t_end - t_start
m = float('nan')
write_log(task=task, data=data_name, in_rows=x.shape[0], question=question, out_rows=ans.shape[0], solution=solution, version=ver, fun=fun, run=1, time_sec=t, mem_gb=m)
del ans
gc.collect()
t_start = timeit.default_timer()
ans = x.groupby(['id6']).agg({'v1':'sum', 'v2':'sum', 'v3':'sum'})
ans.shape[0]
t_end = timeit.default_timer()
t = t_end - t_start
m = float('nan')
write_log(task=task, data=data_name, in_rows=x.shape[0], question=question, out_rows=ans.shape[0], solution=solution, version=ver, fun=fun, run=2, time_sec=t, mem_gb=m)
del ans
gc.collect()
t_start = timeit.default_timer()
ans = x.groupby(['id6']).agg({'v1':'sum', 'v2':'sum', 'v3':'sum'})
ans.shape[0]
t_end = timeit.default_timer()
t = t_end - t_start
m = float('nan')
write_log(task=task, data=data_name, in_rows=x.shape[0], question=question, out_rows=ans.shape[0], solution=solution, version=ver, fun=fun, run=3, time_sec=t, mem_gb=m)
del ans

exit(0)
