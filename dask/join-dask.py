#!/usr/bin/env python

print("# join-dask.py")

import os
import gc
import timeit
import pandas as pd
import dask as dk
import dask.dataframe as dd
#import pydoop.hdfs as hd
from distributed import Executor

execfile("./helpers.py")

src_x = os.path.basename(os.environ['SRC_X_LOCAL']) # basename files must be in wd
src_y = os.path.basename(os.environ['SRC_Y_LOCAL'])

ver = dk.__version__
print(ver)
task = "join"
question = "inner join"
l = [os.path.basename(src_x), os.path.basename(src_y)]
data_name = '-'.join(l)
solution = "dask"
fun = "merge"

e = Executor(os.environ['MASTER'] + ":8786")
e

print("loading datasets...")
# with hd.open(src_x) as f:
#   x = pd.read_csv(f)
x = pd.read_csv(src_x)
x = dd.from_pandas(x, npartitions=8)
# with hd.open(src_y) as f:
#   y = pd.read_csv(f)
y = pd.read_csv(src_y)
y = dd.from_pandas(y, npartitions=8)
x = e.persist(x)
y = e.persist(y)
in_rows = len(x.index)

print("joining...")

gc.collect()
t_start = timeit.default_timer()
ans = x.merge(y, how='inner', on='KEY')
ans = e.persist(ans)
out_rows = len(ans.index)
t_end = timeit.default_timer()
t = t_end - t_start
m = float('nan')
write_log(task=task, data=data_name, in_rows=in_rows, question=question, out_rows=out_rows, solution=solution, version=ver, fun=fun, run=1, time_sec=t, mem_gb=m)
t_start = timeit.default_timer()
print pd.DataFrame({"X2":{1: ans['X2'].sum().compute()}, "Y2":{1: ans['Y2'].sum().compute()}})
print "elapsed: {}".format(timeit.default_timer() - t_start)
del ans

gc.collect()
t_start = timeit.default_timer()
ans = x.merge(y, how='inner', on='KEY')
ans = e.persist(ans)
out_rows = len(ans.index)
t_end = timeit.default_timer()
t = t_end - t_start
m = float('NaN')
write_log(task=task, data=data_name, in_rows=in_rows, question=question, out_rows=out_rows, solution=solution, version=ver, fun=fun, run=2, time_sec=t, mem_gb=m)
del ans

gc.collect()
t_start = timeit.default_timer()
ans = x.merge(y, how='inner', on='KEY')
ans = e.persist(ans)
out_rows = len(ans.index)
t_end = timeit.default_timer()
t = t_end - t_start
m = float('NaN')
write_log(task=task, data=data_name, in_rows=in_rows, question=question, out_rows=out_rows, solution=solution, version=ver, fun=fun, run=3, time_sec=t, mem_gb=m)
del ans

exit(0)
