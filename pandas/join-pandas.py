#!/usr/bin/env python

print("# join-pandas.py")

import os
import gc
import timeit
import pandas as pd
import pydoop.hdfs as hd

execfile("./helpers.py")

src_x = os.environ['SRC_X_LOCAL']
src_y = os.environ['SRC_Y_LOCAL']
# TODO skip for total row count > 2e9 as data volume cap due to pandas scalability, currently just comment out in run.sh

ver = pd.__version__
print(ver)

print("loading datasets...")

with hd.open(src_x) as f:
   x = pd.read_csv(f)

with hd.open(src_y) as f:
   y = pd.read_csv(f)

print("join 1...")
t_start = timeit.default_timer()
ans = x.merge(y, how='inner', on='KEY')
ans.shape[0]
t_end = timeit.default_timer()
t = t_end - t_start
m = float('nan')
write_log(task="join", data="", in_rows=x.shape[0], out_rows=ans.shape[0], solution="pandas", version=ver, fun="merge", run=1, time_sec=t, mem_gb=m)
del ans
gc.collect()

print("join 2...")
t_start = timeit.default_timer()
ans = x.merge(y, how='inner', on='KEY')
ans.shape[0]
t_end = timeit.default_timer()
t = t_end - t_start
m = float('NaN')
write_log(task="join", data="", in_rows=x.shape[0], out_rows=ans.shape[0], solution="pandas", version=ver, fun="merge", run=2, time_sec=t, mem_gb=m)
del ans
gc.collect()

print("join 3...")
t_start = timeit.default_timer()
ans = x.merge(y, how='inner', on='KEY')
ans.shape[0]
t_end = timeit.default_timer()
t = t_end - t_start
m = float('NaN')
write_log(task="join", data="", in_rows=x.shape[0], out_rows=ans.shape[0], solution="pandas", version=ver, fun="merge", run=3, time_sec=t, mem_gb=m)
del ans

exit(0)
