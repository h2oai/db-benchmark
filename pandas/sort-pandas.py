#!/usr/bin/env python

print("# sort-pandas.py")

import os
import gc
import timeit
import pandas as pd
#import pydoop.hdfs as hd

execfile("./helpers.py")

src_x = os.environ['SRC_X_LOCAL']

ver = pd.__version__
print(ver)
git = ""
task = "sort"
question = "by int KEY"
data_name = os.path.basename(src_x)
solution = "pandas"
fun = ".sort"
cache = "TRUE"

print("loading dataset...")

# with hd.open(src_x) as f:
#    x = pd.read_csv(f)
x = pd.read_csv(data_name)

print("joining...")

gc.collect()
t_start = timeit.default_timer()
ans = x.sort_values('KEY')
print ans.shape[0]
t = timeit.default_timer() - t_start
m = float('NaN')
t_start = timeit.default_timer()
chk = [ans['X2'].sum()]
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=x.shape[0], question=question, out_rows=ans.shape[0], solution=solution, version=ver, git=git, fun=fun, run=1, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt)
del ans

gc.collect()
t_start = timeit.default_timer()
ans = x.sort_values('KEY')
print ans.shape[0]
t = timeit.default_timer() - t_start
m = float('NaN')
t_start = timeit.default_timer()
chk = [ans['X2'].sum()]
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=x.shape[0], question=question, out_rows=ans.shape[0], solution=solution, version=ver, git=git, fun=fun, run=2, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt)
del ans

gc.collect()
t_start = timeit.default_timer()
ans = x.sort_values('KEY')
print ans.shape[0]
t = timeit.default_timer() - t_start
m = float('NaN')
t_start = timeit.default_timer()
chk = [ans['X2'].sum()]
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=x.shape[0], question=question, out_rows=ans.shape[0], solution=solution, version=ver, git=git, fun=fun, run=3, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt)
del ans

exit(0)
