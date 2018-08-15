#!/usr/bin/env python

print("# join-modin.py")

import os
import gc
import timeit
import modin.pandas as pd

exec(open("./helpers.py").read())

src_x = os.environ['SRC_X_LOCAL']
src_y = os.environ['SRC_Y_LOCAL']

ver = "" #pd.__version__
git = ""
task = "join"
question = "inner join"
l = [os.path.basename(src_x), os.path.basename(src_y)]
data_name = '-'.join(l)
solution = "modin"
fun = "merge"
cache = "TRUE"

print("loading datasets...")

x = pd.read_csv(os.path.basename(src_x))
y = pd.read_csv(os.path.basename(src_y))

print("joining...")

# NotImplementedError: To contribute to Pandas on Ray, please visit github.com/modin-project/modin
gc.collect()
t_start = timeit.default_timer()
ans = x.merge(y, how='inner', on='KEY')
print(ans.shape)
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = [ans['X2'].sum(), ans['Y2'].sum()]
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=x.shape[0], question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=1, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt)
del ans

gc.collect()
t_start = timeit.default_timer()
ans = x.merge(y, how='inner', on='KEY')
print(ans.shape)
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = [ans['X2'].sum(), ans['Y2'].sum()]
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=x.shape[0], question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=2, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt)
del ans

gc.collect()
t_start = timeit.default_timer()
ans = x.merge(y, how='inner', on='KEY')
print(ans.shape)
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = [ans['X2'].sum(), ans['Y2'].sum()]
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=x.shape[0], question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=3, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt)
del ans

exit(0)
