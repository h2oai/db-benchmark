#!/usr/bin/env python

print("# read-pydatatable.py")

import os
import gc
import timeit
import datatable as dt
from datatable import f, sum

exec(open("./helpers.py").read())

src_grp = os.environ['SRC_GRP_LOCAL']

ver = dt.__version__
git = dt.__git_revision__
task = "read"
data_name = os.path.basename(src_grp)
solution = "pydatatable"
fun = "fread"
cache = "TRUE"

print("reading...")

question = "all rows" #1
gc.collect()
t_start = timeit.default_timer()
ans = dt.fread(data_name)
print(ans.shape)
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = ans[:, sum(f.v3)]
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=x.shape[0], question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=1, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(flatten(chk.topython())), chk_time_sec=chkt)
del ans
gc.collect()
t_start = timeit.default_timer()
ans = dt.fread(data_name)
print(ans.shape)
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = ans[:, sum(f.v3)]
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=x.shape[0], question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=2, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(flatten(chk.topython())), chk_time_sec=chkt)
del ans
gc.collect()
t_start = timeit.default_timer()
ans = dt.fread(data_name)
print(ans.shape)
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = ans[:, sum(f.v3)]
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=x.shape[0], question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=3, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(flatten(chk.topython())), chk_time_sec=chkt)
del ans

question = "top 100 rows" #2
gc.collect()
t_start = timeit.default_timer()
ans = dt.fread(data_name, max_nrows=100)
print(ans.shape)
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = ans[:, sum(f.v3)]
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=x.shape[0], question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=1, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(flatten(chk.topython())), chk_time_sec=chkt)
del ans
gc.collect()
t_start = timeit.default_timer()
ans = dt.fread(data_name, max_nrows=100)
print(ans.shape)
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = ans[:, sum(f.v3)]
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=x.shape[0], question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=2, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(flatten(chk.topython())), chk_time_sec=chkt)
del ans
gc.collect()
t_start = timeit.default_timer()
ans = dt.fread(data_name, max_nrows=100)
print(ans.shape)
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = ans[:, sum(f.v3)]
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=x.shape[0], question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=3, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(flatten(chk.topython())), chk_time_sec=chkt)
del ans

exit(0)
