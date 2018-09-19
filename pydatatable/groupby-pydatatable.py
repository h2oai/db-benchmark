#!/usr/bin/env python

print("# groupby-pydatatable.py")

import os
import gc
import timeit
import datatable as dt
from datatable import f, sum, mean

exec(open("./helpers.py").read())

src_grp = os.environ['SRC_GRP_LOCAL']

ver = dt.__version__
git = dt.__git_revision__
task = "groupby"
data_name = os.path.basename(src_grp)
solution = "pydatatable"
fun = "[.datatable"
cache = "TRUE"

print("loading dataset...")

if os.path.isfile(data_name):
  x = dt.fread(data_name)
else:
  x = dt.fread(src_grp)

print(x.nrows)

print("grouping...")

question = "sum v1 by id1" #1
gc.collect()
t_start = timeit.default_timer()
ans = x[:, {"v1": sum(f.v1)}, f.id1]
print(ans.shape)
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = ans[:, sum(f.v1)]
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=x.shape[0], question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=1, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(flatten(chk.topython())), chk_time_sec=chkt)
del ans
gc.collect()
t_start = timeit.default_timer()
ans = x[:, {"v1": sum(f.v1)}, f.id1]
print(ans.shape)
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = ans[:, sum(f.v1)]
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=x.shape[0], question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=2, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(flatten(chk.topython())), chk_time_sec=chkt)
del ans
gc.collect()
t_start = timeit.default_timer()
ans = x[:, {"v1": sum(f.v1)}, f.id1]
print(ans.shape)
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = ans[:, sum(f.v1)]
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=x.shape[0], question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=3, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(flatten(chk.topython())), chk_time_sec=chkt)
del ans

question = "sum v1 by id1:id2" #2
gc.collect()
t_start = timeit.default_timer()
#ans = x[:, {"v1": sum(f.v1)}, [f.id1, f.id2]] # datatable#1082
ans = dt.Frame()
print(ans.shape)
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
#chk = ans[:, sum(f.v1)]
chk = dt.Frame()
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=x.shape[0], question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=1, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(flatten(chk.topython())), chk_time_sec=chkt)
del ans
gc.collect()
t_start = timeit.default_timer()
#ans = x[:, {"v1": sum(f.v1)}, [f.id1, f.id2]] # datatable#1082
ans = dt.Frame()
print(ans.shape)
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
#chk = ans[:, sum(f.v1)]
chk = dt.Frame()
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=x.shape[0], question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=2, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(flatten(chk.topython())), chk_time_sec=chkt)
del ans
gc.collect()
t_start = timeit.default_timer()
#ans = x[:, {"v1": sum(f.v1)}, [f.id1, f.id2]] # datatable#1082
ans = dt.Frame()
print(ans.shape)
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
#chk = ans[:, sum(f.v1)]
chk = dt.Frame()
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=x.shape[0], question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=3, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(flatten(chk.topython())), chk_time_sec=chkt)
del ans

question = "sum v1 mean v3 by id3" #3
gc.collect()
t_start = timeit.default_timer()
ans = x[:, {"v1": sum(f.v1), "v3": mean(f.v3)}, f.id3]
print(ans.shape)
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = ans[:, [sum(f.v1), sum(f.v3)]]
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=x.shape[0], question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=1, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(flatten(chk.topython())), chk_time_sec=chkt)
del ans
gc.collect()
t_start = timeit.default_timer()
ans = x[:, {"v1": sum(f.v1), "v3": mean(f.v3)}, f.id3]
print(ans.shape)
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = ans[:, [sum(f.v1), mean(f.v3)]]
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=x.shape[0], question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=2, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(flatten(chk.topython())), chk_time_sec=chkt)
del ans
gc.collect()
t_start = timeit.default_timer()
ans = x[:, {"v1": sum(f.v1), "v3": mean(f.v3)}, f.id3]
print(ans.shape)
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = ans[:, [sum(f.v1), mean(f.v3)]]
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=x.shape[0], question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=3, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(flatten(chk.topython())), chk_time_sec=chkt)
del ans

question = "mean v1:v3 by id4" #4
gc.collect()
t_start = timeit.default_timer()
ans = x[:, {"v1": mean(f.v1), "v2": mean(f.v2), "v3": mean(f.v3)}, f.id4]
print(ans.shape)
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = ans[:, [sum(f.v1), sum(f.v2), sum(f.v3)]]
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=x.shape[0], question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=1, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(flatten(chk.topython())), chk_time_sec=chkt)
del ans
gc.collect()
t_start = timeit.default_timer()
ans = x[:, {"v1": mean(f.v1), "v2": mean(f.v2), "v3": mean(f.v3)}, f.id4]
print(ans.shape)
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = ans[:, [sum(f.v1), sum(f.v2), sum(f.v3)]]
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=x.shape[0], question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=2, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(flatten(chk.topython())), chk_time_sec=chkt)
del ans
gc.collect()
t_start = timeit.default_timer()
ans = x[:, {"v1": mean(f.v1), "v2": mean(f.v2), "v3": mean(f.v3)}, f.id4]
print(ans.shape)
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = ans[:, [sum(f.v1), sum(f.v2), sum(f.v3)]]
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=x.shape[0], question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=3, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(flatten(chk.topython())), chk_time_sec=chkt)
del ans

question = "sum v1:v3 by id6" #5
gc.collect()
t_start = timeit.default_timer()
ans = x[:, {"v1": sum(f.v1), "v2": sum(f.v2), "v3": sum(f.v3)}, f.id6]
print(ans.shape)
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = ans[:, [sum(f.v1), sum(f.v2), sum(f.v3)]]
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=x.shape[0], question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=1, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(flatten(chk.topython())), chk_time_sec=chkt)
del ans
gc.collect()
t_start = timeit.default_timer()
ans = x[:, {"v1": sum(f.v1), "v2": sum(f.v2), "v3": sum(f.v3)}, f.id6]
print(ans.shape)
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = ans[:, [sum(f.v1), sum(f.v2), sum(f.v3)]]
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=x.shape[0], question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=2, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(flatten(chk.topython())), chk_time_sec=chkt)
del ans
gc.collect()
t_start = timeit.default_timer()
ans = x[:, {"v1": sum(f.v1), "v2": sum(f.v2), "v3": sum(f.v3)}, f.id6]
print(ans.shape)
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = ans[:, [sum(f.v1), sum(f.v2), sum(f.v3)]]
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=x.shape[0], question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=3, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(flatten(chk.topython())), chk_time_sec=chkt)
del ans

exit(0)
