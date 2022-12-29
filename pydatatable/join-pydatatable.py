#!/usr/bin/env python3

print("# join-pydatatable.py", flush=True)

import os
import gc
import sys
import timeit
import datatable as dt
from datatable import f, sum, join
from datatable.math import isfinite
dt.options.progress.enabled = False

exec(open("./_helpers/helpers.py").read())

ver = dt.__version__.split("+", 1)[0]
git = dt.build_info.git_revision
task = "join"
solution = "pydatatable"
fun = "join"
cache = "TRUE"

data_name = os.environ['SRC_DATANAME']
on_disk = data_name.split("_")[1] == "1e9" # on-disk data storage #126
fext = "jay" if on_disk else "csv"
src_jn_x = os.path.join("data", data_name+"."+fext)
y_data_name = join_to_tbls(data_name)
src_jn_y = [os.path.join("data", y_data_name[0]+"."+fext), os.path.join("data", y_data_name[1]+"."+fext), os.path.join("data", y_data_name[2]+"."+fext)]
if len(src_jn_y) != 3:
  raise Exception("Something went wrong in preparing files used for join")

print("loading datasets " + data_name + ", " + y_data_name[0] + ", " + y_data_name[2] + ", " + y_data_name[2], flush=True)

n_flag = int(float(data_name.split("_")[1]))
na_flag = int(data_name.split("_")[3])
if n_flag==1e9 and na_flag > 0:
  print("skip due to n=1e9 and na_flag>0: h2oai/datatable#2808", flush=True, file=sys.stderr) ## when removing remember to create 1e9 jay file
  exit(0) # fread string with NAs generates extra distinct group h2oai/datatable#2808

print("using disk memory-mapped data storage" if on_disk else "using in-memory data storage", flush=True)
x = dt.fread(src_jn_x, na_strings=[''])
small = dt.fread(src_jn_y[0], na_strings=[''])
medium = dt.fread(src_jn_y[1], na_strings=[''])
big = dt.fread(src_jn_y[2], na_strings=[''])

print(x.nrows, flush=True)
print(small.nrows, flush=True)
print(medium.nrows, flush=True)
print(big.nrows, flush=True)

task_init = timeit.default_timer()
print("joining...", flush=True)

question = "small inner on int" # q1
gc.collect()
y = small.copy(deep=True)
t_start = timeit.default_timer()
y.key = 'id1'
ans = x[:, :, join(y)][isfinite(f.v2), :] # , on='id1'
tmp = ans.copy(deep=True) ## ensure join results materialized #141
print(ans.shape, flush=True)
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = ans[:, [sum(f.v1), sum(f.v2)]]
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=x.shape[0], question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=1, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(flatten(chk.to_list())), chk_time_sec=chkt, on_disk=on_disk)
del ans, y, tmp
gc.collect()
y = small.copy(deep=True)
t_start = timeit.default_timer()
y.key = 'id1'
ans = x[:, :, join(y)][isfinite(f.v2), :] # , on='id1'
tmp = ans.copy(deep=True)
print(ans.shape, flush=True)
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = ans[:, [sum(f.v1), sum(f.v2)]]
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=x.shape[0], question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=2, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(flatten(chk.to_list())), chk_time_sec=chkt, on_disk=on_disk)
print(ans.head(3), flush=True)
print(ans.tail(3), flush=True)
del ans, y, tmp

question = "medium inner on int" # q2
gc.collect()
y = medium.copy(deep=True)
t_start = timeit.default_timer()
y.key = 'id2'
ans = x[:, :, join(y)][isfinite(f.v2), :] # , on='id2'
tmp = ans.copy(deep=True)
print(ans.shape, flush=True)
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = ans[:, [sum(f.v1), sum(f.v2)]]
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=x.shape[0], question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=1, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(flatten(chk.to_list())), chk_time_sec=chkt, on_disk=on_disk)
del ans, y, tmp
gc.collect()
y = medium.copy(deep=True)
t_start = timeit.default_timer()
y.key = 'id2'
ans = x[:, :, join(y)][isfinite(f.v2), :] # , on='id2'
tmp = ans.copy(deep=True)
print(ans.shape, flush=True)
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = ans[:, [sum(f.v1), sum(f.v2)]]
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=x.shape[0], question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=2, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(flatten(chk.to_list())), chk_time_sec=chkt, on_disk=on_disk)
print(ans.head(3), flush=True)
print(ans.tail(3), flush=True)
del ans, y, tmp

question = "medium outer on int" # q3
gc.collect()
y = medium.copy(deep=True)
t_start = timeit.default_timer()
y.key = 'id2'
ans = x[:, :, join(y)] # , on='id2', how='left'
tmp = ans.copy(deep=True)
print(ans.shape, flush=True)
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = ans[:, [sum(f.v1), sum(f.v2)]]
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=x.shape[0], question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=1, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(flatten(chk.to_list())), chk_time_sec=chkt, on_disk=on_disk)
del ans, y, tmp
gc.collect()
y = medium.copy(deep=True)
t_start = timeit.default_timer()
y.key = 'id2'
ans = x[:, :, join(y)] # , on='id2', how='left'
tmp = ans.copy(deep=True)
print(ans.shape, flush=True)
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = ans[:, [sum(f.v1), sum(f.v2)]]
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=x.shape[0], question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=2, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(flatten(chk.to_list())), chk_time_sec=chkt, on_disk=on_disk)
print(ans.head(3), flush=True)
print(ans.tail(3), flush=True)
del ans, y, tmp

question = "medium inner on factor" # q4
gc.collect()
y = medium.copy(deep=True)
t_start = timeit.default_timer()
y.key = 'id5'
ans = x[:, :, join(y)][isfinite(f.v2), :] # , on='id5'
tmp = ans.copy(deep=True)
print(ans.shape, flush=True)
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = ans[:, [sum(f.v1), sum(f.v2)]]
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=x.shape[0], question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=1, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(flatten(chk.to_list())), chk_time_sec=chkt, on_disk=on_disk)
del ans, y, tmp
gc.collect()
y = medium.copy(deep=True)
t_start = timeit.default_timer()
y.key = 'id5'
ans = x[:, :, join(y)][isfinite(f.v2), :] # , on='id5'
tmp = ans.copy(deep=True)
print(ans.shape, flush=True)
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = ans[:, [sum(f.v1), sum(f.v2)]]
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=x.shape[0], question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=2, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(flatten(chk.to_list())), chk_time_sec=chkt, on_disk=on_disk)
print(ans.head(3), flush=True)
print(ans.tail(3), flush=True)
del ans, y, tmp

## to address #141 we do tmp=ans.copy(deep=True) which for q5 produces OOM thus try to_jay only here

question = "big inner on int" # q5
gc.collect()
y = big.copy(deep=True)
t_start = timeit.default_timer()
y.key = 'id3'
ans = x[:, :, join(y)][isfinite(f.v2), :] # , on='id3'
tmp = ans.copy(deep=True)
print(ans.shape, flush=True)
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = ans[:, [sum(f.v1), sum(f.v2)]]
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=x.shape[0], question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=1, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(flatten(chk.to_list())), chk_time_sec=chkt, on_disk=on_disk)
del ans, y, tmp
gc.collect()
y = big.copy(deep=True)
t_start = timeit.default_timer()
y.key = 'id3'
ans = x[:, :, join(y)][isfinite(f.v2), :] # , on='id3'
tmp = ans.copy(deep=True)
print(ans.shape, flush=True)
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = ans[:, [sum(f.v1), sum(f.v2)]]
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=x.shape[0], question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=2, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(flatten(chk.to_list())), chk_time_sec=chkt, on_disk=on_disk)
print(ans.head(3), flush=True)
print(ans.tail(3), flush=True)
del ans, y, tmp

print("joining finished, took %0.fs" % (timeit.default_timer()-task_init), flush=True)

exit(0)
