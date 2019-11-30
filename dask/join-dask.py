#!/usr/bin/env python

print("# join-dask.py", flush=True)

import os
import gc
import timeit
import pandas as pd
import dask as dk
import dask.dataframe as dd

exec(open("./_helpers/helpers.py").read())

ver = dk.__version__
git = dk.__git_revision__
task = "join"
solution = "dask"
fun = ".merge"
cache = "TRUE"

data_name = os.environ['SRC_JN_LOCAL']
src_jn_x = os.path.join("data", data_name+".csv")
y_data_name = join_to_tbls(data_name)
src_jn_y = [os.path.join("data", y_data_name[0]+".csv"), os.path.join("data", y_data_name[1]+".csv"), os.path.join("data", y_data_name[2]+".csv")]
if len(src_jn_y) != 3:
    raise Exception("Something went wrong in preparing files used for join")

print("loading datasets " + data_name + ", " + y_data_name[0] + ", " + y_data_name[1] + ", " + y_data_name[2], flush=True)

x = dd.read_csv(src_jn_x, na_filter=False, dtype={'id4':'category', 'id5':'category', 'id6':'category'}).persist()
small = dd.read_csv(src_jn_y[0], na_filter=False, dtype={'id4':'category'}).persist()
medium = dd.read_csv(src_jn_y[1], na_filter=False, dtype={'id4':'category', 'id5':'category'}).persist()
big = dd.read_csv(src_jn_y[2], na_filter=False, dtype={'id4':'category', 'id5':'category', 'id6':'category'}).persist()

in_rows = len(x)
print(in_rows, flush=True)
print(len(small.index), flush=True)
print(len(medium.index), flush=True)
print(len(big.index), flush=True)

task_init = timeit.default_timer()
print("joining...", flush=True)

question = "small inner on int" # q1
gc.collect()
t_start = timeit.default_timer()
ans = x.merge(small, on='id1').compute()
print(ans.shape, flush=True)
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = [ans['v1'].sum(), ans['v2'].sum()]
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=in_rows, question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=1, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt)
del ans
gc.collect()
t_start = timeit.default_timer()
ans = x.merge(small, on='id1').compute()
print(ans.shape, flush=True)
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = [ans['v1'].sum(), ans['v2'].sum()]
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=in_rows, question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=2, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt)
print(ans.head(3), flush=True)
print(ans.tail(3), flush=True)
del ans

question = "medium inner on int" # q2
gc.collect()
t_start = timeit.default_timer()
ans = x.merge(medium, on='id2').compute()
print(ans.shape, flush=True)
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = [ans['v1'].sum(), ans['v2'].sum()]
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=in_rows, question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=1, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt)
del ans
gc.collect()
t_start = timeit.default_timer()
ans = x.merge(medium, on='id2').compute()
print(ans.shape, flush=True)
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = [ans['v1'].sum(), ans['v2'].sum()]
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=in_rows, question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=2, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt)
print(ans.head(3), flush=True)
print(ans.tail(3), flush=True)
del ans

question = "medium outer on int" # q3
gc.collect()
t_start = timeit.default_timer()
ans = x.merge(medium, how='left', on='id2').compute()
print(ans.shape, flush=True)
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = [ans['v1'].sum(), ans['v2'].sum()]
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=in_rows, question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=1, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt)
del ans
gc.collect()
t_start = timeit.default_timer()
ans = x.merge(medium, how='left', on='id2').compute()
print(ans.shape, flush=True)
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = [ans['v1'].sum(), ans['v2'].sum()]
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=in_rows, question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=2, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt)
print(ans.head(3), flush=True)
print(ans.tail(3), flush=True)
del ans

question = "medium inner on factor" # q4
gc.collect()
t_start = timeit.default_timer()
ans = x.merge(medium, on='id5').compute()
print(ans.shape, flush=True)
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = [ans['v1'].sum(), ans['v2'].sum()]
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=in_rows, question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=1, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt)
del ans
gc.collect()
t_start = timeit.default_timer()
ans = x.merge(medium, on='id5').compute()
print(ans.shape, flush=True)
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = [ans['v1'].sum(), ans['v2'].sum()]
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=in_rows, question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=2, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt)
print(ans.head(3), flush=True)
print(ans.tail(3), flush=True)
del ans

question = "big inner on int" # q5
gc.collect()
t_start = timeit.default_timer()
ans = x.merge(big, on='id3').compute()
print(ans.shape, flush=True)
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = [ans['v1'].sum(), ans['v2'].sum()]
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=in_rows, question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=1, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt)
del ans
gc.collect()
t_start = timeit.default_timer()
ans = x.merge(big, on='id3').compute()
print(ans.shape, flush=True)
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = [ans['v1'].sum(), ans['v2'].sum()]
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=in_rows, question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=2, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt)
print(ans.head(3), flush=True)
print(ans.tail(3), flush=True)
del ans

print("joining finished, took %0.fs" % (timeit.default_timer()-task_init), flush=True)

exit(0)
