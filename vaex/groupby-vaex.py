#!/usr/bin/env python

print("# groupby-vaex.py", flush=True)

import os
import gc
import timeit
import vaex

exec(open("./_helpers/helpers.py").read())

ver = vaex.__version__['vaex-core']
git = '-'
task = "groupby"
solution = "vaex"
fun = ".groupby"
cache = "TRUE"
on_disk = "TRUE"

# vaex.cache.redis()

data_name = os.environ['SRC_GRP_LOCAL']
src_grp = os.path.join("data", data_name+".csv")
print("loading dataset %s" % data_name, flush=True)

x = vaex.open(src_grp, convert=True)
print("loaded dataset")
x.ordinal_encode('id1', inplace=True)
x.ordinal_encode('id2', inplace=True)
x.ordinal_encode('id3', inplace=True)

task_init = timeit.default_timer()
print("grouping...", flush=True)

question = "sum v1 by id1"  # q1
gc.collect()
t_start = timeit.default_timer()
ans = x.groupby(['id1']).agg({'v1': 'sum'})
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = [ans['v1'].sum()]
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=x.shape[0], question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=1, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
del ans
gc.collect()
t_start = timeit.default_timer()
ans = x.groupby(['id1']).agg({'v1': 'sum'})
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = [ans['v1'].sum()]
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=x.shape[0], question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=2, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
del ans

question = "sum v1 by id1:id2"  # q2
gc.collect()
t_start = timeit.default_timer()
ans = x.groupby(['id1', 'id2']).agg({'v1': 'sum'})
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = [ans['v1'].sum()]
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=x.shape[0], question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=1, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
del ans
gc.collect()
t_start = timeit.default_timer()
ans = x.groupby(['id1', 'id2']).agg({'v1': 'sum'})
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = [ans['v1'].sum()]
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=x.shape[0], question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=2, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
print(ans.head(3), flush=True)
print(ans.tail(3), flush=True)
del ans

question = "sum v1 mean v3 by id3"  # q3
gc.collect()
t_start = timeit.default_timer()
ans = x.groupby(['id3']).agg({'v1': 'sum', 'v3': 'mean'})
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = [ans['v1'].sum(), ans['v3'].sum()]
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=x.shape[0], question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=1, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
del ans
gc.collect()
t_start = timeit.default_timer()
ans = x.groupby(['id3']).agg({'v1':'sum', 'v3':'mean'})
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = [ans['v1'].sum(), ans['v3'].sum()]
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=x.shape[0], question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=2, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
print(ans.head(3), flush=True)
print(ans.tail(3), flush=True)
del ans

question = "mean v1:v3 by id4"  # q4
gc.collect()
t_start = timeit.default_timer()
ans = x.groupby(['id4']).agg({'v1':'mean', 'v2':'mean', 'v3':'mean'})
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = [ans['v1'].sum(), ans['v2'].sum(), ans['v3'].sum()]
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=x.shape[0], question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=1, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
del ans
gc.collect()
t_start = timeit.default_timer()
ans = x.groupby(['id4']).agg({'v1':'mean', 'v2':'mean', 'v3':'mean'})
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = [ans['v1'].sum(), ans['v2'].sum(), ans['v3'].sum()]
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=x.shape[0], question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=2, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
print(ans.head(3), flush=True)
print(ans.tail(3), flush=True)
del ans

question = "sum v1:v3 by id6"  # q5
gc.collect()
t_start = timeit.default_timer()
ans = x.groupby(['id6']).agg({'v1': 'sum', 'v2': 'sum', 'v3': 'sum'})
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = [ans['v1'].sum(), ans['v2'].sum(), ans['v3'].sum()]
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=x.shape[0], question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=1, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
del ans
gc.collect()
t_start = timeit.default_timer()
ans = x.groupby(['id6']).agg({'v1': 'sum', 'v2': 'sum', 'v3': 'sum'})
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = [ans['v1'].sum(), ans['v2'].sum(), ans['v3'].sum()]
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=x.shape[0], question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=2, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
print(ans.head(3), flush=True)
print(ans.tail(3), flush=True)
del ans

# exact median not implemented
# question = "median v3 sd v3 by id4 id5" # q6
# gc.collect()
# t_start = timeit.default_timer()
# ans = x.groupby(['id4','id5']).agg({'v3': ['median','std']})
# # ans.reset_index(inplace=True)
# # print(ans.shape, flush=True)
# t = timeit.default_timer() - t_start
# m = memory_usage()
# t_start = timeit.default_timer()
# chk = [ans['v3']['median'].sum(), ans['v3']['std'].sum()]
# chkt = timeit.default_timer() - t_start
# write_log(task=task, data=data_name, in_rows=x.shape[0], question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=1, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
# del ans
# gc.collect()
# t_start = timeit.default_timer()
# ans = x.groupby(['id4','id5']).agg({'v3': ['median','std']})
# # ans.reset_index(inplace=True)
# # print(ans.shape, flush=True)
# t = timeit.default_timer() - t_start
# m = memory_usage()
# t_start = timeit.default_timer()
# chk = [ans['v3']['median'].sum(), ans['v3']['std'].sum()]
# chkt = timeit.default_timer() - t_start
# write_log(task=task, data=data_name, in_rows=x.shape[0], question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=2, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
# print(ans.head(3), flush=True)
# print(ans.tail(3), flush=True)
# del ans

# we need to see how we do this
# question = "max v1 - min v2 by id3" # q7
# gc.collect()
# t_start = timeit.default_timer()
# ans = x.groupby(['id3']).agg({'v1': 'max', 'v2': 'min'}).assign(range_v1_v2=lambda x: x['v1'] - x['v2'])[['range_v1_v2']]
# # ans.reset_index(inplace=True)
# # print(ans.shape, flush=True)
# t = timeit.default_timer() - t_start
# m = memory_usage()
# t_start = timeit.default_timer()
# chk = [ans['range_v1_v2'].sum()]
# chkt = timeit.default_timer() - t_start
# write_log(task=task, data=data_name, in_rows=x.shape[0], question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=1, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
# del ans
# gc.collect()
# t_start = timeit.default_timer()
# ans = x.groupby(['id3']).agg({'v1': 'max', 'v2': 'min'}).assign(range_v1_v2=lambda x: x['v1'] - x['v2'])[['range_v1_v2']]
# # ans.reset_index(inplace=True)
# # print(ans.shape, flush=True)
# t = timeit.default_timer() - t_start
# m = memory_usage()
# t_start = timeit.default_timer()
# chk = [ans['range_v1_v2'].sum()]
# chkt = timeit.default_timer() - t_start
# write_log(task=task, data=data_name, in_rows=x.shape[0], question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=2, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
# print(ans.head(3), flush=True)
# print(ans.tail(3), flush=True)
# del ans


# maybe we can do this with first
# question = "largest two v3 by id6" # q8
# gc.collect()
# t_start = timeit.default_timer()
# ans = x[['id6','v3']].sort_values('v3', ascending=False).groupby(['id6']).head(2)
# ans.reset_index(drop=True, inplace=True)
# # print(ans.shape, flush=True)
# t = timeit.default_timer() - t_start
# m = memory_usage()
# t_start = timeit.default_timer()
# chk = [ans['v3'].sum()]
# chkt = timeit.default_timer() - t_start
# write_log(task=task, data=data_name, in_rows=x.shape[0], question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=1, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
# del ans
# gc.collect()
# t_start = timeit.default_timer()
# ans = x[['id6','v3']].sort_values('v3', ascending=False).groupby(['id6']).head(2)
# # ans.reset_index(drop=True, inplace=True)
# # print(ans.shape, flush=True)
# t = timeit.default_timer() - t_start
# m = memory_usage()
# t_start = timeit.default_timer()
# chk = [ans['v3'].sum()]
# chkt = timeit.default_timer() - t_start
# write_log(task=task, data=data_name, in_rows=x.shape[0], question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=2, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
# print(ans.head(3), flush=True)
# print(ans.tail(3), flush=True)
# del ans

# can do this with corr
# question = "regression v1 v2 by id2 id4" # q9
# #ans = x[['id2','id4','v1','v2']].groupby(['id2','id4']).corr().iloc[0::2][['v2']]**2 # slower, 76s vs 47s on 1e8 1e2
# gc.collect()
# t_start = timeit.default_timer()
# ans = x[['id2','id4','v1','v2']].groupby(['id2','id4']).apply(lambda x: vaex.Series({'r2': x.corr()['v1']['v2']**2}))
# # ans.reset_index(inplace=True)
# # print(ans.shape, flush=True)
# t = timeit.default_timer() - t_start
# m = memory_usage()
# t_start = timeit.default_timer()
# chk = [ans['r2'].sum()]
# chkt = timeit.default_timer() - t_start
# write_log(task=task, data=data_name, in_rows=x.shape[0], question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=1, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
# del ans
# gc.collect()
# t_start = timeit.default_timer()
# ans = x[['id2','id4','v1','v2']].groupby(['id2','id4']).apply(lambda x: vaex.Series({'r2': x.corr()['v1']['v2']**2}))
# # ans.reset_index(inplace=True)
# # print(ans.shape, flush=True)
# t = timeit.default_timer() - t_start
# m = memory_usage()
# t_start = timeit.default_timer()
# chk = [ans['r2'].sum()]
# chkt = timeit.default_timer() - t_start
# write_log(task=task, data=data_name, in_rows=x.shape[0], question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=2, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
# print(ans.head(3), flush=True)
# print(ans.tail(3), flush=True)
# # del ans

# segfault
# question = "sum v3 count by id1:id6" # q10
# gc.collect()
# t_start = timeit.default_timer()
# ans = x.groupby(['id1','id2','id3','id4','id5','id6']).agg({'v3':'sum', 'v1':'count'})
# # ans.reset_index(inplace=True)
# # print(ans.shape, flush=True)
# t = timeit.default_timer() - t_start
# m = memory_usage()
# t_start = timeit.default_timer()
# chk = [ans['v3'].sum(), ans['v1'].sum()]
# chkt = timeit.default_timer() - t_start
# write_log(task=task, data=data_name, in_rows=x.shape[0], question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=1, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
# del ans
# gc.collect()
# t_start = timeit.default_timer()
# ans = x.groupby(['id1','id2','id3','id4','id5','id6']).agg({'v3':'sum', 'v1':'count'})
# # ans.reset_index(inplace=True)
# # print(ans.shape, flush=True)
# t = timeit.default_timer() - t_start
# m = memory_usage()
# t_start = timeit.default_timer()
# chk = [ans['v3'].sum(), ans['v1'].sum()]
# chkt = timeit.default_timer() - t_start
# write_log(task=task, data=data_name, in_rows=x.shape[0], question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=2, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
# print(ans.head(3), flush=True)
# print(ans.tail(3), flush=True)
# del ans

print("grouping finished, took %0.fs" % (timeit.default_timer()-task_init), flush=True)

exit(0)