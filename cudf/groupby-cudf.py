#!/usr/bin/env python

print("# groupby-cudf.py", flush=True)

import os
import gc
import timeit
import cudf as cu

exec(open("./_helpers/helpers.py").read())

ver = cu.__version__.split("+", 1)[0]
git = ""
task = "groupby"
solution = "cudf"
fun = ".groupby"
cache = "TRUE"
on_disk = "FALSE"

data_name = os.environ['SRC_GRP_LOCAL']
src_grp = os.path.join("data", data_name+".csv")
print("loading dataset %s" % data_name, flush=True)

x = cu.read_csv(src_grp, header=0, dtype=['str','str','str','int32','int32','int32','int32','int32','float64'])
x['id1'] = x['id1'].astype('category')
x['id2'] = x['id2'].astype('category')
x['id3'] = x['id3'].astype('category')
print(len(x.index), flush=True)

task_init = timeit.default_timer()
print("grouping...", flush=True)

question = "sum v1 by id1" # q1
gc.collect()
t_start = timeit.default_timer()
ans = x.groupby(['id1'],as_index=False).agg({'v1':'sum'}).reset_index(drop=True)
print(ans.shape, flush=True)
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = [ans['v1'].sum()]
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=x.shape[0], question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=1, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
del ans
gc.collect()
t_start = timeit.default_timer()
ans = x.groupby(['id1'],as_index=False).agg({'v1':'sum'}).reset_index(drop=True)
print(ans.shape, flush=True)
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = [ans['v1'].sum()]
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=x.shape[0], question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=2, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
print(ans.head(3), flush=True)
print(ans.tail(3), flush=True)
del ans

question = "sum v1 by id1:id2" # q2
gc.collect()
t_start = timeit.default_timer()
ans = x.groupby(['id1','id2'],as_index=False).agg({'v1':'sum'}).reset_index(drop=True)
print(ans.shape, flush=True)
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = [ans['v1'].sum()]
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=x.shape[0], question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=1, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
del ans
gc.collect()
t_start = timeit.default_timer()
ans = x.groupby(['id1','id2'],as_index=False).agg({'v1':'sum'}).reset_index(drop=True)
print(ans.shape, flush=True)
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = [ans['v1'].sum()]
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=x.shape[0], question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=2, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
print(ans.head(3), flush=True)
print(ans.tail(3), flush=True)
del ans

question = "sum v1 mean v3 by id3" # q3
gc.collect()
t_start = timeit.default_timer()
ans = x.groupby(['id3'],as_index=False).agg({'v1':'sum', 'v3':'mean'}).reset_index(drop=True)
print(ans.shape, flush=True)
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = [ans['v1'].sum(), ans['v3'].sum()]
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=x.shape[0], question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=1, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
del ans
gc.collect()
t_start = timeit.default_timer()
ans = x.groupby(['id3'],as_index=False).agg({'v1':'sum', 'v3':'mean'}).reset_index(drop=True)
print(ans.shape, flush=True)
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = [ans['v1'].sum(), ans['v3'].sum()]
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=x.shape[0], question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=2, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
print(ans.head(3), flush=True)
print(ans.tail(3), flush=True)
del ans

question = "mean v1:v3 by id4" # q4
gc.collect()
t_start = timeit.default_timer()
ans = x.groupby(['id4'],as_index=False).agg({'v1':'mean', 'v2':'mean', 'v3':'mean'}).reset_index(drop=True)
print(ans.shape, flush=True)
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = [ans['v1'].sum(), ans['v2'].sum(), ans['v3'].sum()]
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=x.shape[0], question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=1, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
del ans
gc.collect()
t_start = timeit.default_timer()
ans = x.groupby(['id4'],as_index=False).agg({'v1':'mean', 'v2':'mean', 'v3':'mean'}).reset_index(drop=True)
print(ans.shape, flush=True)
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = [ans['v1'].sum(), ans['v2'].sum(), ans['v3'].sum()]
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=x.shape[0], question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=2, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
print(ans.head(3), flush=True)
print(ans.tail(3), flush=True)
del ans

question = "sum v1:v3 by id6" # q5
gc.collect()
t_start = timeit.default_timer()
ans = x.groupby(['id6'],as_index=False).agg({'v1':'sum', 'v2':'sum', 'v3':'sum'}).reset_index(drop=True)
print(ans.shape, flush=True)
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = [ans['v1'].sum(), ans['v2'].sum(), ans['v3'].sum()]
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=x.shape[0], question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=1, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
del ans
gc.collect()
t_start = timeit.default_timer()
ans = x.groupby(['id6'],as_index=False).agg({'v1':'sum', 'v2':'sum', 'v3':'sum'}).reset_index(drop=True)
print(ans.shape, flush=True)
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = [ans['v1'].sum(), ans['v2'].sum(), ans['v3'].sum()]
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=x.shape[0], question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=2, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
print(ans.head(3), flush=True)
print(ans.tail(3), flush=True)
del ans

#question = "median v3 sd v3 by id4 id5" # q6 # median not yet implemented: https://github.com/rapidsai/cudf/issues/1085
#gc.collect()
#t_start = timeit.default_timer()
#ans = x.groupby(['id4','id5'],as_index=False).agg({'v3': ['median','std']})
#print(ans.shape, flush=True)
#t = timeit.default_timer() - t_start
#m = memory_usage()
#t_start = timeit.default_timer()
#chk = [ans['v3']['median'].sum(), ans['v3']['std'].sum()]
#chkt = timeit.default_timer() - t_start
#write_log(task=task, data=data_name, in_rows=x.shape[0], question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=1, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
#del ans
#gc.collect()
#t_start = timeit.default_timer()
#ans = x.groupby(['id4','id5'],as_index=False).agg({'v3': ['median','std']})
#print(ans.shape, flush=True)
#t = timeit.default_timer() - t_start
#m = memory_usage()
#t_start = timeit.default_timer()
#chk = [ans['v3']['median'].sum(), ans['v3']['std'].sum()]
#chkt = timeit.default_timer() - t_start
#write_log(task=task, data=data_name, in_rows=x.shape[0], question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=2, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
#print(ans.head(3), flush=True)
#print(ans.tail(3), flush=True)
#del ans

#question = "max v1 - min v2 by id3" # q7 # not yet implemented: https://github.com/rapidsai/cudf/issues/2591
#gc.collect()
#t_start = timeit.default_timer()
#ans = x.groupby(['id3'],as_index=False).agg({'v1': 'max', 'v2': 'min'}).assign(range_v1_v2=lambda x: x['v1'] - x['v2'])[['range_v1_v2']]
#print(ans.shape, flush=True)
#t = timeit.default_timer() - t_start
#m = memory_usage()
#t_start = timeit.default_timer()
#chk = [ans['range_v1_v2'].sum()]
#chkt = timeit.default_timer() - t_start
#write_log(task=task, data=data_name, in_rows=x.shape[0], question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=1, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
#del ans
#gc.collect()
#t_start = timeit.default_timer()
#ans = x.groupby(['id3'],as_index=False).agg({'v1': 'max', 'v2': 'min'}).assign(range_v1_v2=lambda x: x['v1'] - x['v2'])[['range_v1_v2']]
#print(ans.shape, flush=True)
#t = timeit.default_timer() - t_start
#m = memory_usage()
#t_start = timeit.default_timer()
#chk = [ans['range_v1_v2'].sum()]
#chkt = timeit.default_timer() - t_start
#write_log(task=task, data=data_name, in_rows=x.shape[0], question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=2, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
#print(ans.head(3), flush=True)
#print(ans.tail(3), flush=True)
#del ans

#question = "largest two v3 by id6" # q8 # not yet implemented: https://github.com/rapidsai/cudf/issues/2592
#gc.collect()
#t_start = timeit.default_timer()
#ans = x[['id6','v3']].sort_values('v3', ascending=False).groupby(['id6'],as_index=False).head(2)
#print(ans.shape, flush=True)
#t = timeit.default_timer() - t_start
#m = memory_usage()
#t_start = timeit.default_timer()
#chk = [ans['v3'].sum()]
#chkt = timeit.default_timer() - t_start
#write_log(task=task, data=data_name, in_rows=x.shape[0], question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=1, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
#del ans
#gc.collect()
#t_start = timeit.default_timer()
#ans = x[['id6','v3']].sort_values('v3', ascending=False).groupby(['id6'],as_index=False).head(2)
#print(ans.shape, flush=True)
#t = timeit.default_timer() - t_start
#m = memory_usage()
#t_start = timeit.default_timer()
#chk = [ans['v3'].sum()]
#chkt = timeit.default_timer() - t_start
#write_log(task=task, data=data_name, in_rows=x.shape[0], question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=2, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
#print(ans.head(3), flush=True)
#print(ans.tail(3), flush=True)
#del ans

#question = "regression v1 v2 by id2 id4" # q9 # not yet implemented: https://github.com/rapidsai/cudf/issues/1267
#gc.collect()
#t_start = timeit.default_timer()
#x[['v1','v2']].corr()
#ans = x[['id2','id4','v1','v2']].groupby(['id2','id4'],as_index=False).apply(lambda x: pd.Series({'r2': x.corr()['v1']['v2']**2}))
#print(ans.shape, flush=True)
#t = timeit.default_timer() - t_start
#m = memory_usage()
#t_start = timeit.default_timer()
#chk = [ans['r2'].sum()]
#chkt = timeit.default_timer() - t_start
#write_log(task=task, data=data_name, in_rows=x.shape[0], question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=1, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
#del ans
#gc.collect()
#t_start = timeit.default_timer()
#ans = x[['id2','id4','v1','v2']].groupby(['id2','id4'],as_index=False).apply(lambda x: pd.Series({'r2': x.corr()['v1']['v2']**2}))
#print(ans.shape, flush=True)
#t = timeit.default_timer() - t_start
#m = memory_usage()
#t_start = timeit.default_timer()
#chk = [ans['r2'].sum()]
#chkt = timeit.default_timer() - t_start
#write_log(task=task, data=data_name, in_rows=x.shape[0], question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=2, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
#print(ans.head(3), flush=True)
#print(ans.tail(3), flush=True)
#del ans

question = "sum v3 count by id1:id6" # q10
gc.collect()
t_start = timeit.default_timer()
ans = x.groupby(['id1','id2','id3','id4','id5','id6'],as_index=False).agg({'v3':'sum', 'v1':'count'}).reset_index(drop=True)
print(ans.shape, flush=True)
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = [ans['v3'].sum(), ans['v1'].sum()]
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=x.shape[0], question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=1, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
del ans
gc.collect()
t_start = timeit.default_timer()
ans = x.groupby(['id1','id2','id3','id4','id5','id6'],as_index=False).agg({'v3':'sum', 'v1':'count'}).reset_index(drop=True)
print(ans.shape, flush=True)
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = [ans['v3'].sum(), ans['v1'].sum()]
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=x.shape[0], question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=2, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
print(ans.head(3), flush=True)
print(ans.tail(3), flush=True)
del ans

print("grouping finished, took %0.fs" % (timeit.default_timer()-task_init), flush=True)

exit(0)
