#!/usr/bin/env python

print("# groupby-pydatatable.py")

import os
import gc
import timeit
import datatable as dt
from datatable import f, mean # TODO add sum here once h2oai/datatable#1065

exec(open("./helpers.py").read())

src_grp = os.environ['SRC_GRP_LOCAL']

ver = dt.__version__
print(ver)
git = dt.__git_revision__
task = "groupby"
data_name = os.path.basename(src_grp)
solution = "pydatatable"
fun = "(.datatable"
cache = "TRUE"

print("loading dataset...")

x = dt.fread(data_name)

print("grouping...")

question = "sum v1 by id1" #1
gc.collect()
t_start = timeit.default_timer()
ans = x(select=mean(f.v1), groupby="id1") # TODO: change mean to sum once h2oai/datatable#1065
print(ans.shape)
t = timeit.default_timer() - t_start
m = float('NaN')
t_start = timeit.default_timer()
chk = [ans['V0'].sum()] # TODO: use proper var name once h2oai/datatable#1071
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=x.shape[0], question=question, out_rows=ans.shape[0], solution=solution, version=ver, git=git, fun=fun, run=1, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt)
del ans
gc.collect()
t_start = timeit.default_timer()
ans = x(select=mean(f.v1), groupby="id1") # TODO: change mean to sum once h2oai/datatable#1065
print(ans.shape)
t = timeit.default_timer() - t_start
m = float('NaN')
t_start = timeit.default_timer()
chk = [ans['V0'].sum()] # TODO: use proper var name once h2oai/datatable#1071
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=x.shape[0], question=question, out_rows=ans.shape[0], solution=solution, version=ver, git=git, fun=fun, run=2, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt)
del ans
gc.collect()
t_start = timeit.default_timer()
ans = x(select=mean(f.v1), groupby="id1") # TODO: change mean to sum once h2oai/datatable#1065
print(ans.shape)
t = timeit.default_timer() - t_start
m = float('NaN')
t_start = timeit.default_timer()
chk = [ans['V0'].sum()] # TODO: use proper var name once h2oai/datatable#1071
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=x.shape[0], question=question, out_rows=ans.shape[0], solution=solution, version=ver, git=git, fun=fun, run=3, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt)
del ans

#question = "sum v1 by id1:id2" #2
#gc.collect()
#t_start = timeit.default_timer()
#ans = x(select=sum(f.v1), groupby=["id1","id2"])
#print(ans.shape)
#t = timeit.default_timer() - t_start
#m = float('NaN')
#t_start = timeit.default_timer()
#chk = [ans['v1'].sum()]
#chkt = timeit.default_timer() - t_start
#write_log(task=task, data=data_name, in_rows=x.shape[0], question=question, out_rows=ans.shape[0], solution=solution, version=ver, git=git, fun=fun, run=1, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt)
#del ans
#gc.collect()
#t_start = timeit.default_timer()
#ans = x(select=sum(f.v1), groupby=["id1","id2"])
#print(ans.shape)
#t = timeit.default_timer() - t_start
#m = float('NaN')
#t_start = timeit.default_timer()
#chk = [ans['v1'].sum()]
#chkt = timeit.default_timer() - t_start
#write_log(task=task, data=data_name, in_rows=x.shape[0], question=question, out_rows=ans.shape[0], solution=solution, version=ver, git=git, fun=fun, run=2, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt)
#del ans
#gc.collect()
#t_start = timeit.default_timer()
#ans = x(select=sum(f.v1), groupby=["id1","id2"])
#print(ans.shape)
#t = timeit.default_timer() - t_start
#m = float('NaN')
#t_start = timeit.default_timer()
#chk = [ans['v1'].sum()]
#chkt = timeit.default_timer() - t_start
#write_log(task=task, data=data_name, in_rows=x.shape[0], question=question, out_rows=ans.shape[0], solution=solution, version=ver, git=git, fun=fun, run=3, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt)
#del ans

#question = "sum v1 mean v3 by id3" #3
#gc.collect()
#t_start = timeit.default_timer()
#ans = x(select=[sum(f.v1), mean(f.v3)], groupby="id3")
#print(ans.shape)
#t = timeit.default_timer() - t_start
#m = float('NaN')
#t_start = timeit.default_timer()
#chk = [ans['v1'].sum(), ans['v3'].sum()]
#chkt = timeit.default_timer() - t_start
#write_log(task=task, data=data_name, in_rows=x.shape[0], question=question, out_rows=ans.shape[0], solution=solution, version=ver, git=git, fun=fun, run=1, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt)
#del ans
#gc.collect()
#t_start = timeit.default_timer()
#ans = x(select=[sum(f.v1), mean(f.v3)], groupby="id3")
#print(ans.shape)
#t = timeit.default_timer() - t_start
#m = float('NaN')
#t_start = timeit.default_timer()
#chk = [ans['v1'].sum(), ans['v3'].sum()]
#chkt = timeit.default_timer() - t_start
#write_log(task=task, data=data_name, in_rows=x.shape[0], question=question, out_rows=ans.shape[0], solution=solution, version=ver, git=git, fun=fun, run=2, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt)
#del ans
#gc.collect()
#t_start = timeit.default_timer()
#ans = x(select=[sum(f.v1), mean(f.v3)], groupby="id3")
#print(ans.shape)
#t = timeit.default_timer() - t_start
#m = float('NaN')
#t_start = timeit.default_timer()
#chk = [ans['v1'].sum(), ans['v3'].sum()]
#chkt = timeit.default_timer() - t_start
#write_log(task=task, data=data_name, in_rows=x.shape[0], question=question, out_rows=ans.shape[0], solution=solution, version=ver, git=git, fun=fun, run=3, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt)
#del ans

#question = "mean v1:v3 by id4" #4
#gc.collect()
#t_start = timeit.default_timer()
#ans = x(select=[mean(f.v1), mean(f.v2), mean(f.v3)], groupby="id4")
#print(ans.shape)
#t = timeit.default_timer() - t_start
#m = float('NaN')
#t_start = timeit.default_timer()
#chk = [ans['v1'].sum(), ans['v2'].sum(), ans['v3'].sum()]
#chkt = timeit.default_timer() - t_start
#write_log(task=task, data=data_name, in_rows=x.shape[0], question=question, out_rows=ans.shape[0], solution=solution, version=ver, git=git, fun=fun, run=1, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt)
#del ans
#gc.collect()
#t_start = timeit.default_timer()
#ans = x(select=[mean(f.v1), mean(f.v2), mean(f.v3)], groupby="id4")
#print(ans.shape)
#t = timeit.default_timer() - t_start
#m = float('NaN')
#t_start = timeit.default_timer()
#chk = [ans['v1'].sum(), ans['v2'].sum(), ans['v3'].sum()]
#chkt = timeit.default_timer() - t_start
#write_log(task=task, data=data_name, in_rows=x.shape[0], question=question, out_rows=ans.shape[0], solution=solution, version=ver, git=git, fun=fun, run=2, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt)
#del ans
#gc.collect()
#t_start = timeit.default_timer()
#ans = x(select=[mean(f.v1), mean(f.v2), mean(f.v3)], groupby="id4")
#print(ans.shape)
#t = timeit.default_timer() - t_start
#m = float('NaN')
#t_start = timeit.default_timer()
#chk = [ans['v1'].sum(), ans['v2'].sum(), ans['v3'].sum()]
#chkt = timeit.default_timer() - t_start
#write_log(task=task, data=data_name, in_rows=x.shape[0], question=question, out_rows=ans.shape[0], solution=solution, version=ver, git=git, fun=fun, run=3, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt)
#del ans

#question = "sum v1:v3 by id6" #5
#gc.collect()
#t_start = timeit.default_timer()
#ans = x(select=[sum(f.v1), sum(f.v2), sum(f.v3)], groupby="id6")
#print(ans.shape)
#t = timeit.default_timer() - t_start
#m = float('NaN')
#t_start = timeit.default_timer()
#chk = [ans['v1'].sum(), ans['v2'].sum(), ans['v3'].sum()]
#chkt = timeit.default_timer() - t_start
#write_log(task=task, data=data_name, in_rows=x.shape[0], question=question, out_rows=ans.shape[0], solution=solution, version=ver, git=git, fun=fun, run=1, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt)
#del ans
#gc.collect()
#t_start = timeit.default_timer()
#ans = x(select=[sum(f.v1), sum(f.v2), sum(f.v3)], groupby="id6")
#print(ans.shape)
#t = timeit.default_timer() - t_start
#m = float('NaN')
#t_start = timeit.default_timer()
#chk = [ans['v1'].sum(), ans['v2'].sum(), ans['v3'].sum()]
#chkt = timeit.default_timer() - t_start
#write_log(task=task, data=data_name, in_rows=x.shape[0], question=question, out_rows=ans.shape[0], solution=solution, version=ver, git=git, fun=fun, run=2, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt)
#del ans
#gc.collect()
#t_start = timeit.default_timer()
#ans = x(select=[sum(f.v1), sum(f.v2), sum(f.v3)], groupby="id6")
#print(ans.shape)
#t = timeit.default_timer() - t_start
#m = float('NaN')
#t_start = timeit.default_timer()
#chk = [ans['v1'].sum(), ans['v2'].sum(), ans['v3'].sum()]
#chkt = timeit.default_timer() - t_start
#write_log(task=task, data=data_name, in_rows=x.shape[0], question=question, out_rows=ans.shape[0], solution=solution, version=ver, git=git, fun=fun, run=3, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt)
#del ans

exit(0)
