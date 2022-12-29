#!/usr/bin/env python3

print("# sort-pydatatable.py")

import os
import gc
import timeit
import datatable as dt
from datatable import f, sum

exec(open("./helpers.py").read())

src_x = os.environ['SRC_X_LOCAL']

ver = dt.__version__
git = dt.__git_revision__
task = "sort"
question = "by int KEY"
data_name = os.path.basename(src_x)
solution = "pydatatable"
fun = ".sort"
cache = "TRUE"

print("loading dataset...")

x = dt.fread(data_name)

print("sorting...")

gc.collect()
t_start = timeit.default_timer()
ans = x.sort('KEY')
print(ans.shape)
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = ans[:, sum(f.X2)]
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=x.shape[0], question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=1, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(flatten(chk.topython())), chk_time_sec=chkt)
del ans

gc.collect()
t_start = timeit.default_timer()
ans = x.sort('KEY')
print(ans.shape)
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = ans[:, sum(f.X2)]
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=x.shape[0], question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=2, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(flatten(chk.topython())), chk_time_sec=chkt)
del ans

gc.collect()
t_start = timeit.default_timer()
ans = x.sort('KEY')
print(ans.shape)
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = ans[:, sum(f.X2)]
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=x.shape[0], question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=3, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(flatten(chk.topython())), chk_time_sec=chkt)
del ans

exit(0)
