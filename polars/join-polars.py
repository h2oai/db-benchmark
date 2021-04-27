#!/usr/bin/env python

print("# join-polars.py", flush=True)

import os
import gc
import timeit
import polars as pl

exec(open("./_helpers/helpers.py").read())

ver = pl.__version__
task = "join"
git = ""
solution = "polars"
fun = ".join"
cache = "TRUE"
on_disk = "FALSE"

data_name = os.environ["SRC_DATANAME"]
src_jn_x = os.path.join("data", data_name + ".csv")
y_data_name = join_to_tbls(data_name)
src_jn_y = [os.path.join("data", y_data_name[0] + ".csv"), os.path.join("data", y_data_name[1] + ".csv"), os.path.join("data", y_data_name[2] + ".csv")]
if len(src_jn_y) != 3:
  raise Exception("Something went wrong in preparing files used for join")

print("loading datasets " + data_name + ", " + y_data_name[0] + ", " + y_data_name[2] + ", " + y_data_name[2], flush=True)

with pl.StringCache():
  x = pl.read_csv(src_jn_x, dtype={"id1":pl.Int32, "id2":pl.Int32, "id3":pl.Int32, "v1":pl.Float64})
  x["id4"] = x["id4"].cast(pl.Categorical)
  x["id5"] = x["id5"].cast(pl.Categorical)
  x["id6"] = x["id6"].cast(pl.Categorical)
  small = pl.read_csv(src_jn_y[0], dtype={"id1":pl.Int32, "v2":pl.Float64})
  small["id4"] = small["id4"].cast(pl.Categorical)
  medium = pl.read_csv(src_jn_y[1], dtype={"id1":pl.Int32, "id2":pl.Int32, "v2":pl.Float64})
  medium["id4"] = medium["id4"].cast(pl.Categorical)
  medium["id5"] = medium["id5"].cast(pl.Categorical)
  big = pl.read_csv(src_jn_y[2], dtype={"id1":pl.Int32, "id2":pl.Int32, "id3":pl.Int32, "v2":pl.Float64})
  big["id4"] = big["id4"].cast(pl.Categorical)
  big["id5"] = big["id5"].cast(pl.Categorical)
  big["id6"] = big["id6"].cast(pl.Categorical)

print(len(x), flush=True)
print(len(small), flush=True)
print(len(medium), flush=True)
print(len(big), flush=True)

task_init = timeit.default_timer()
print("joining...", flush=True)

question = "small inner on int" # q1
gc.collect()
t_start = timeit.default_timer()
ans = x.join(small, on="id1")
print(ans.shape, flush=True)
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = [ans["v1"].cast(pl.Int64).sum(), ans["v2"].cast(pl.Int64).sum()]
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=x.shape[0], question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=1, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
del ans
gc.collect()
t_start = timeit.default_timer()
ans = x.join(small, on="id1")
print(ans.shape, flush=True)
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = [ans["v1"].cast(pl.Int64).sum(), ans["v2"].cast(pl.Int64).sum()]
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=x.shape[0], question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=2, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
print(ans.head(3), flush=True)
print(ans.tail(3), flush=True)
del ans

question = "medium inner on int" # q2
gc.collect()
t_start = timeit.default_timer()
ans = x.join(medium, on="id2")
print(ans.shape, flush=True)
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = [ans["v1"].cast(pl.Int64).sum(), ans["v2"].cast(pl.Int64).sum()]
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=x.shape[0], question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=1, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
del ans
gc.collect()
t_start = timeit.default_timer()
ans = x.join(medium, on="id2")
print(ans.shape, flush=True)
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = [ans["v1"].cast(pl.Int64).sum(), ans["v2"].cast(pl.Int64).sum()]
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=x.shape[0], question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=2, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
print(ans.head(3), flush=True)
print(ans.tail(3), flush=True)
del ans

question = "medium outer on int" # q3
gc.collect()
t_start = timeit.default_timer()
ans = x.join(medium, how="left", on="id2")
print(ans.shape, flush=True)
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = [ans["v1"].cast(pl.Int64).sum(), ans["v2"].cast(pl.Int64).sum()]
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=x.shape[0], question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=1, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
del ans
gc.collect()
t_start = timeit.default_timer()
ans = x.join(medium, how="left", on="id2")
print(ans.shape, flush=True)
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = [ans["v1"].cast(pl.Int64).sum(), ans["v2"].cast(pl.Int64).sum()]
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=x.shape[0], question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=2, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
print(ans.head(3), flush=True)
print(ans.tail(3), flush=True)
del ans

question = "medium inner on factor" # q4
gc.collect()
t_start = timeit.default_timer()
ans = x.join(medium, on="id5")
print(ans.shape, flush=True)
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = [ans["v1"].cast(pl.Int64).sum(), ans["v2"].cast(pl.Int64).sum()]
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=x.shape[0], question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=1, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
del ans
gc.collect()
t_start = timeit.default_timer()
ans = x.join(medium, on="id5")
print(ans.shape, flush=True)
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = [ans["v1"].cast(pl.Int64).sum(), ans["v2"].cast(pl.Int64).sum()]
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=x.shape[0], question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=2, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
print(ans.head(3), flush=True)
print(ans.tail(3), flush=True)
del ans

question = "big inner on int" # q5
gc.collect()
t_start = timeit.default_timer()
ans = x.join(big, on="id3")
print(ans.shape, flush=True)
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = [ans["v1"].cast(pl.Int64).sum(), ans["v2"].cast(pl.Int64).sum()]
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=x.shape[0], question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=1, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
del ans
gc.collect()
t_start = timeit.default_timer()
ans = x.join(big, on="id3")
print(ans.shape, flush=True)
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = [ans["v1"].cast(pl.Int64).sum(), ans["v2"].cast(pl.Int64).sum()]
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=x.shape[0], question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=2, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
print(ans.head(3), flush=True)
print(ans.tail(3), flush=True)
del ans

print("joining finished, took %0.fs" % (timeit.default_timer() - task_init), flush=True)

exit(0)
