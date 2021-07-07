#!/usr/bin/env python

print("# join-vaex.py", flush=True)

import os
import gc
import timeit
import vaex

exec(open("./_helpers/helpers.py").read())

ver = vaex.__version__['vaex-core']
git = 'alpha'
task = "join"
solution = "vaex"
fun = ".join"
cache = "TRUE"
on_disk = "FALSE"

data_name = os.environ['SRC_DATANAME']
src_jn_x = os.path.join("data", data_name+".csv")
y_data_name = join_to_tbls(data_name)
src_jn_y = [os.path.join("data", y_data_name[0]+".csv"), os.path.join("data", y_data_name[1]+".csv"), os.path.join("data", y_data_name[2]+".csv")]
if len(src_jn_y) != 3:
    raise Exception("Something went wrong in preparing files used for join")

print("loading datasets " + data_name + ", " + y_data_name[0] + ", " + y_data_name[1] + ", " + y_data_name[2], flush=True)

print(src_jn_x, src_jn_y)
x = vaex.open(src_jn_x, convert=True, dtype={"id1": "Int8", "id2": "Int32", "id3": "Int32", "v2": "Float32"})
x.ordinal_encode('id1', inplace=True)
x.ordinal_encode('id2', inplace=True)
x.ordinal_encode('id3', inplace=True)


small = vaex.open(src_jn_y[0], convert=True, dtype={"id1": "Int16", "v2": "Float32"})
small.ordinal_encode('id1', inplace=True)
medium = vaex.open(src_jn_y[1], convert=True, dtype={"id1": "Int16", "id2": "Int32", "v2": "Float32"})
medium.ordinal_encode('id1', inplace=True)
medium.ordinal_encode('id2', inplace=True)
big = vaex.open(src_jn_y[2], convert=True, dtype={"id1": "Int16", "id2": "Int32", "id3": "Int32", "v2": "Float32"})
big.ordinal_encode('id1', inplace=True)
big.ordinal_encode('id2', inplace=True)
big.ordinal_encode('id3', inplace=True)

print(len(x), flush=True)
print(len(small), flush=True)
print(len(medium), flush=True)
print(len(big), flush=True)

def benchmark(func, question, chk_sum_cols):
    gc.collect()
    t_start = timeit.default_timer()
    ans = func()
    print(ans.shape, flush=True)
    t = timeit.default_timer() - t_start
    m = memory_usage()
    t_start = timeit.default_timer()
    chk = [ans[col].sum() for col in chk_sum_cols]
    chkt = timeit.default_timer() - t_start
    write_log(task=task, data=data_name, in_rows=x.shape[0], question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=1, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
    del ans
    gc.collect()
    t_start = timeit.default_timer()
    ans = func()
    t = timeit.default_timer() - t_start
    m = memory_usage()
    t_start = timeit.default_timer()
    chk = [ans[col].sum() for col in chk_sum_cols]
    chkt = timeit.default_timer() - t_start
    write_log(task=task, data=data_name, in_rows=x.shape[0], question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=2, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
    print(ans.head(3), flush=True)
    print(ans.tail(3), flush=True)
    del ans


def question_1():
    return x.join(small, how='inner', on='id1', rsuffix='_r')


def question_2():
    return x.join(medium, how='inner', on='id2', rsuffix='_r')


def question_3():
    return x.join(medium, how='left', on='id2', rsuffix='_r')


def question_4():
    return x.join(medium, how='inner', on='id5', rsuffix='_r')


def question_5():
    return x.join(big, how='inner', on='id3', rsuffix='_r')


task_init = timeit.default_timer()
print("joining...", flush=True)

benchmark(question_1, question="small inner on int", chk_sum_cols=['v1', 'v2'])
benchmark(question_2, question="medium inner on int", chk_sum_cols=['v1', 'v2'])
benchmark(question_3, question="medium outer on int", chk_sum_cols=['v1', 'v2'])
benchmark(question_4, question="medium inner on factor", chk_sum_cols=['v1', 'v2'])
benchmark(question_5, question="big inner on int", chk_sum_cols=['v1', 'v2'])

print("joining finished, took %0.fs" % (timeit.default_timer()-task_init), flush=True)

exit(0)
