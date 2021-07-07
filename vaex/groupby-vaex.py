#!/usr/bin/env python

print("# groupby-vaex.py", flush=True)

import os
import gc
import timeit
import vaex
vaex.multithreading.thread_count_default = 8

exec(open("./_helpers/helpers.py").read())

ver = vaex.__version__['vaex-core']
git = '-'
task = "groupby"
solution = "vaex"
fun = ".groupby"
cache = "TRUE"
on_disk = "TRUE"

# vaex.cache.redis()

data_name = os.environ['SRC_DATANAME']
src_grp = os.path.join("data", data_name+".csv")
print("loading dataset %s" % data_name, flush=True)

x = vaex.open(src_grp, convert=True,  dtype={"id4":"Int32", "id5":"Int32", "id6":"Int32", "v1":"Int32", "v2":"Int32"})
print("loaded dataset")
x.ordinal_encode('id1', inplace=True)
x.ordinal_encode('id2', inplace=True)
x.ordinal_encode('id3', inplace=True)
x.ordinal_encode('id4', inplace=True)
x.ordinal_encode('id5', inplace=True)
x.ordinal_encode('id6', inplace=True)


# Generic benchmark function - to improve code readability
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
    print(ans.shape, flush=True)
    t = timeit.default_timer() - t_start
    m = memory_usage()
    t_start = timeit.default_timer()
    chk = [ans[col].sum() for col in chk_sum_cols]
    chkt = timeit.default_timer() - t_start
    write_log(task=task, data=data_name, in_rows=x.shape[0], question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=2, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
    print(ans.head(3), flush=True)
    print(ans.tail(3), flush=True)
    del ans


# Questions
def question_1():
    return x.groupby(['id1']).agg({'v1': 'sum'})


def question_2():
    return x.groupby(['id1', 'id2']).agg({'v1': 'sum'})


def question_3():
    return x.groupby(['id3']).agg({'v1': 'sum', 'v3': 'mean'})


def question_4():
    return x.groupby(['id4']).agg({'v1':'mean', 'v2':'mean', 'v3':'mean'})


def question_5():
    return x.groupby(['id6']).agg({'v1': 'sum', 'v2': 'sum', 'v3': 'sum'})


def question_6():
    return x.groupby(['id4','id5']).agg({'v3': ['median','std']})


def question_7():
    return x.groupby(['id3']).agg({'v1': 'max', 'v2': 'min'}).assign(range_v1_v2=lambda x: x['v1'] - x['v2'])[['range_v1_v2']]


def question_8():
    return x[['id6','v3']].sort_values('v3', ascending=False).groupby(['id6']).head(2)


def question_9():
    return x[['id2','id4','v1','v2']].groupby(['id2','id4']).apply(lambda x: vaex.Series({'r2': x.corr()['v1']['v2']**2}))


def question_10():
    return x.groupby(['id1','id2','id3','id4','id5','id6']).agg({'v3':'sum', 'v1':'count'})


task_init = timeit.default_timer()
print("grouping...", flush=True)

benchmark(question_1, question="sum v1 by id1", chk_sum_cols=['v1'])
benchmark(question_2, question="sum v1 by id1:id2", chk_sum_cols=['v1'])
benchmark(question_3, question="sum v1 mean v3 by id3", chk_sum_cols=['v1', 'v3'])
benchmark(question_4, question="mean v1:v3 by id4", chk_sum_cols=['v1', 'v2', 'v3'])
benchmark(question_5, question="sum v1:v3 by id6", chk_sum_cols=['v1', 'v2', 'v3'])
# benchmark(question_6, question="median v3 sd v3 by id4 id5", chk_sum_cols=['v3_median', 'v3_std'])
# benchmark(question_7, question="max v1 - min v2 by id3", chk_sum_cols=['range_v1_v2'])с
# benchmark(question_8, question="largest two v3 by id6", chk_sum_cols=['v3'])
# benchmark(question_9, question="regression v1 v2 by id2 id4", chk_sum_cols=['r2'])
# benchmark(question_10, question="sum v3 count by id1:id6", chk_sum_cols=['v3', 'v1'])

print("grouping finished, took %0.3fs" % (timeit.default_timer()-task_init), flush=True)

exit(0)
