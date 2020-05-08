#!/usr/bin/env python
import os
import gc
import timeit
import pandas as pd
import dask as dk
import dask.dataframe as dd
import logging

if __name__ == "__main__":
    # we put whole test code inside main guard to because processes started by distributed
    # client might import this file

    print("# groupby-dask.py", flush=True)
    exec(open("./_helpers/helpers.py").read())

    ver = dk.__version__
    git = dk.__git_revision__
    task = "groupby"
    solution = "dask"
    fun = ".groupby"
    cache = "TRUE"

    data_name = os.environ['SRC_GRP_LOCAL']
    data_size, grp_size = data_name.split("_")[1:3]
    data_size, grp_size = int(float(data_size)), int(float(grp_size))

    from dask import distributed
    # we use process-pool instead of thread-pool due to GIL cost
    client = distributed.Client(processes=True, silence_logs=logging.ERROR)

    # since we are running on local cluster of processes, we would prefer to keep the communication
    # between workers to relative minimum, thus it's better to trade some tasks granularity for
    # better processing locality
    with dk.config.set({"optimization.fuse.ave-width": 20}):
        on_disk = data_size == 1e9 # on-disk data storage #126
        fext = "parquet" if on_disk else "csv"
        src_grp = os.path.join("data", data_name+"."+fext)
        print("loading dataset %s" % data_name, flush=True)

        print("using disk memory-mapped data storage" if on_disk else "using in-memory data storage", flush=True)
        if on_disk:
            x = dd.read_parquet(src_grp, engine="fastparquet")
        else:
            x = dd.read_csv(src_grp, na_filter=False, dtype={"id1": "category", "id2": "category", "id3": "str", "id4": "int32", "id5": "int32", "id6": "int32", "v1": "int32", "v2": "int32", "v3": "float64"})

        # utilize index for small groups
        x = x.set_index("id3")
        x = x.persist()
        # sync data reading, and rebalance data among workers:
        client.rebalance(x)

        in_rows = len(x)
        print(in_rows, flush=True)

        task_init = timeit.default_timer()
        print("grouping...", flush=True)

        question = "sum v1 by id1" # q1
        gc.collect()
        t_start = timeit.default_timer()
        ans = x.groupby(['id1']).agg({'v1':'sum'}).compute()
        ans.reset_index(inplace=True) # #68
        print(ans.shape, flush=True)
        t = timeit.default_timer() - t_start
        m = memory_usage()
        t_start = timeit.default_timer()
        chk = [ans.v1.sum()]
        chkt = timeit.default_timer() - t_start
        write_log(task=task, data=data_name, in_rows=in_rows, question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=1, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
        del ans
        gc.collect()
        t_start = timeit.default_timer()
        ans = x.groupby(['id1']).agg({'v1':'sum'}).compute()
        ans.reset_index(inplace=True)
        print(ans.shape, flush=True)
        t = timeit.default_timer() - t_start
        m = memory_usage()
        t_start = timeit.default_timer()
        chk = [ans.v1.sum()]
        chkt = timeit.default_timer() - t_start
        write_log(task=task, data=data_name, in_rows=in_rows, question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=2, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
        print(ans.head(3), flush=True)
        print(ans.tail(3), flush=True)
        del ans

        question = "sum v1 by id1:id2" # q2
        gc.collect()
        t_start = timeit.default_timer()
        ans = x.groupby(['id1','id2']).agg({'v1':'sum'}).compute()
        ans.reset_index(inplace=True)
        print(ans.shape, flush=True)
        t = timeit.default_timer() - t_start
        m = memory_usage()
        t_start = timeit.default_timer()
        chk = [ans.v1.sum()]
        chkt = timeit.default_timer() - t_start
        write_log(task=task, data=data_name, in_rows=in_rows, question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=1, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
        del ans
        gc.collect()
        t_start = timeit.default_timer()
        ans = x.groupby(['id1','id2']).agg({'v1':'sum'}).compute()
        ans.reset_index(inplace=True)
        print(ans.shape, flush=True)
        t = timeit.default_timer() - t_start
        m = memory_usage()
        t_start = timeit.default_timer()
        chk = [ans.v1.sum()]
        chkt = timeit.default_timer() - t_start
        write_log(task=task, data=data_name, in_rows=in_rows, question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=2, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
        print(ans.head(3), flush=True)
        print(ans.tail(3), flush=True)
        del ans

        question = "sum v1 mean v3 by id3" # q3
        gc.collect()
        t_start = timeit.default_timer()
        ans = x.groupby(['id3']).agg({'v1':'sum', 'v3':'mean'}).compute()
        ans.reset_index(inplace=True)
        print(ans.shape, flush=True)
        t = timeit.default_timer() - t_start
        m = memory_usage()
        t_start = timeit.default_timer()
        chk = [ans.v1.sum(), ans.v3.sum()]
        chkt = timeit.default_timer() - t_start
        write_log(task=task, data=data_name, in_rows=in_rows, question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=1, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
        del ans
        gc.collect()
        t_start = timeit.default_timer()
        ans = x.groupby(['id3']).agg({'v1':'sum', 'v3':'mean'}).compute()
        ans.reset_index(inplace=True)
        print(ans.shape, flush=True)
        t = timeit.default_timer() - t_start
        m = memory_usage()
        t_start = timeit.default_timer()
        chk = [ans.v1.sum(), ans.v3.sum()]
        chkt = timeit.default_timer() - t_start
        write_log(task=task, data=data_name, in_rows=in_rows, question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=2, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
        print(ans.head(3), flush=True)
        print(ans.tail(3), flush=True)
        del ans

        question = "mean v1:v3 by id4" # q4
        gc.collect()
        t_start = timeit.default_timer()
        ans = x.groupby(['id4']).agg({'v1':'mean', 'v2':'mean', 'v3':'mean'}).compute()
        ans.reset_index(inplace=True)
        print(ans.shape, flush=True)
        t = timeit.default_timer() - t_start
        m = memory_usage()
        t_start = timeit.default_timer()
        chk = [ans.v1.sum(), ans.v2.sum(), ans.v3.sum()]
        chkt = timeit.default_timer() - t_start
        write_log(task=task, data=data_name, in_rows=in_rows, question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=1, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
        del ans
        gc.collect()
        t_start = timeit.default_timer()
        ans = x.groupby(['id4']).agg({'v1':'mean', 'v2':'mean', 'v3':'mean'}).compute()
        ans.reset_index(inplace=True)
        print(ans.shape, flush=True)
        t = timeit.default_timer() - t_start
        m = memory_usage()
        t_start = timeit.default_timer()
        chk = [ans.v1.sum(), ans.v2.sum(), ans.v3.sum()]
        chkt = timeit.default_timer() - t_start
        write_log(task=task, data=data_name, in_rows=in_rows, question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=2, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
        print(ans.head(3), flush=True)
        print(ans.tail(3), flush=True)
        del ans

        question = "sum v1:v3 by id6" # q5
        gc.collect()
        t_start = timeit.default_timer()
        ans = x.groupby(['id6']).agg({'v1':'sum', 'v2':'sum', 'v3':'sum'}).compute()
        ans.reset_index(inplace=True)
        print(ans.shape, flush=True)
        t = timeit.default_timer() - t_start
        m = memory_usage()
        t_start = timeit.default_timer()
        chk = [ans.v1.sum(), ans.v2.sum(), ans.v3.sum()]
        chkt = timeit.default_timer() - t_start
        write_log(task=task, data=data_name, in_rows=in_rows, question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=1, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
        del ans
        gc.collect()
        t_start = timeit.default_timer()
        ans = x.groupby(['id6']).agg({'v1':'sum', 'v2':'sum', 'v3':'sum'}).compute()
        ans.reset_index(inplace=True)
        print(ans.shape, flush=True)
        t = timeit.default_timer() - t_start
        m = memory_usage()
        t_start = timeit.default_timer()
        chk = [ans.v1.sum(), ans.v2.sum(), ans.v3.sum()]
        chkt = timeit.default_timer() - t_start
        write_log(task=task, data=data_name, in_rows=in_rows, question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=2, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
        print(ans.head(3), flush=True)
        print(ans.tail(3), flush=True)
        del ans

        #question = "median v3 sd v3 by id4 id5" # q6 # median function not yet implemented: https://github.com/dask/dask/issues/4362
        #gc.collect()
        #t_start = timeit.default_timer()
        #ans = x.groupby(['id4','id5']).agg({'v3': ['median','std']}).compute()
        #ans.reset_index(inplace=True)
        #print(ans.shape, flush=True)
        #t = timeit.default_timer() - t_start
        #m = memory_usage()
        #t_start = timeit.default_timer()
        #chk = [ans['v3']['median'].sum(), ans['v3']['std'].sum()]
        #chkt = timeit.default_timer() - t_start
        #write_log(task=task, data=data_name, in_rows=in_rows, question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=1, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
        #del ans
        #gc.collect()
        #t_start = timeit.default_timer()
        #ans = x.groupby(['id4','id5']).agg({'v3': ['median','std']}).compute()
        #ans.reset_index(inplace=True)
        #print(ans.shape, flush=True)
        #t = timeit.default_timer() - t_start
        #m = memory_usage()
        #t_start = timeit.default_timer()
        #chk = [ans['v3']['median'].sum(), ans['v3']['std'].sum()]
        #chkt = timeit.default_timer() - t_start
        #write_log(task=task, data=data_name, in_rows=in_rows, question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=2, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
        #print(ans.head(3), flush=True)
        #print(ans.tail(3), flush=True)
        #del ans

        question = "max v1 - min v2 by id3" # q7
        gc.collect()
        t_start = timeit.default_timer()
        ans = x.groupby(['id3']).agg({'v1': 'max', 'v2': 'min'}).assign(range_v1_v2=lambda x: x['v1'] - x['v2'])[['range_v1_v2']].compute()
        ans.reset_index(inplace=True)
        print(ans.shape, flush=True)
        t = timeit.default_timer() - t_start
        m = memory_usage()
        t_start = timeit.default_timer()
        chk = [ans['range_v1_v2'].sum()]
        chkt = timeit.default_timer() - t_start
        write_log(task=task, data=data_name, in_rows=in_rows, question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=1, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
        del ans
        gc.collect()
        t_start = timeit.default_timer()
        ans = x.groupby(['id3']).agg({'v1': 'max', 'v2': 'min'}).assign(range_v1_v2=lambda x: x['v1'] - x['v2'])[['range_v1_v2']].compute()
        ans.reset_index(inplace=True)
        print(ans.shape, flush=True)
        t = timeit.default_timer() - t_start
        m = memory_usage()
        t_start = timeit.default_timer()
        chk = [ans['range_v1_v2'].sum()]
        chkt = timeit.default_timer() - t_start
        write_log(task=task, data=data_name, in_rows=in_rows, question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=2, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
        print(ans.head(3), flush=True)
        print(ans.tail(3), flush=True)
        del ans

        question = "largest two v3 by id6" # q8
        gc.collect()
        t_start = timeit.default_timer()
        ans = x[['id6','v3']].groupby(['id6']).apply(lambda x: x.nlargest(2, columns='v3'), meta={'id6': 'int64', 'v3': 'float64'})[['v3']].compute()
        ans.reset_index(level=("id6"), inplace=True)
        ans.reset_index(drop=True, inplace=True) # drop because nlargest creates some extra new index field
        print(ans.shape, flush=True)
        t = timeit.default_timer() - t_start
        m = memory_usage()
        t_start = timeit.default_timer()
        chk = [ans['v3'].sum()]
        chkt = timeit.default_timer() - t_start
        write_log(task=task, data=data_name, in_rows=in_rows, question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=1, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
        del ans
        gc.collect()
        t_start = timeit.default_timer()
        ans = x[['id6','v3']].groupby(['id6']).apply(lambda x: x.nlargest(2, columns='v3'), meta={'id6': 'int64', 'v3': 'float64'})[['v3']].compute()
        ans.reset_index(level=("id6"), inplace=True)
        ans.reset_index(drop=True, inplace=True)
        print(ans.shape, flush=True)
        t = timeit.default_timer() - t_start
        m = memory_usage()
        t_start = timeit.default_timer()
        chk = [ans['v3'].sum()]
        chkt = timeit.default_timer() - t_start
        write_log(task=task, data=data_name, in_rows=in_rows, question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=2, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
        print(ans.head(3), flush=True)
        print(ans.tail(3), flush=True)
        del ans

        #question = "regression v1 v2 by id2 id4" # q9 - https://github.com/dask/dask/issues/4828
        #gc.collect()
        #t_start = timeit.default_timer()
        #ans2 = x[['id2','id4','v1','v2']].groupby(['id2','id4']).cov().compute()
        #ans = x[['id2','id4','v1','v2']].groupby(['id2','id4']).apply(lambda x: pd.Series({'r2': x.corr()['v1']['v2']**2}), meta={'r2': 'float64'}).compute()
        #ans.reset_index(inplace=True)
        #print(ans.shape, flush=True)
        #t = timeit.default_timer() - t_start
        #m = memory_usage()
        #t_start = timeit.default_timer()
        #chk = [ans['r2'].sum()]
        #chkt = timeit.default_timer() - t_start
        #write_log(task=task, data=data_name, in_rows=in_rows, question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=1, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
        #del ans
        #gc.collect()
        #t_start = timeit.default_timer()
        #ans = x[['id2','id4','v1','v2']].groupby(['id2','id4']).apply(lambda x: pd.Series({'r2': x.corr()['v1']['v2']**2}), meta={'r2': 'float64'}).compute()
        #ans.reset_index(inplace=True)
        #print(ans.shape, flush=True)
        #t = timeit.default_timer() - t_start
        #m = memory_usage()
        #t_start = timeit.default_timer()
        #chk = [ans['r2'].sum()]
        #chkt = timeit.default_timer() - t_start
        #write_log(task=task, data=data_name, in_rows=in_rows, question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=2, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
        #print(ans.head(3), flush=True)
        #print(ans.tail(3), flush=True)
        #del ans

        question = "sum v3 count by id1:id6" # q10
        gc.collect()
        t_start = timeit.default_timer()
        ans = x.groupby(['id1','id2','id3','id4','id5','id6']).agg({'v3':'sum', 'v1':'count'}).compute() # column name different than expected, ignore it because: ValueError: Metadata inference failed in `rename`: Original error is below: ValueError('Level values must be unique: [nan, nan] on level 0',)
        ans.reset_index(inplace=True)
        print(ans.shape, flush=True)
        t = timeit.default_timer() - t_start
        m = memory_usage()
        t_start = timeit.default_timer()
        chk = [ans.v3.sum(), ans.v1.sum()]
        chkt = timeit.default_timer() - t_start
        write_log(task=task, data=data_name, in_rows=in_rows, question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=1, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
        del ans
        gc.collect()
        t_start = timeit.default_timer()
        ans = x.groupby(['id1','id2','id3','id4','id5','id6']).agg({'v3':'sum', 'v1':'count'}).compute()
        ans.reset_index(inplace=True)
        print(ans.shape, flush=True)
        t = timeit.default_timer() - t_start
        m = memory_usage()
        t_start = timeit.default_timer()
        chk = [ans.v3.sum(), ans.v1.sum()]
        chkt = timeit.default_timer() - t_start
        write_log(task=task, data=data_name, in_rows=in_rows, question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=2, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
        print(ans.head(3), flush=True)
        print(ans.tail(3), flush=True)
        del ans

        print("grouping finished, took %0.fs" % (timeit.default_timer()-task_init), flush=True)

        exit(0)
