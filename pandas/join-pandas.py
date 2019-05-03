#!/usr/bin/env python3
import os
import gc
import timeit
import pandas as pd
import sys
import argparse

exec(open("./helpers.py").read())

src_x = os.environ['SRC_X_LOCAL']
src_y = os.environ['SRC_Y_LOCAL']

ver = pd.__version__
git = ""
task = "join"
question = "inner join"
l = [os.path.basename(src_x), os.path.basename(src_y)]
data_name = '-'.join(l)
solution = "pandas"
fun = "merge"
cache = "TRUE"

if os.path.basename(src_x) == "X1e9_2c.csv":
    print("# join with pandas skipped for 1e9 x 1e9 (20GB x 20GB) due to memory error on 125GB mem machine")
    sys.exit(0)

print("loading datasets...")

# with hd.open(src_x) as f:
#    x = pd.read_csv(f)
x = pd.read_csv(os.path.basename(src_x))

# with hd.open(src_y) as f:
#    y = pd.read_csv(f)
y = pd.read_csv(os.path.basename(src_y))

print("joining...")

def run_benchmark(run: int):
    '''Run benchark for join operation.

    run: The iteration number of current run.
    '''
    t_start = timeit.default_timer()
    ans = x.merge(y, how='inner', on='KEY')
    print('Shape:', ans.shape)
    m = memory_usage()
    t_start = timeit.default_timer()
    chk = [ans['X2'].sum(), ans['Y2'].sum()]
    chkt = timeit.default_timer() - t_start
    write_log(task=task, data=data_name, in_rows=x.shape[0], question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=run, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Run pandas join operation.')
    parser.add_argument('--run', type=int, required=True)
    args = parser.parse_args()

    run_benchmark(args.run)
    sys.exit(0)
