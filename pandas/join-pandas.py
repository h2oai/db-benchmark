#!/usr/bin/env python

print("# join-pandas.py")

import os
import gc
import time
import timeit
import csv
import math
import pandas as pd
import pydoop.hdfs as hd

def write_log(timestamp, task, data, in_rows, out_rows, solution, fun, run, time_sec, mem_gb):
   csv_file = os.environ['CSV_TIME_FILE']
   if math.isnan(time_sec):
      time_sec = ""
   if math.isnan(mem_gb):
      mem_gb = ""
   log_row = [timestamp, task, data, in_rows, out_rows, solution, fun, run, time_sec, mem_gb]
   log_header = ["timestamp","task","data","in_rows","out_rows","solution","fun","run","time_sec","mem_gb"]
   append = os.path.isfile(csv_file)
   print('# ' + ','.join(str(x) for x in log_row))
   if append:
      with open(csv_file, 'a') as f:
         w = csv.writer(f)
         w.writerow(log_row)
   else:
      with open(csv_file, 'w+') as f:
         w = csv.writer(f)
         w.writerow(log_header)
         w.writerow(log_row)
   return True

src_x = os.environ['SRC_X_LOCAL']
src_y = os.environ['SRC_Y_LOCAL']
# TODO skip for total row count > 2e8 as data volume cap due to pandas scalability, currently just comment out in run.sh

print("loading datasets...")

with hd.open(src_x) as f:
   x = pd.read_csv(f)

with hd.open(src_y) as f:
   y = pd.read_csv(f)

print("join 1...")
t_start = timeit.default_timer()
ans = x.merge(y, how='inner', on='KEY')
ans.shape[0]
t_end = timeit.default_timer()
t = t_end - t_start
m = float('nan')
write_log(timestamp=time.time(), task="join", data="", in_rows=x.shape[0], out_rows=ans.shape[0], solution="pandas", fun="merge", run=1, time_sec=round(t, 3), mem_gb=round(m, 3))
del ans
gc.collect()

print("join 2...")
t_start = timeit.default_timer()
ans = x.merge(y, how='inner', on='KEY')
ans.shape[0]
t_end = timeit.default_timer()
t = t_end - t_start
m = float('NaN')
write_log(timestamp=time.time(), task="join", data="", in_rows=x.shape[0], out_rows=ans.shape[0], solution="pandas", fun="merge", run=2, time_sec=round(t, 3), mem_gb=round(m, 3))
del ans
gc.collect()

print("join 3...")
t_start = timeit.default_timer()
ans = x.merge(y, how='inner', on='KEY')
ans.shape[0]
t_end = timeit.default_timer()
t = t_end - t_start
m = float('NaN')
write_log(timestamp=time.time(), task="join", data="", in_rows=x.shape[0], out_rows=ans.shape[0], solution="pandas", fun="merge", run=3, time_sec=round(t, 3), mem_gb=round(m, 3))
del ans

exit(0)
