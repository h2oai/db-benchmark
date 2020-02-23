import time
import csv
import math
import psutil
import os
import platform

def write_log(task, data, in_rows, question, out_rows, out_cols, solution, version, git, fun, run, time_sec, mem_gb, cache, chk, chk_time_sec, on_disk):
   batch = os.getenv('BATCH', "") 
   timestamp = time.time()
   csv_file = os.getenv('CSV_TIME_FILE', "time.csv")
   nodename = platform.node()
   comment = "" # placeholder for updates to timing data
   time_sec = round(time_sec, 3)
   chk_time_sec = round(chk_time_sec, 3)
   mem_gb = round(mem_gb, 3)
   if math.isnan(time_sec):
      time_sec = ""
   if math.isnan(mem_gb):
      mem_gb = ""
   log_row = [nodename, batch, timestamp, task, data, in_rows, question, out_rows, out_cols, solution, version, git, fun, run, time_sec, mem_gb, cache, chk, chk_time_sec, comment, on_disk]
   log_header = ["nodename","batch","timestamp","task","data","in_rows","question","out_rows","out_cols","solution","version","git","fun","run","time_sec","mem_gb","cache","chk","chk_time_sec","comment","on_disk"]
   if os.path.isfile(csv_file) and not(os.path.getsize(csv_file)):
      os.remove(csv_file)
   append = os.path.isfile(csv_file)
   csv_verbose = os.getenv('CSV_VERBOSE', "false")
   if csv_verbose.lower()=="true":
      print('# ' + ','.join(str(x) for x in log_row))
   if append:
      with open(csv_file, 'a') as f:
         w = csv.writer(f, lineterminator='\n')
         w.writerow(log_row)
   else:
      with open(csv_file, 'w+') as f:
         w = csv.writer(f, lineterminator='\n')
         w.writerow(log_header)
         w.writerow(log_row)
   return True

def str_round(x):
   if type(x).__name__ in ["float","float64"]:
      x = round(x,3)
   return str(x)

flatten = lambda l: [item for sublist in l for item in sublist]

def make_chk(values):
   s = ';'.join(str_round(x) for x in values)
   return s.replace(",","_") # comma is reserved for csv separator

def memory_usage():
    process = psutil.Process(os.getpid())
    return process.memory_info().rss/(1024**3) # GB units

def join_to_tbls(data_name):
    x_n = int(float(data_name.split("_")[1]))
    y_n = ["{:.0e}".format(x_n/1e6), "{:.0e}".format(x_n/1e3), "{:.0e}".format(x_n)]
    y_n = [y_n[0].replace('+0', ''), y_n[1].replace('+0', ''), y_n[2].replace('+0', '')]
    return [data_name.replace('NA', y_n[0]), data_name.replace('NA', y_n[1]), data_name.replace('NA', y_n[2])]
