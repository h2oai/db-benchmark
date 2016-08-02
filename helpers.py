import time
import csv
import math

def write_log(task, data, in_rows, question, out_rows, solution, version, fun, run, time_sec, mem_gb, cache, chk, chk_time_sec):
   batch = os.getenv('BATCH', "") 
   timestamp = time.time()
   csv_file = os.getenv('CSV_TIME_FILE', "time.csv")
   comment = "" # placeholder for updates to timing data
   time_sec = round(time_sec, 3)
   chk_time_sec = round(chk_time_sec, 3)
   mem_gb = round(mem_gb, 3)
   if math.isnan(time_sec):
      time_sec = ""
   if math.isnan(mem_gb):
      mem_gb = ""
   git = ""
   log_row = [batch, timestamp, task, data, in_rows, question, out_rows, solution, version, git, fun, run, time_sec, mem_gb, cache, chk, chk_time_sec, comment]
   log_header = ["batch","timestamp","task","data","in_rows","question","out_rows","solution","version","git","fun","run","time_sec","mem_gb","cache","chk","chk_time_sec","comment"]
   append = os.path.isfile(csv_file)
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

def make_chk(values):
   s = ';'.join(str_round(x) for x in values)
   return s.replace(",","_") # comma is reserved for csv separator
