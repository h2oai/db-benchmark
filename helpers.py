import time
import csv
import math

def write_log(task, data, in_rows, out_rows, solution, fun, run, time_sec, mem_gb):
   batch = os.getenv('BATCH', "") 
   timestamp = time.time()
   csv_file = os.getenv('CSV_TIME_FILE', "time.csv")
   comment = "" # placeholder for updates to timing data
   time_sec = round(time_sec, 3)
   mem_gb = round(mem_gb, 3)
   if math.isnan(time_sec):
      time_sec = ""
   if math.isnan(mem_gb):
      mem_gb = ""
   log_row = [batch, timestamp, task, data, in_rows, out_rows, solution, fun, run, time_sec, mem_gb, comment]
   log_header = ["batch","timestamp","task","data","in_rows","out_rows","solution","fun","run","time_sec","mem_gb","comment"]
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
