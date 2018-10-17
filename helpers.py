#!/usr/bin/env python
import csv
import gc
import math
import os
import psutil
import time


# TODO: convert this to a private method of BenchmarkTask,
#       once all solutions start using that class
def write_log(task, data, in_rows, question, out_rows, out_cols, solution,
              version, git, fun, run, time_sec, mem_gb, cache, chk,
              chk_time_sec):
    batch = os.getenv('BATCH', "")
    timestamp = time.time()
    csv_file = os.getenv('CSV_TIME_FILE', "time.csv")
    comment = ""  # placeholder for updates to timing data
    time_sec = round(time_sec, 3)
    chk_time_sec = round(chk_time_sec, 3)
    mem_gb = round(mem_gb, 3)
    if math.isnan(time_sec):
        time_sec = ""
    if math.isnan(mem_gb):
        mem_gb = ""
    log_row = [batch, timestamp, task, data, in_rows, question, out_rows,
               out_cols, solution, version, git, fun, run, time_sec, mem_gb,
               cache, chk, chk_time_sec, comment]
    log_header = ["batch", "timestamp", "task", "data", "in_rows", "question",
                  "out_rows", "out_cols", "solution", "version", "git", "fun",
                  "run", "time_sec", "mem_gb", "cache", "chk", "chk_time_sec",
                  "comment"]
    append = os.path.isfile(csv_file)
    csv_verbose = os.getenv('CSV_VERBOSE', "true")
    if csv_verbose.lower() == "true":
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
    if type(x).__name__ in ["float", "float64"]:
        x = round(x, 3)
    return str(x)


def flatten(l):
    return [item for sublist in l for item in sublist]


def make_chk(values):
    s = ';'.join(str_round(x) for x in values)
    return s.replace(",", "_")  # comma is reserved for csv separator


def memory_usage():
    process = psutil.Process(os.getpid())
    return process.memory_info().rss/(1024**3)  # GB units


# ------------------------------------------------------------------------------

class BenchmarkTask:
    """
    Main class for benchmarking Python-based solutions. The subclasses are
    expected to override all variables marked as "???", as well as two methods
    `compute()` and `verify()`.
    """
    solution = "???"
    version = "???"
    gitver = "???"
    cache = "???"
    task = "???"
    fun = "???"
    question = "???"

    def __init__(self, data, filename):
        self._data = data
        self._data_name = os.path.basename(filename)

    def compute(self, x):
        """Compute the task's work for dataset `x`, and return the result"""
        raise NotImplementedError

    def verify(self, y):
        """
        Given `y` the result of the compute step, calculate task-specific
        summary stats on that dataset and return the result.
        """
        raise NotImplementedError

    def run(self):
        """Run the benchmark."""
        print("Performing task: <%s> on a dataset %r"
              % (self.question, self._data))
        for i in [1, 2, 3]:
            gc.collect()
            t_start = time.time()
            ans = self.compute(self._data)
            ans_shape = ans.shape
            t_compute = time.time() - t_start
            mem_used = memory_usage()
            t_start = time.time()
            chk = self.verify(ans)
            chk_str = make_chk(flatten(chk.topython()))
            t_verify = time.time() - t_start
            write_log(task=self.task,
                      solution=self.solution,
                      question=self.question,
                      fun=self.fun,
                      data=self._data_name,
                      in_rows=self._data.nrows,
                      out_rows=ans_shape[0],
                      out_cols=ans.shape[1],
                      version=self.version,
                      git=self.gitver,
                      run=i,
                      time_sec=t_compute,
                      mem_gb=mem_used,
                      cache=self.cache,
                      chk=chk_str,
                      chk_time_sec=t_verify)
            del ans
            del chk
            print("  Run %d: result shape = %r, time to run: %.3fs, "
                  "time to check: %.3fs"
                  % (i, ans_shape, t_compute, t_verify))
        print()
