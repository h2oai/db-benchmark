function memory_usage()
  missing
end;

function make_chk(x)
  string(x)
end;

function write_log(run, task, data, in_rows, question, out_rows, out_cols, solution, version, git, fun, time_sec, mem_gb, cache, chk, chk_time_sec)
  file="time-julia.csv"
  #batch=ENV["BATCH"]
  batch="" # TODO default
  comment="" # placeholder for updates to timing data
  time_sec=round(time_sec, digits=3)
  mem_gb="" #round(mem_gb, digits=3)
  chk_time_sec=round(chk_time_sec, digits=3)
  timestamp=time()
  csv_verbose = false # TODO default and print: ENV["CSV_VERBOSE"]
  log = DataFrame(batch=batch, timestamp=timestamp, task=task, data=data, in_rows=in_rows, question=question, out_rows=out_rows, out_cols=out_cols, solution=solution, version=version, git=git, fun=fun, run=run, time_sec=time_sec, mem_gb=mem_gb, cache=cache, chk=chk, chk_time_sec=chk_time_sec, comment=comment)
  CSV.write(file, log, append=true)
end;
