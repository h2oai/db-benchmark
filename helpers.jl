using Printf; # sprintf macro to print in non-scientific format
using Pkg;

# from https://github.com/JuliaLang/Pkg.jl/issues/793
function getpkgmeta(name::AbstractString)
    fname = joinpath(dirname(Base.active_project()), "Manifest.toml")
    Pkg.TOML.parse(read(fname, String))[name][1]
end;

function memory_usage()
  pid = getpid()
  s = read(pipeline(`ps -o rss $pid`,`tail -1`), String)
  parse(Float64, replace(s, "\n" => "")) / (1024^2)
end;

function make_chk(x)
  n = length(x)
  res = ""
  for i = 1:n
    res = string(res, i==1 ? "" : ";", @sprintf("%0.3f", x[i]))
  end
  res
end;

function write_log(run, task, data, in_rows, question, out_rows, out_cols, solution, version, git, fun, time_sec, mem_gb, cache, chk, chk_time_sec)
  file=try
    ENV["CSV_TIME_FILE"]
  catch
    "time.csv"
  end;
  file="$(pwd())/$file";
  batch=try
    ENV["BATCH"]
  catch
    ""
  end;
  nodename=gethostname()
  comment="" # placeholder for updates to timing data
  time_sec=round(time_sec, digits=3)
  mem_gb=round(mem_gb, digits=3)
  chk_time_sec=round(chk_time_sec, digits=3)
  timestamp=@sprintf("%0.6f", time())
  csv_verbose = false # hardcoded for now, TODO ENV["CSV_VERBOSE"] and print
  log = DataFrame(nodename=nodename, batch=batch, timestamp=timestamp, task=task, data=data, in_rows=in_rows, question=question, out_rows=out_rows, out_cols=out_cols, solution=solution, version=version, git=git, fun=fun, run=run, time_sec=time_sec, mem_gb=mem_gb, cache=uppercase(string(cache)), chk=chk, chk_time_sec=chk_time_sec, comment=comment)
  CSV.write(file, log, append=true)
end;
