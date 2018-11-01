#!/usr/bin/env julia

print("# groupby-juliadf.jl\n");

using DataFrames;
using CSV;
using Statistics; # mean function
using Pkg; # to get DataFrames version

include("$(pwd())/helpers.jl");

ver = string(Pkg.installed()["DataFrames"]);
git = ""; # https://github.com/JuliaLang/Pkg.jl/issues/793
task = "groupby";
solution = "juliadf";
fun = "by";
cache = true;

print("loading dataset...\n");
src_grp = ENV["SRC_GRP_LOCAL"];
data_name = basename(src_grp);

x = CSV.read(data_name, categorical=true);
in_rows = size(x, 1);
println(in_rows);

print("grouping...\n");

#ANS = aggregate(x[[:id1, :v1]], :id1, sum);
# above call:
#   cannot run sum(v1), mean(v2)
#   does not retain names
# thus we use `by ... DataFrame`, if it is possible to improve that please report
# see #30 and xiaodaigh/FastGroupBy.jl#7

question = "sum v1 by id1"; #1
GC.gc();
t_start = time_ns();
ANS = by(x, :id1) do df; DataFrame(v1 = sum(df.v1)); end;
println(size(ANS));
t = (time_ns() - t_start)/1.0e9;
m = memory_usage();
t_start = time_ns();
chk = sum(ANS.v1);
chkt = (time_ns() - t_start)/1.0e9;
write_log(1, task, data_name, in_rows, question, size(ANS, 1), size(ANS, 2), solution, ver, git, fun, t, m, cache, make_chk(chk), chkt);
ANS = 0;
GC.gc();
t_start = time_ns();
ANS = by(x, :id1) do df; DataFrame(v1 = sum(df.v1)); end;
println(size(ANS));
t = (time_ns() - t_start)/1.0e9;
m = memory_usage();
t_start = time_ns();
chk = sum(ANS.v1);
chkt = (time_ns() - t_start)/1.0e9;
write_log(2, task, data_name, in_rows, question, size(ANS, 1), size(ANS, 2), solution, ver, git, fun, t, m, cache, make_chk(chk), chkt);
ANS = 0;

question = "sum v1 by id1:id2" #2
GC.gc();
t_start = time_ns();
ANS = by(x, [:id1, :id2]) do df; DataFrame(v1 = sum(df.v1)); end;
println(size(ANS));
t = (time_ns() - t_start)/1.0e9;
m = memory_usage();
t_start = time_ns();
chk = sum(ANS.v1);
chkt = (time_ns() - t_start)/1.0e9;
write_log(1, task, data_name, in_rows, question, size(ANS, 1), size(ANS, 2), solution, ver, git, fun, t, m, cache, make_chk(chk), chkt);
ANS = 0;
GC.gc();
t_start = time_ns();
ANS = by(x, [:id1, :id2]) do df; DataFrame(v1 = sum(df.v1)); end;
println(size(ANS));
t = (time_ns() - t_start)/1.0e9;
m = memory_usage();
t_start = time_ns();
chk = sum(ANS.v1);
chkt = (time_ns() - t_start)/1.0e9;
write_log(2, task, data_name, in_rows, question, size(ANS, 1), size(ANS, 2), solution, ver, git, fun, t, m, cache, make_chk(chk), chkt);
ANS = 0;

question = "sum v1 mean v3 by id3" #3
GC.gc();
t_start = time_ns();
ANS = by(x, :id3) do df; DataFrame(v1 = sum(df.v1), v3 = mean(df.v3)); end;
println(size(ANS));
t = (time_ns() - t_start)/1.0e9;
m = memory_usage();
t_start = time_ns();
chk = [sum(ANS.v1), sum(ANS.v3)];
chkt = (time_ns() - t_start)/1.0e9;
write_log(1, task, data_name, in_rows, question, size(ANS, 1), size(ANS, 2), solution, ver, git, fun, t, m, cache, make_chk(chk), chkt);
ANS = 0;
GC.gc();
t_start = time_ns();
ANS = by(x, :id3) do df; DataFrame(v1 = sum(df.v1), v3 = mean(df.v3)); end;
println(size(ANS));
t = (time_ns() - t_start)/1.0e9;
m = memory_usage();
t_start = time_ns();
chk = [sum(ANS.v1), sum(ANS.v3)];
chkt = (time_ns() - t_start)/1.0e9;
write_log(2, task, data_name, in_rows, question, size(ANS, 1), size(ANS, 2), solution, ver, git, fun, t, m, cache, make_chk(chk), chkt);
ANS = 0;

question = "mean v1:v3 by id4" #4
GC.gc();
t_start = time_ns();
ANS = by(x, :id4) do df; DataFrame(v1 = mean(df.v1), v2 = mean(df.v2), v3 = mean(df.v3)); end;
println(size(ANS));
t = (time_ns() - t_start)/1.0e9;
m = memory_usage();
t_start = time_ns();
chk = [sum(ANS.v1), sum(ANS.v2), sum(ANS.v3)];
chkt = (time_ns() - t_start)/1.0e9;
write_log(1, task, data_name, in_rows, question, size(ANS, 1), size(ANS, 2), solution, ver, git, fun, t, m, cache, make_chk(chk), chkt);
ANS = 0;
GC.gc();
t_start = time_ns();
ANS = by(x, :id4) do df; DataFrame(v1 = mean(df.v1), v2 = mean(df.v2), v3 = mean(df.v3)); end;
println(size(ANS));
t = (time_ns() - t_start)/1.0e9;
m = memory_usage();
t_start = time_ns();
chk = [sum(ANS.v1), sum(ANS.v2), sum(ANS.v3)];
chkt = (time_ns() - t_start)/1.0e9;
write_log(2, task, data_name, in_rows, question, size(ANS, 1), size(ANS, 2), solution, ver, git, fun, t, m, cache, make_chk(chk), chkt);
ANS = 0;

question = "sum v1:v3 by id6" #5
GC.gc();
t_start = time_ns();
ANS = by(x, :id6) do df; DataFrame(v1 = sum(df.v1), v2 = sum(df.v2), v3 = sum(df.v3)); end;
println(size(ANS));
t = (time_ns() - t_start)/1.0e9;
m = memory_usage();
t_start = time_ns();
chk = [sum(ANS.v1), sum(ANS.v2), sum(ANS.v3)];
chkt = (time_ns() - t_start)/1.0e9;
write_log(1, task, data_name, in_rows, question, size(ANS, 1), size(ANS, 2), solution, ver, git, fun, t, m, cache, make_chk(chk), chkt);
ANS = 0;
GC.gc();
t_start = time_ns();
ANS = by(x, :id6) do df; DataFrame(v1 = sum(df.v1), v2 = sum(df.v2), v3 = sum(df.v3)); end;
println(size(ANS));
t = (time_ns() - t_start)/1.0e9;
m = memory_usage();
t_start = time_ns();
chk = [sum(ANS.v1), sum(ANS.v2), sum(ANS.v3)];
chkt = (time_ns() - t_start)/1.0e9;
write_log(2, task, data_name, in_rows, question, size(ANS, 1), size(ANS, 2), solution, ver, git, fun, t, m, cache, make_chk(chk), chkt);
ANS = 0;

exit();
