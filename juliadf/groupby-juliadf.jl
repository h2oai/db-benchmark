#!/usr/bin/env julia

print("# groupby-juliadf.jl\n");

using DataFrames;
using CSV; #Feather;
using Statistics; # mean function

include("$(pwd())/helpers.jl");

pkgmeta = getpkgmeta("DataFrames");
ver = pkgmeta["version"];
git = pkgmeta["git-tree-sha1"];
task = "groupby";
solution = "juliadf";
fun = "by";
cache = true;

src_grp = ENV["SRC_GRP_LOCAL"];
data_name = SubString(src_grp, 1, length(src_grp)-4);
println(string("loading dataset ", data_name))

#x = Feather.materialize(string("data/", src_grp)); # JuliaData/Feather.jl#97
x = CSV.read(string("data/", src_grp), categorical=true);
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
ANS = by(x, :id1, v1 = :v1=>sum);
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
ANS = by(x, :id1, v1 = :v1=>sum);
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
ANS = by(x, [:id1, :id2], v1 = :v1=>sum);
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
ANS = by(x, [:id1, :id2], v1 = :v1=>sum);
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
ANS = by(x, :id3, v1 = :v1=>sum, v3 = :v3=>mean);
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
ANS = by(x, :id3, v1 = :v1=>sum, v3 = :v3=>mean);
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
ANS = by(x, :id4, v1 = :v1=>mean, v2 = :v2=>mean, v3 = :v3=>mean);
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
ANS = by(x, :id4, v1 = :v1=>mean, v2 = :v2=>mean, v3 = :v3=>mean);
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
ANS = by(x, :id6, v1 = :v1=>sum, v2 = :v2=>sum, v3 = :v3=>sum);
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
ANS = by(x, :id6, v1 = :v1=>sum, v2 = :v2=>sum, v3 = :v3=>sum);
println(size(ANS));
t = (time_ns() - t_start)/1.0e9;
m = memory_usage();
t_start = time_ns();
chk = [sum(ANS.v1), sum(ANS.v2), sum(ANS.v3)];
chkt = (time_ns() - t_start)/1.0e9;
write_log(2, task, data_name, in_rows, question, size(ANS, 1), size(ANS, 2), solution, ver, git, fun, t, m, cache, make_chk(chk), chkt);
ANS = 0;

exit();
