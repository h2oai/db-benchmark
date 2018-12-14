#!/usr/bin/env julia

print("# groupby-juliadf.jl\n"); flush(stdout);

using DataFrames;
using CSV;
using Statistics; # mean function

include("$(pwd())/helpers.jl");

pkgmeta = getpkgmeta("DataFrames");
ver = pkgmeta["version"];
git = pkgmeta["git-tree-sha1"];
task = "groupby";
solution = "juliadf";
fun = "by";
cache = true;

data_name = ENV["SRC_GRP_LOCAL"];
src_grp = string("data/", data_name, ".csv")
println(string("loading dataset ", data_name)); flush(stdout);

x = CSV.read(src_grp, categorical=0.05);
in_rows = size(x, 1);
println(in_rows); flush(stdout);

print("grouping...\n"); flush(stdout);

question = "sum v1 by id1"; # q1
GC.gc();
t_start = time_ns();
ANS = by(x, :id1, v1 = :v1=>sum);
println(size(ANS)); flush(stdout);
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
println(size(ANS)); flush(stdout);
t = (time_ns() - t_start)/1.0e9;
m = memory_usage();
t_start = time_ns();
chk = sum(ANS.v1);
chkt = (time_ns() - t_start)/1.0e9;
write_log(2, task, data_name, in_rows, question, size(ANS, 1), size(ANS, 2), solution, ver, git, fun, t, m, cache, make_chk(chk), chkt);
ANS = 0;

question = "sum v1 by id1:id2"; # q2
GC.gc();
t_start = time_ns();
ANS = by(x, [:id1, :id2], v1 = :v1=>sum);
println(size(ANS)); flush(stdout);
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
println(size(ANS)); flush(stdout);
t = (time_ns() - t_start)/1.0e9;
m = memory_usage();
t_start = time_ns();
chk = sum(ANS.v1);
chkt = (time_ns() - t_start)/1.0e9;
write_log(2, task, data_name, in_rows, question, size(ANS, 1), size(ANS, 2), solution, ver, git, fun, t, m, cache, make_chk(chk), chkt);
ANS = 0;

question = "sum v1 mean v3 by id3"; # q3
GC.gc();
t_start = time_ns();
ANS = by(x, :id3, v1 = :v1=>sum, v3 = :v3=>mean);
println(size(ANS)); flush(stdout);
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
println(size(ANS)); flush(stdout);
t = (time_ns() - t_start)/1.0e9;
m = memory_usage();
t_start = time_ns();
chk = [sum(ANS.v1), sum(ANS.v3)];
chkt = (time_ns() - t_start)/1.0e9;
write_log(2, task, data_name, in_rows, question, size(ANS, 1), size(ANS, 2), solution, ver, git, fun, t, m, cache, make_chk(chk), chkt);
ANS = 0;

question = "mean v1:v3 by id4"; # q4
GC.gc();
t_start = time_ns();
ANS = by(x, :id4, v1 = :v1=>mean, v2 = :v2=>mean, v3 = :v3=>mean);
println(size(ANS)); flush(stdout);
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
println(size(ANS)); flush(stdout);
t = (time_ns() - t_start)/1.0e9;
m = memory_usage();
t_start = time_ns();
chk = [sum(ANS.v1), sum(ANS.v2), sum(ANS.v3)];
chkt = (time_ns() - t_start)/1.0e9;
write_log(2, task, data_name, in_rows, question, size(ANS, 1), size(ANS, 2), solution, ver, git, fun, t, m, cache, make_chk(chk), chkt);
ANS = 0;

question = "sum v1:v3 by id6"; # q5
GC.gc();
t_start = time_ns();
ANS = by(x, :id6, v1 = :v1=>sum, v2 = :v2=>sum, v3 = :v3=>sum);
println(size(ANS)); flush(stdout);
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
println(size(ANS)); flush(stdout);
t = (time_ns() - t_start)/1.0e9;
m = memory_usage();
t_start = time_ns();
chk = [sum(ANS.v1), sum(ANS.v2), sum(ANS.v3)];
chkt = (time_ns() - t_start)/1.0e9;
write_log(2, task, data_name, in_rows, question, size(ANS, 1), size(ANS, 2), solution, ver, git, fun, t, m, cache, make_chk(chk), chkt);
ANS = 0;

question = "sum v3 count by id1:id6"; # q6
GC.gc();
t_start = time_ns();
ANS = by(x, [:id1, :id2, :id3, :id4, :id5, :id6], v3 = :v3=>sum, count = :v3=>length);
println(size(ANS)); flush(stdout);
t = (time_ns() - t_start)/1.0e9;
m = memory_usage();
t_start = time_ns();
chk = [sum(ANS.v3), sum(ANS.count)];
chkt = (time_ns() - t_start)/1.0e9;
write_log(1, task, data_name, in_rows, question, size(ANS, 1), size(ANS, 2), solution, ver, git, fun, t, m, cache, make_chk(chk), chkt);
ANS = 0;
GC.gc();
t_start = time_ns();
ANS = by(x, [:id1, :id2, :id3, :id4, :id5, :id6], v3 = :v3=>sum, count = :v3=>length);
println(size(ANS)); flush(stdout);
t = (time_ns() - t_start)/1.0e9;
m = memory_usage();
t_start = time_ns();
chk = [sum(ANS.v3), sum(ANS.count)];
chkt = (time_ns() - t_start)/1.0e9;
write_log(2, task, data_name, in_rows, question, size(ANS, 1), size(ANS, 2), solution, ver, git, fun, t, m, cache, make_chk(chk), chkt);
ANS = 0;

exit();
