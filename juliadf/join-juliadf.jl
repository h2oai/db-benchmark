#!/usr/bin/env julia

print("# join-juliadf.jl\n"); flush(stdout);

using DataFrames;
using CSV;

include("$(pwd())/helpers.jl");

pkgmeta = getpkgmeta("DataFrames");
ver = pkgmeta["version"];
git = pkgmeta["git-tree-sha1"];
task = "join";
solution = "juliadf";
fun = "join";
cache = true;

data_name = ENV["SRC_JN_LOCAL"];
src_jn_x = string("data/", data_name, ".csv");
y_data_name = join_to_tbls(data_name);
src_jn_y = [string("data/", y_data_name[1], ".csv"), string("data/", y_data_name[2], ".csv"), string("data/", y_data_name[3], ".csv")];
if length(src_jn_y) != 3
  error("Something went wrong in preparing files used for join")
end;

println(string("loading datasets ", data_name, ", ", y_data_name[1], ", ", y_data_name[2], ", ", y_data_name[3])); flush(stdout);

x = DataFrame(CSV.File(src_jn_x, pool=true));
small = DataFrame(CSV.File(src_jn_y[1], pool=true));
medium = DataFrame(CSV.File(src_jn_y[2], pool=true));
big = DataFrame(CSV.File(src_jn_y[3], pool=true));

in_rows = size(x, 1);
println(in_rows); flush(stdout);
println(size(small, 1)); flush(stdout);
println(size(medium, 1)); flush(stdout);
println(size(big, 1)); flush(stdout);

print("joining...\n"); flush(stdout);

question = "small inner on int"; # q1
GC.gc();
t = @elapsed (ANS = join(x, small, on = :id1, makeunique=true); println(size(ANS)); flush(stdout));
m = memory_usage();
chkt = @elapsed chk = [sum(ANS.v1), sum(ANS.v2)];
write_log(1, task, data_name, in_rows, question, size(ANS, 1), size(ANS, 2), solution, ver, git, fun, t, m, cache, make_chk(chk), chkt);
ANS = 0;
GC.gc();
t = @elapsed (ANS = join(x, small, on = :id1, makeunique=true); println(size(ANS)); flush(stdout));
m = memory_usage();
chkt = @elapsed chk = [sum(ANS.v1), sum(ANS.v2)];
write_log(2, task, data_name, in_rows, question, size(ANS, 1), size(ANS, 2), solution, ver, git, fun, t, m, cache, make_chk(chk), chkt);
println(first(ANS, 3));
println(last(ANS, 3));
ANS = 0;

question = "medium inner on int"; # q2
GC.gc();
t = @elapsed (ANS = join(x, medium, on = :id2, makeunique=true); println(size(ANS)); flush(stdout));
m = memory_usage();
chkt = @elapsed chk = [sum(ANS.v1), sum(ANS.v2)];
write_log(1, task, data_name, in_rows, question, size(ANS, 1), size(ANS, 2), solution, ver, git, fun, t, m, cache, make_chk(chk), chkt);
ANS = 0;
GC.gc();
t = @elapsed (ANS = join(x, medium, on = :id2, makeunique=true); println(size(ANS)); flush(stdout));
m = memory_usage();
chkt = @elapsed chk = [sum(ANS.v1), sum(ANS.v2)];
write_log(2, task, data_name, in_rows, question, size(ANS, 1), size(ANS, 2), solution, ver, git, fun, t, m, cache, make_chk(chk), chkt);
println(first(ANS, 3));
println(last(ANS, 3));
ANS = 0;

question = "medium outer on int"; # q3
GC.gc();
t = @elapsed (ANS = join(x, medium, kind = :left, on = :id2, makeunique=true); println(size(ANS)); flush(stdout));
m = memory_usage();
chkt = @elapsed chk = [sum(ANS.v1), sum(ANS.v2)];
write_log(1, task, data_name, in_rows, question, size(ANS, 1), size(ANS, 2), solution, ver, git, fun, t, m, cache, make_chk(chk), chkt);
ANS = 0;
GC.gc();
t = @elapsed (ANS = join(x, medium, kind = :left, on = :id2, makeunique=true); println(size(ANS)); flush(stdout));
m = memory_usage();
chkt = @elapsed chk = [sum(ANS.v1), sum(ANS.v2)];
write_log(2, task, data_name, in_rows, question, size(ANS, 1), size(ANS, 2), solution, ver, git, fun, t, m, cache, make_chk(chk), chkt);
println(first(ANS, 3));
println(last(ANS, 3));
ANS = 0;

question = "medium inner on factor"; # q4
GC.gc();
t = @elapsed (ANS = join(x, medium, on = :id5, makeunique=true); println(size(ANS)); flush(stdout));
m = memory_usage();
t_start = time_ns();
chkt = @elapsed chk = [sum(ANS.v1), sum(ANS.v2)];
write_log(1, task, data_name, in_rows, question, size(ANS, 1), size(ANS, 2), solution, ver, git, fun, t, m, cache, make_chk(chk), chkt);
ANS = 0;
GC.gc();
t = @elapsed (ANS = join(x, medium, on = :id5, makeunique=true); println(size(ANS)); flush(stdout));
m = memory_usage();
chkt = @elapsed chk = [sum(ANS.v1), sum(ANS.v2)];
write_log(2, task, data_name, in_rows, question, size(ANS, 1), size(ANS, 2), solution, ver, git, fun, t, m, cache, make_chk(chk), chkt);
println(first(ANS, 3));
println(last(ANS, 3));
ANS = 0;

question = "big inner on int"; # q5
GC.gc();
t = @elapsed (ANS = join(x, big, on = :id3, makeunique=true); println(size(ANS)); flush(stdout));
m = memory_usage();
chkt = @elapsed chk = [sum(ANS.v1), sum(ANS.v2)];
write_log(1, task, data_name, in_rows, question, size(ANS, 1), size(ANS, 2), solution, ver, git, fun, t, m, cache, make_chk(chk), chkt);
ANS = 0;
GC.gc();
t = @elapsed (ANS = join(x, big, on = :id3, makeunique=true); println(size(ANS)); flush(stdout));
m = memory_usage();
chkt = @elapsed chk = [sum(ANS.v1), sum(ANS.v2)];
write_log(2, task, data_name, in_rows, question, size(ANS, 1), size(ANS, 2), solution, ver, git, fun, t, m, cache, make_chk(chk), chkt);
println(first(ANS, 3));
println(last(ANS, 3));
ANS = 0;

exit();
