#!/usr/bin/env julia

print("# join-juliadf.jl\n"); flush(stdout);

using DataFrames
using CSV
using Printf

# Precompile methods for common patterns
DataFrames.precompile(true);

include("$(pwd())/_helpers/helpers.jl");

pkgmeta = getpkgmeta("DataFrames");
ver = pkgmeta["version"];
git = pkgmeta["git-tree-sha1"];
task = "join";
solution = "juliadf";
fun = "join";
cache = true;
on_disk = false;

data_name = ENV["SRC_DATANAME"];
src_jn_x = string("data/", data_name, ".csv");
y_data_name = join_to_tbls(data_name);
src_jn_y = [string("data/", y_data_name[1], ".csv"), string("data/", y_data_name[2], ".csv"), string("data/", y_data_name[3], ".csv")];
if length(src_jn_y) != 3
    error("Something went wrong in preparing files used for join")
end;

println(string("loading datasets ", data_name, ", ", y_data_name[1], ", ", y_data_name[2], ", ", y_data_name[3])); flush(stdout);

x_df = CSV.read(src_jn_x, DataFrame, types=[Int32, Int32, Int32, String15, String15, String15, Float64], ntasks=1, pool=true);
small_df = CSV.read(src_jn_y[1], DataFrame, types=[Int32, String15, Float64], ntasks=1, pool=true);
medium_df = CSV.read(src_jn_y[2], DataFrame, types=[Int32, Int32, String15, String15, Float64], ntasks=1, pool=true);
big_df = CSV.read(src_jn_y[3], DataFrame, types=[Int32, Int32, Int32, String15, String15, String15, Float64], ntasks=1, pool=true);

in_rows = size(x_df, 1);
println(in_rows); flush(stdout);
println(size(small_df, 1)); flush(stdout);
println(size(medium_df, 1)); flush(stdout);
println(size(big_df, 1)); flush(stdout);

task_init = time();
print("joining...\n"); flush(stdout);

question = "small inner on int"; # q1
GC.gc(false);
t = @elapsed (ANS = innerjoin(x_df, small_df, on = :id1, makeunique=true, matchmissing=:equal); println(size(ANS)); flush(stdout));
m = memory_usage();
chkt = @elapsed chk = [sum(skipmissing(ANS.v1)), sum(skipmissing(ANS.v2))];
write_log(1, task, data_name, in_rows, question, size(ANS, 1), size(ANS, 2), solution, ver, git, fun, t, m, cache, make_chk(chk), chkt, on_disk);
ANS = 0;
GC.gc(false);
t = @elapsed (ANS = innerjoin(x_df, small_df, on = :id1, makeunique=true, matchmissing=:equal); println(size(ANS)); flush(stdout));
m = memory_usage();
chkt = @elapsed chk = [sum(skipmissing(ANS.v1)), sum(skipmissing(ANS.v2))];
write_log(2, task, data_name, in_rows, question, size(ANS, 1), size(ANS, 2), solution, ver, git, fun, t, m, cache, make_chk(chk), chkt, on_disk);
println(first(ANS, 3));
println(last(ANS, 3));
ANS = 0;

question = "medium inner on int"; # q2
GC.gc(false);
t = @elapsed (ANS = innerjoin(x_df, medium_df, on = :id2, makeunique=true, matchmissing=:equal); println(size(ANS)); flush(stdout));
m = memory_usage();
chkt = @elapsed chk = [sum(skipmissing(ANS.v1)), sum(skipmissing(ANS.v2))];
write_log(1, task, data_name, in_rows, question, size(ANS, 1), size(ANS, 2), solution, ver, git, fun, t, m, cache, make_chk(chk), chkt, on_disk);
ANS = 0;
GC.gc(false);
t = @elapsed (ANS = innerjoin(x_df, medium_df, on = :id2, makeunique=true, matchmissing=:equal); println(size(ANS)); flush(stdout));
m = memory_usage();
chkt = @elapsed chk = [sum(skipmissing(ANS.v1)), sum(skipmissing(ANS.v2))];
write_log(2, task, data_name, in_rows, question, size(ANS, 1), size(ANS, 2), solution, ver, git, fun, t, m, cache, make_chk(chk), chkt, on_disk);
println(first(ANS, 3));
println(last(ANS, 3));
ANS = 0;

question = "medium outer on int"; # q3
GC.gc(false);
t = @elapsed (ANS = leftjoin(x_df, medium_df, on = :id2, makeunique=true, matchmissing=:equal); println(size(ANS)); flush(stdout));
m = memory_usage();
chkt = @elapsed chk = [sum(skipmissing(ANS.v1)), sum(skipmissing(ANS.v2))];
write_log(1, task, data_name, in_rows, question, size(ANS, 1), size(ANS, 2), solution, ver, git, fun, t, m, cache, make_chk(chk), chkt, on_disk);
ANS = 0;
GC.gc(false);
t = @elapsed (ANS = leftjoin(x_df, medium_df, on = :id2, makeunique=true, matchmissing=:equal); println(size(ANS)); flush(stdout));
m = memory_usage();
chkt = @elapsed chk = [sum(skipmissing(ANS.v1)), sum(skipmissing(ANS.v2))];
write_log(2, task, data_name, in_rows, question, size(ANS, 1), size(ANS, 2), solution, ver, git, fun, t, m, cache, make_chk(chk), chkt, on_disk);
println(first(ANS, 3));
println(last(ANS, 3));
ANS = 0;

question = "medium inner on factor"; # q4
GC.gc(false);
t = @elapsed (ANS = innerjoin(x_df, medium_df, on = :id5, makeunique=true, matchmissing=:equal); println(size(ANS)); flush(stdout));
m = memory_usage();
t_start = time_ns();
chkt = @elapsed chk = [sum(skipmissing(ANS.v1)), sum(skipmissing(ANS.v2))];
write_log(1, task, data_name, in_rows, question, size(ANS, 1), size(ANS, 2), solution, ver, git, fun, t, m, cache, make_chk(chk), chkt, on_disk);
ANS = 0;
GC.gc(false);
t = @elapsed (ANS = innerjoin(x_df, medium_df, on = :id5, makeunique=true, matchmissing=:equal); println(size(ANS)); flush(stdout));
m = memory_usage();
chkt = @elapsed chk = [sum(skipmissing(ANS.v1)), sum(skipmissing(ANS.v2))];
write_log(2, task, data_name, in_rows, question, size(ANS, 1), size(ANS, 2), solution, ver, git, fun, t, m, cache, make_chk(chk), chkt, on_disk);
println(first(ANS, 3));
println(last(ANS, 3));
ANS = 0;

question = "big inner on int"; # q5
GC.gc(false);
t = @elapsed (ANS = innerjoin(x_df, big_df, on = :id3, makeunique=true, matchmissing=:equal); println(size(ANS)); flush(stdout));
m = memory_usage();
chkt = @elapsed chk = [sum(skipmissing(ANS.v1)), sum(skipmissing(ANS.v2))];
write_log(1, task, data_name, in_rows, question, size(ANS, 1), size(ANS, 2), solution, ver, git, fun, t, m, cache, make_chk(chk), chkt, on_disk);
ANS = 0;
GC.gc(false);
t = @elapsed (ANS = innerjoin(x_df, big_df, on = :id3, makeunique=true, matchmissing=:equal); println(size(ANS)); flush(stdout));
m = memory_usage();
chkt = @elapsed chk = [sum(skipmissing(ANS.v1)), sum(skipmissing(ANS.v2))];
write_log(2, task, data_name, in_rows, question, size(ANS, 1), size(ANS, 2), solution, ver, git, fun, t, m, cache, make_chk(chk), chkt, on_disk);
println(first(ANS, 3));
println(last(ANS, 3));
ANS = 0;

print(@sprintf "joining finished, took %.0fs\n" (time()-task_init)); flush(stdout);

exit();
