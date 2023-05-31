#!/usr/bin/env julia

print("# join-juliads.jl\n"); flush(stdout);

using InMemoryDatasets;
using Printf;
using DLMReader
using PooledArrays
using Arrow

# Force Julia to precompile methods for common patterns
IMD.warmup()

include("$(pwd())/_helpers/helpersds.jl");

pkgmeta = getpkgmeta("InMemoryDatasets");
ver = pkgmeta["version"];
git = "";
task = "join";
solution = "juliads";
fun = "join";
cache = true;
on_disk = false;
isondisk(indata) = parse(Float64, split(indata, "_")[2])>=10^9

data_name = ENV["SRC_DATANAME"];
src_jn_x = string("data/", data_name, ".csv");
y_data_name = join_to_tbls(data_name);
src_jn_y = [string("data/", y_data_name[1], ".csv"), string("data/", y_data_name[2], ".csv"), string("data/", y_data_name[3], ".csv")];
if length(src_jn_y) != 3
  error("Something went wrong in preparing files used for join")
end;

println(string("loading datasets ", data_name, ", ", y_data_name[1], ", ", y_data_name[2], ", ", y_data_name[3])); flush(stdout);

# temporary file which will be deleted after the run - usually located at /tmp/
_tmp_storage = tempname() 
if isondisk(data_name)
  on_disk = true
  big_df = filereader(src_jn_y[3], types=[Int32, Int32, Int32, Characters{6}, Characters{9}, Characters{12}, Float64]);
  modify!(big_df, [:id4, :id5]=>PooledArray)
  Arrow.write(_tmp_storage, big_df[!, :], ntasks=1)
  big_df = 0
  GC.gc(true)
end
x_df = filereader(src_jn_x, types=[Int32, Int32, Int32, Characters{6}, Characters{9}, Characters{12}, Float64]);
small_df = filereader(src_jn_y[1], types=[Int32, Characters{6}, Float64]);
medium_df = filereader(src_jn_y[2], types=[Int32, Int32, Characters{6}, Characters{9}, Float64]);
if isondisk(data_name)
  big_df = Dataset(Arrow.Table(_tmp_storage))
else
  big_df = filereader(src_jn_y[3], types=[Int32, Int32, Int32, Characters{6}, Characters{9}, Characters{12}, Float64]);
  modify!(big_df, [:id4, :id5]=>PooledArray)
end

modify!(x_df, [:id4, :id5]=>PooledArray)
modify!(small_df, :id4=>PooledArray)
modify!(medium_df, [:id4, :id5]=>PooledArray)

in_rows = size(x_df, 1);
println(in_rows); flush(stdout);
println(size(small_df, 1)); flush(stdout);
println(size(medium_df, 1)); flush(stdout);
println(size(big_df, 1)); flush(stdout);

task_init = time();
print("joining...\n"); flush(stdout);

question = "small inner on int"; # q1
GC.gc();
t = @elapsed (ANS = innerjoin(x_df, small_df, on = :id1, makeunique=true); println(size(ANS)); flush(stdout));
m = memory_usage();
chkt = @elapsed chk = [sum(ANS.v1), sum(ANS.v2)];
write_log(1, task, data_name, in_rows, question, size(ANS, 1), size(ANS, 2), solution, ver, git, fun, t, m, cache, make_chk(chk), chkt, on_disk);
ANS = 0;
GC.gc();
t = @elapsed (ANS = innerjoin(x_df, small_df, on = :id1, makeunique=true); println(size(ANS)); flush(stdout));
m = memory_usage();
chkt = @elapsed chk = [sum(ANS.v1), sum(ANS.v2)];
write_log(2, task, data_name, in_rows, question, size(ANS, 1), size(ANS, 2), solution, ver, git, fun, t, m, cache, make_chk(chk), chkt, on_disk);
println(first(ANS, 3));
println(last(ANS, 3));
ANS = 0;

question = "medium inner on int"; # q2
GC.gc();
t = @elapsed (ANS = innerjoin(x_df, medium_df, on = :id2, makeunique=true); println(size(ANS)); flush(stdout));
m = memory_usage();
chkt = @elapsed chk = [sum(ANS.v1), sum(ANS.v2)];
write_log(1, task, data_name, in_rows, question, size(ANS, 1), size(ANS, 2), solution, ver, git, fun, t, m, cache, make_chk(chk), chkt, on_disk);
ANS = 0;
GC.gc();
t = @elapsed (ANS = innerjoin(x_df, medium_df, on = :id2, makeunique=true); println(size(ANS)); flush(stdout));
m = memory_usage();
chkt = @elapsed chk = [sum(ANS.v1), sum(ANS.v2)];
write_log(2, task, data_name, in_rows, question, size(ANS, 1), size(ANS, 2), solution, ver, git, fun, t, m, cache, make_chk(chk), chkt, on_disk);
println(first(ANS, 3));
println(last(ANS, 3));
ANS = 0;

question = "medium outer on int"; # q3
GC.gc();
t = @elapsed (ANS = leftjoin(x_df, medium_df, on = :id2, makeunique=true); println(size(ANS)); flush(stdout));
m = memory_usage();
chkt = @elapsed chk = [sum(ANS.v1), sum(ANS.v2)];
write_log(1, task, data_name, in_rows, question, size(ANS, 1), size(ANS, 2), solution, ver, git, fun, t, m, cache, make_chk(chk), chkt, on_disk);
ANS = 0;
GC.gc();
t = @elapsed (ANS = leftjoin(x_df, medium_df, on = :id2, makeunique=true); println(size(ANS)); flush(stdout));
m = memory_usage();
chkt = @elapsed chk = [sum(ANS.v1), sum(ANS.v2)];
write_log(2, task, data_name, in_rows, question, size(ANS, 1), size(ANS, 2), solution, ver, git, fun, t, m, cache, make_chk(chk), chkt, on_disk);
println(first(ANS, 3));
println(last(ANS, 3));
ANS = 0;

question = "medium inner on factor"; # q4
GC.gc();
t = @elapsed (ANS = innerjoin(x_df, medium_df, on = :id5, makeunique=true); println(size(ANS)); flush(stdout));
m = memory_usage();
t_start = time_ns();
chkt = @elapsed chk = [sum(ANS.v1), sum(ANS.v2)];
write_log(1, task, data_name, in_rows, question, size(ANS, 1), size(ANS, 2), solution, ver, git, fun, t, m, cache, make_chk(chk), chkt, on_disk);
ANS = 0;
GC.gc();
t = @elapsed (ANS = innerjoin(x_df, medium_df, on = :id5, makeunique=true); println(size(ANS)); flush(stdout));
m = memory_usage();
chkt = @elapsed chk = [sum(ANS.v1), sum(ANS.v2)];
write_log(2, task, data_name, in_rows, question, size(ANS, 1), size(ANS, 2), solution, ver, git, fun, t, m, cache, make_chk(chk), chkt, on_disk);
println(first(ANS, 3));
println(last(ANS, 3));
ANS = 0;

question = "big inner on int"; # q5
GC.gc();
t = @elapsed (ANS = innerjoin(x_df, big_df, on = :id3, makeunique=true); println(size(ANS)); flush(stdout));
m = memory_usage();
chkt = @elapsed chk = [sum(ANS.v1), sum(ANS.v2)];
write_log(1, task, data_name, in_rows, question, size(ANS, 1), size(ANS, 2), solution, ver, git, fun, t, m, cache, make_chk(chk), chkt, on_disk);
ANS = 0;
GC.gc();
t = @elapsed (ANS = innerjoin(x_df, big_df, on = :id3, makeunique=true); println(size(ANS)); flush(stdout));
m = memory_usage();
chkt = @elapsed chk = [sum(ANS.v1), sum(ANS.v2)];
write_log(2, task, data_name, in_rows, question, size(ANS, 1), size(ANS, 2), solution, ver, git, fun, t, m, cache, make_chk(chk), chkt, on_disk);
println(first(ANS, 3));
println(last(ANS, 3));
ANS = 0;

print(@sprintf "joining finished, took %.0fs\n" (time()-task_init)); flush(stdout);

exit();
