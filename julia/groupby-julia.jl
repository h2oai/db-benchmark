#!/usr/bin/env julia

print("# groupby-julia\n");

using DataFrames;
using CSV;

include("./../helpers.jl");

ver = "";
git = "";
task = "groupby";
solution = "julia";
fun = "";
cache = true;

print("loading dataset...\n");
src_grp = ENV["SRC_GRP_LOCAL"];
data_name = basename(src_grp);

x = CSV.read(data_name, categorical=false);
in_rows = size(x, 1);
println(in_rows);

print("grouping...\n");

question = "sum v1 by id1"; #1
GC.gc();
t_start = time_ns();
ANS = aggregate(x[[:id1, :v1]], :id1, sum);
println(size(ANS));
t = (time_ns() - t_start)/1.0e9;
m = memory_usage();
t_start = time_ns();
chk = sum(ANS.v1_sum);
chkt = (time_ns() - t_start)/1.0e9;
write_log(1, task, data_name, in_rows, question, size(ANS, 1), size(ANS, 2), solution, ver, git, fun, t, m, cache, make_chk(chk), chkt);
ANS = 0;

# 2
#fn, byveccv, val = +, (x[:id1], x[:id2]), x[:v1]
#fgroupreduce(+, (x[:id1], x[:id2]), x[:v1])

# 3
#fastby((sum, mean), x[:id3], (x[:v1], x[:v3]))

# 4
#fastby((mean, mean, mean), x[:id4], (x[:v1], x[:v2], x[:v3]));

# 5
#fastby((sum, sum, sum), x[:id6], (x[:v1], x[:v2], x[:v3]));

exit();
