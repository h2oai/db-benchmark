#!/usr/bin/env julia

# ./../julia-1.0.0/bin/julia

print("# groupby-julia\n");

#include("./helpers.jl");

using DataFrames;
using CSV;
#using FastGroupBy; # raise error, not recommended for string anyway

x = CSV.read("G1_1e6_1e2.csv", categorical=false);

# https://github.com/xiaodaigh/DataBench.jl/blob/master/benchmark/benchmark_groupby_vs_r_2_benchmark.jl

# 1
t_start = time_ns();
ans = fastby(sum, x[:id1], x[:v1]);
t = (time_ns() - t_start)/1.0e9;

# 2
fn, byveccv, val = +, (x[:id1], x[:id2]), x[:v1]
fgroupreduce(+, (x[:id1], x[:id2]), x[:v1])

# 3
fastby((sum, mean), x[:id3], (x[:v1], x[:v3]))

# 4
fastby((mean, mean, mean), x[:id4], (x[:v1], x[:v2], x[:v3]));

# 5
fastby((sum, sum, sum), x[:id6], (x[:v1], x[:v2], x[:v3]));
