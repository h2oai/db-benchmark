#!/usr/bin/env julia

print("# groupby-julia\n");

#include("./helpers.jl");

using DataFrames;
using CSV;
#using FastGroupBy; # raise error, not recommended for string anyway

x = CSV.read("db-benchmark/G1_1e6_1e2.csv", categorical=false);

# https://github.com/xiaodaigh/DataBench.jl/blob/master/benchmark/benchmark_groupby_vs_r_2_benchmark.jl
