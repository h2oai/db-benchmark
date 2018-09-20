cd ..
wget https://julialang-s3.julialang.org/bin/linux/x64/1.0/julia-1.0.0-linux-x86_64.tar.gz
tar -xvf julia-1.0.0-linux-x86_64.tar.gz
cd db-benchmark
../julia-1.0.0/bin/julia

using Pkg;
Pkg.add("DataFrames");
Pkg.add("CSV");
#Pkg.add("FastGroupBy"); # using raise error, not recommended for string anyway

exit();
