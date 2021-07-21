
# install julia
wget https://julialang-s3.julialang.org/bin/linux/x64/1.6/julia-1.6.2-linux-x86_64.tar.gz
tar -xvf julia-1.6.2-linux-x86_64.tar.gz
sudo mv julia-1.6.2 /opt
rm julia-1.6.2-linux-x86_64.tar.gz
# put to paths
echo 'export JULIA_HOME=/opt/julia-1.6.2' >> path.env
echo 'export PATH=$PATH:$JULIA_HOME/bin' >> path.env
# note that cron job must have path updated as well

source path.env

# install julia dataframes and csv packages
julia -q -e 'using Pkg; Pkg.add(["DataFrames","CSV"])'
julia -q -e 'include("$(pwd())/_helpers/helpers.jl"); pkgmeta = getpkgmeta("DataFrames"); println(string(pkgmeta["version"])); pkgmeta = getpkgmeta("CSV"); println(string(pkgmeta["version"]))'
