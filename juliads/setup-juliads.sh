
# install julia
wget https://julialang-s3.julialang.org/bin/linux/x64/1.7/julia-1.7.2-linux-x86_64.tar.gz
tar -xvf julia-1.7.2-linux-x86_64.tar.gz
sudo mv julia-1.7.2 /opt
rm julia-1.7.2-linux-x86_64.tar.gz
# put to paths
echo 'export JULIA_HOME=/opt/julia-1.7.2' >> path.env
echo 'export PATH=$PATH:$JULIA_HOME/bin' >> path.env
# note that cron job must have path updated as well

source path.env

# install julia InMemoryDatasets and csv packages
julia -q -e 'using Pkg; Pkg.add(["InMemoryDatasets","DLMReader"])'
julia -q -e 'include("$(pwd())/_helpers/helpersds.jl"); pkgmeta = getpkgmeta("InMemoryDatasets"); println(string(pkgmeta["version"])); pkgmeta = getpkgmeta("DLMReader"); println(string(pkgmeta["version"]))'
