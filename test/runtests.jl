addprocs(4)

if VERSION < v"0.5.0-dev"
    using BaseTestNext
else
    using Base.Test
end
using Requires
try
    Pkg.installed("ArrayFire")
    using ArrayFire
    using ComputeFramework
    include("gpu.jl")
catch 
    info("ArrayFire isn't installed. Skipping GPU tests")
end
using ComputeFramework

include("domain.jl")
include("array.jl")
