addprocs(4)

if VERSION < v"0.5.0-dev"
    using BaseTestNext
else
    using Base.Test
end
using Dagger

include("domain.jl")
include("array.jl")
