addprocs(4)

using ComputeFramework

using BaseTestNext
@everywhere const Test = BaseTestNext

include("util.jl")
include("distribute.jl")
include("redistribute.jl")
include("show.jl")
