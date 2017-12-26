addprocs(2)

using Base.Test
using Dagger

include("domain.jl")
include("array.jl")
Dagger.cleanup()
#include("cache.jl")
