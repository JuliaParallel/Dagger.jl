using Distributed
addprocs(2)

using Test
using Dagger

include("fakeproc.jl")

include("domain.jl")
include("array.jl")
include("scheduler.jl")
include("processors.jl")
include("fault-tolerance.jl")
println(stderr, "tests done. cleaning up...")
Dagger.cleanup()
#include("cache.jl")
println(stderr, "all done.")
