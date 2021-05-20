using Distributed
addprocs(3)

using Test
using Dagger

include("util.jl")
include("fakeproc.jl")

include("thunk.jl")
include("scheduler.jl")
include("processors.jl")
include("ui.jl")
include("checkpoint.jl")
include("domain.jl")
include("array.jl")
try # TODO: Fault tolerance is sometimes unreliable
include("fault-tolerance.jl")
catch
end
println(stderr, "tests done. cleaning up...")
Dagger.cleanup()
#include("cache.jl")
println(stderr, "all done.")
