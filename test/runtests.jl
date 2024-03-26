using Distributed
addprocs(3)

using Test
using Dagger
using UUIDs
import MemPool

include("util.jl")
include("fakeproc.jl")

include("thunk.jl")

#= FIXME: Unreliable, and some thunks still get retained
# N.B. We need a few of these probably because of incremental WeakRef GC
@everywhere GC.gc()
@everywhere GC.gc()
@everywhere GC.gc()
@everywhere GC.gc()
sleep(1)
@test isempty(Dagger.Sch.EAGER_ID_MAP)
state = Dagger.Sch.EAGER_STATE[]
@test isempty(state.waiting)
@test_broken length(keys(state.waiting_data)) == 1
# Ensure that all cache entries have expired
@test_broken isempty(state.cache)
=#

include("scheduler.jl")
include("processors.jl")
include("memory-spaces.jl")
include("logging.jl")
include("checkpoint.jl")
include("scopes.jl")
include("options.jl")
include("mutation.jl")
include("task-queues.jl")
include("datadeps.jl")
include("domain.jl")
include("array.jl")
include("linalg.jl")
include("cache.jl")
include("diskcaching.jl")
include("file-io.jl")

try # TODO: Fault tolerance is sometimes unreliable
#include("fault-tolerance.jl")
catch
end
println(stderr, "tests done. cleaning up...")
Dagger.cleanup()
println(stderr, "all done.")
