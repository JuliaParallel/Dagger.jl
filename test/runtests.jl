using Distributed
addprocs(3)

using Test
using Dagger
using UUIDs

#function test_set_eager_options()
#    Dagger.Sch.eager_options(; single=1)
#    eager_ctxt1 = @spawn _get_sch_ctxt(3)
#    Dagger.Sch.eager_options(; single=0)
#    sleep(1)
#    eager_ctxt0 = @spawn _get_sch_ctxt(1)
#    @test fetch(eager_ctxt1).options.single == 1
#    @test fetch(eager_ctxt0).options.single == 0
#end
#@everywhere using Dagger
#@everywhere _get_sch_ctxt(seconds) = (sleep(seconds); Dagger.Sch.eager_context())
#@testset "Set eager options once" begin
#    test_set_eager_options()
#end

####
#### uncomment above, get errors below
####

##Set eager options once: Error During Test at /home/ubuntu/projects/Dagger.jl/test/runtests.jl:18
##  Test threw exception
##  Expression: (fetch(eager_ctxt1)).options.single == 1
##  type Nothing has no field options
##  Stacktrace:
##   [1] getproperty(x::Nothing, f::Symbol)
##     @ Base ./Base.jl:33
##   [2] test_set_eager_options()
##     @ Main ~/projects/Dagger.jl/test/runtests.jl:18
##   [3] macro expansion
##     @ ~/projects/Dagger.jl/test/runtests.jl:23 [inlined]
##   [4] macro expansion
##     @ /buildworker/worker/package_linux64/build/usr/share/julia/stdlib/v1.6/Test/src/Test.jl:1151 [inlined]
##   [5] top-level scope
##     @ ~/projects/Dagger.jl/test/runtests.jl:23
##Set eager options once: Error During Test at /home/ubuntu/projects/Dagger.jl/test/runtests.jl:19
##  Test threw exception
##  Expression: (fetch(eager_ctxt0)).options.single == 0
##  type Nothing has no field options
##  Stacktrace:
##   [1] getproperty(x::Nothing, f::Symbol)
##     @ Base ./Base.jl:33
##   [2] test_set_eager_options()
##     @ Main ~/projects/Dagger.jl/test/runtests.jl:19
##   [3] macro expansion
##     @ ~/projects/Dagger.jl/test/runtests.jl:23 [inlined]
##   [4] macro expansion
##     @ /buildworker/worker/package_linux64/build/usr/share/julia/stdlib/v1.6/Test/src/Test.jl:1151 [inlined]
##   [5] top-level scope
##     @ ~/projects/Dagger.jl/test/runtests.jl:23
##Test Summary:          | Error  Total
##Set eager options once |     2      2

include("util.jl")

include("fakeproc.jl")
include("thunk.jl")

function test_set_eager_options()
    Dagger.Sch.eager_options(; single=1)
    eager_ctxt1 = @spawn _get_sch_ctxt(3)
    Dagger.Sch.eager_options(; single=0)
    sleep(1)
    eager_ctxt0 = @spawn _get_sch_ctxt(1)
    @test fetch(eager_ctxt1).options.single == 1
    @test fetch(eager_ctxt0).options.single == 0
end
@everywhere using Dagger
@everywhere _get_sch_ctxt(seconds) = (sleep(seconds); Dagger.Sch.eager_context())
@testset "Set eager options once again" begin
    test_set_eager_options()
end
#Set eager options once again: Test Failed at /home/ubuntu/projects/Dagger.jl/test/runtests.jl:68
#  Expression: (fetch(eager_ctxt1)).options.single == 1
#   Evaluated: 0 == 1
#Stacktrace:
# [1] test_set_eager_options()
#   @ Main ~/projects/Dagger.jl/test/runtests.jl:68
# [2] macro expansion
#   @ ~/projects/Dagger.jl/test/runtests.jl:75 [inlined]
# [3] macro expansion
#   @ /buildworker/worker/package_linux64/build/usr/share/julia/stdlib/v1.6/Test/src/Test.jl:1151 [inlined]
# [4] top-level scope
#   @ ~/projects/Dagger.jl/test/runtests.jl:75
#Set eager options once again: Error During Test at /home/ubuntu/projects/Dagger.jl/test/runtests.jl:69
#  Test threw exception
#  Expression: (fetch(eager_ctxt0)).options.single == 0
#  type Nothing has no field options
#  Stacktrace:
#   [1] getproperty(x::Nothing, f::Symbol)
#     @ Base ./Base.jl:33
#   [2] test_set_eager_options()
#     @ Main ~/projects/Dagger.jl/test/runtests.jl:69
#   [3] macro expansion
#     @ ~/projects/Dagger.jl/test/runtests.jl:75 [inlined]
#   [4] macro expansion
#     @ /buildworker/worker/package_linux64/build/usr/share/julia/stdlib/v1.6/Test/src/Test.jl:1151 [inlined]
#   [5] top-level scope
#     @ ~/projects/Dagger.jl/test/runtests.jl:75
#Test Summary:                | Fail  Error  Total
#Set eager options once again |    1      1      2



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

#include("scheduler.jl")
#include("processors.jl")
#include("ui.jl")
#include("checkpoint.jl")
#include("scopes.jl")
#include("domain.jl")
#include("array.jl")
#include("cache.jl")
#include("table.jl")
#try # TODO: Fault tolerance is sometimes unreliable
#include("fault-tolerance.jl")
#catch
#end
#println(stderr, "tests done. cleaning up...")
#Dagger.cleanup()
#println(stderr, "all done.")
