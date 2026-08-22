# MPI coverage for Phase 7a: lifting the `uniform_execution()` half of
# `spawn_datadeps`'s `sync` forcing (src/datadeps/queue.jl). `hierarchical`
# forcing is untouched and out of scope here -- these tests all pass
# `hierarchical=false` explicitly.
#
# What this suite is checking, specifically:
#   - Two consecutive `sync=false` regions on the flat (non-hierarchical) path
#     genuinely defer their epilogue under MPI, and a single trailing
#     `Dagger.synchronize()` still produces the right answer -- i.e. the
#     deferred write-back/free/tag-allocation sequence is rank-uniform even
#     though it now runs well after the region that queued it returned.
#   - A second Julia Task racing to plan a region while another is already
#     planning gets a clear `DataDepsConcurrentPlanningError`, not a hang --
#     see `Dagger.with_datadeps_planning_token`/`Dagger.DATADEPS_PLANNING_TOKEN`
#     (src/datadeps/context.jl).
#
# `Dagger.check_uniformity!(true)` is on throughout (see below): under
# deferral a tag/MPIRefID desync would otherwise be silent right up until it
# deadlocks, since the cross-rank compare that would catch it is normally off
# by default.
#
# Run: mpiexec -n 4 julia --project test/mpi_async.jl
# (or via test/run_mpi.jl, see test/mpi.jl's header)

using Dagger, MPI, Random, Test
using Dagger: In, Out, InOut

# This suite only means something against the checked-out dev tree -- a stale
# registered Dagger would silently "pass" by exercising old, still-forced-sync
# behavior instead of the code under test.
let repo_root = realpath(joinpath(@__DIR__, ".."))
    @assert startswith(realpath(pathof(Dagger)), repo_root) "pathof(Dagger) = $(pathof(Dagger)) does not resolve to this checkout ($repo_root)"
end

const MPIExt = Base.get_extension(Dagger, :MPIExt)

Dagger.accelerate!(:mpi)
Dagger.check_uniformity!(true)
const comm = MPI.COMM_WORLD
const rank = MPI.Comm_rank(comm)
const nranks = MPI.Comm_size(comm)

mpi_procs() = sort(collect(Dagger.get_processors(MPIExt.MPIClusterProc(comm)));
                   by=p->(p.rank, Dagger.short_name(p)))
proc_for_rank(r) = first(filter(p->p.rank == r, mpi_procs()))
rank_scope(r) = Dagger.ExactScope(proc_for_rank(r))

add1!(X) = (X .+= 1; nothing)
scale2!(X) = (X .*= 2; nothing)

@testset "MPI Async (Phase 7a)" begin

@testset "Two deferred regions pipeline correctly" begin
    # Cross-rank so the elided round-trip through origin is a genuine
    # inter-rank transfer, not bookkeeping -- see PLAN.md's Phase 7a
    # rationale. r1 writes A, then (without any intervening synchronize) r2
    # reads-and-writes A: region 2's InOut(A) must be satisfied from
    # wherever region 1's `DataDepsState` says A now lives (r1's slot), not
    # by round-tripping back through the origin rank first.
    Random.seed!(4200 + rank)
    A = rand(8, 8)
    ref = copy(A)

    r1 = min(1, nranks-1)
    r2 = min(2, nranks-1)

    @test Dagger.issynchronized()

    Dagger.spawn_datadeps(hierarchical=false, sync=false) do
        Dagger.@spawn scope=rank_scope(r1) add1!(InOut(A))
    end
    # Still deferred: nothing has been waited on, written back, or freed yet.
    @test !Dagger.issynchronized()

    Dagger.spawn_datadeps(hierarchical=false, sync=false) do
        Dagger.@spawn scope=rank_scope(r2) scale2!(InOut(A))
    end
    @test !Dagger.issynchronized()

    Dagger.synchronize()
    @test Dagger.issynchronized()

    ref .+= 1
    ref .*= 2
    # Rank 0 owns the original (non-Chunk) argument and receives the
    # deferred write-back once `synchronize()` actually flushes it.
    if rank == 0
        @test A ≈ ref
    end
end

@testset "Interleaved plain @spawn stays correct across a deferred boundary" begin
    # A third deferred region depending on the result of the first two,
    # exercising a slightly longer pipeline (three regions, one drain).
    Random.seed!(4300 + rank)
    B = rand(6, 6)
    ref = copy(B)
    r1 = min(1, nranks-1)
    r2 = min(2, nranks-1)

    Dagger.spawn_datadeps(hierarchical=false, sync=false) do
        Dagger.@spawn scope=rank_scope(r1) add1!(InOut(B))
    end
    Dagger.spawn_datadeps(hierarchical=false, sync=false) do
        Dagger.@spawn scope=rank_scope(r2) add1!(InOut(B))
    end
    Dagger.spawn_datadeps(hierarchical=false, sync=false) do
        Dagger.@spawn scope=rank_scope(0) scale2!(InOut(B))
    end
    Dagger.synchronize()

    ref .+= 1; ref .+= 1; ref .*= 2
    if rank == 0
        @test B ≈ ref
    end
end

@testset "Concurrent planners error instead of deadlocking" begin
    # Simulate a second Task holding the planning token from underneath a
    # planner: a genuinely different Task must actually own the lock for
    # this to test anything (a `ReentrantLock` re-entered by the *same* Task
    # would silently succeed). A `Channel` handshake avoids a timing-based
    # race between "holder acquired" and "we attempt to plan".
    ready = Channel{Nothing}(1)
    release = Channel{Nothing}(1)
    holder = Threads.@spawn begin
        lock(Dagger.DATADEPS_PLANNING_TOKEN)
        put!(ready, nothing)
        take!(release)
        unlock(Dagger.DATADEPS_PLANNING_TOKEN)
    end
    take!(ready)
    try
        @test_throws Dagger.DataDepsConcurrentPlanningError begin
            Dagger.spawn_datadeps(hierarchical=false) do
                Dagger.@spawn 1+1
            end
        end
    finally
        put!(release, nothing)
        wait(holder)
    end

    # The token is released again and the system is fully usable afterwards
    # (this also exercises hierarchical=false + sync=true, the default under
    # MPI once `hierarchical` is turned off explicitly).
    C = ones(4, 4)
    Dagger.spawn_datadeps(hierarchical=false) do
        Dagger.@spawn scope=rank_scope(min(1, nranks-1)) add1!(InOut(C))
    end
    if rank == 0
        @test C == fill(2.0, 4, 4)
    end
end

end # @testset "MPI Async (Phase 7a)"

MPI.Barrier(comm)
Core.println("[$rank] MPI async suite OK")
