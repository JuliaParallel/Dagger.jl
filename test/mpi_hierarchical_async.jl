# MPI coverage for Phase 7b: lifting the `hierarchical` half of
# `spawn_datadeps`'s `sync` forcing (src/datadeps/queue.jl), so consecutive
# `sync=false` regions pipeline under `hierarchical=true` -- the *default* --
# under MPI. test/mpi_async.jl already covers the `hierarchical=false` (flat)
# path from Phase 7a; this file is its hierarchical counterpart and never
# passes `hierarchical=false` anywhere.
#
# Under `uniform_execution()` (always true here), `distribute_tasks_hierarchical!`
# always takes the shared-state scheduling strategy
# (`schedule_partitions_sequential!`) -- see the carry-in/publish-back N.B. in
# `_distribute_tasks_hierarchical!` (src/datadeps/hierarchical.jl) -- so this
# suite exercises exactly the code path that was taught to plan directly
# against `ddctx.state`/`ddctx.write_num` and defer its write-back/free
# instead of reconciling per-partition states and copying-from-and-freeing
# immediately every region.
#
# `Dagger.check_uniformity!(true)` is on throughout, same reasoning as
# mpi_async.jl: under deferral a tag/MPIRefID desync would otherwise stay
# silent right up until it deadlocks.
#
# Run: mpiexec -n 4 julia --project test/mpi_hierarchical_async.jl
# (or via test/run_mpi.jl, see test/mpi.jl's header)

using Dagger, MPI, Random, Test
using Dagger: In, Out, InOut

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

@testset "MPI Hierarchical Async (Phase 7b)" begin

@testset "Two deferred regions pipeline correctly (hierarchical=true, the default)" begin
    Random.seed!(5200 + rank)
    A = rand(8, 8)
    ref = copy(A)

    r1 = min(1, nranks-1)
    r2 = min(2, nranks-1)

    @test Dagger.issynchronized()

    # `hierarchical` is left at its default (true) -- this is the whole point
    # of the suite: nothing here opts out of the default planner.
    Dagger.spawn_datadeps(sync=false) do
        Dagger.@spawn scope=rank_scope(r1) add1!(InOut(A))
    end
    # Still deferred: the write-back to rank 0's origin hasn't run yet. If
    # `hierarchical=true` still silently coerced `sync=true` (the pre-Phase-7b
    # behavior), or if carry-in/publish-back merely stubbed through without
    # actually deferring, rank 0's `A` would already reflect the +1 here.
    @test !Dagger.issynchronized()
    if rank == 0
        @test A ≈ ref
    end

    Dagger.spawn_datadeps(sync=false) do
        Dagger.@spawn scope=rank_scope(r2) scale2!(InOut(A))
    end
    @test !Dagger.issynchronized()
    if rank == 0
        @test A ≈ ref
    end

    Dagger.synchronize()
    @test Dagger.issynchronized()

    ref .+= 1
    ref .*= 2
    if rank == 0
        @test A ≈ ref
    end
end

@testset "Three-region pipeline, single drain" begin
    Random.seed!(5300 + rank)
    B = rand(6, 6)
    ref = copy(B)
    r1 = min(1, nranks-1)
    r2 = min(2, nranks-1)

    Dagger.spawn_datadeps(sync=false) do
        Dagger.@spawn scope=rank_scope(r1) add1!(InOut(B))
    end
    Dagger.spawn_datadeps(sync=false) do
        Dagger.@spawn scope=rank_scope(r2) add1!(InOut(B))
    end
    # Final write lands back on rank 0 itself, exercising write-back elision
    # against the carried-in state's bookkeeping rather than a fresh one.
    Dagger.spawn_datadeps(sync=false) do
        Dagger.@spawn scope=rank_scope(0) scale2!(InOut(B))
    end
    @test !Dagger.issynchronized()
    Dagger.synchronize()
    @test Dagger.issynchronized()

    ref .+= 1; ref .+= 1; ref .*= 2
    if rank == 0
        @test B ≈ ref
    end
end

@testset "Concurrent planners error instead of deadlocking (hierarchical=true)" begin
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
            Dagger.spawn_datadeps() do
                Dagger.@spawn 1+1
            end
        end
    finally
        put!(release, nothing)
        wait(holder)
    end

    C = ones(4, 4)
    Dagger.spawn_datadeps() do
        Dagger.@spawn scope=rank_scope(min(1, nranks-1)) add1!(InOut(C))
    end
    if rank == 0
        @test C == fill(2.0, 4, 4)
    end
end

end # @testset "MPI Hierarchical Async (Phase 7b)"

MPI.Barrier(comm)
Core.println("[$rank] MPI hierarchical async suite OK")
