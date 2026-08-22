# Non-MPI coverage for Phase 7b: lifting `spawn_datadeps`'s `hierarchical`-half
# of the `sync` forcing (src/datadeps/queue.jl), so consecutive `sync=false`
# regions pipeline under `hierarchical=true` -- the *default* -- and not just
# under the explicit `hierarchical=false` opt-out that test/mpi_async.jl and
# test/array/fft_async.jl cover.
#
# Two scheduling strategies live inside `distribute_tasks_hierarchical!`
# (see `_distribute_tasks_hierarchical!`'s carry-in/publish-back N.B.,
# hierarchical.jl), and this file exercises both:
#
#   - Shared-state (`schedule_partitions_sequential!`): taken whenever
#     processors span more than one memory space -- i.e. any multi-process
#     (`-p`) run, which is why the first testset requires `-p 3`. This is the
#     branch that was taught carry-in/publish-back: it now plans directly
#     against the task's persistent `DataDepsContext.state`/`write_num`
#     instead of a fresh, region-scoped `DataDepsState`, and its epilogue
#     defers write-back/free instead of running them immediately. The `A ==
#     ref` checks below are the "did this silently fall back to synchronous"
#     guard the plan calls for: if `hierarchical=true` still coerced
#     `sync=true` (or if carry-in/publish-back were merely appended rather
#     than actually wired in), the deferred write-back would already have run
#     by the time we check, and these would spuriously pass with `A`
#     up-to-date instead of proving it wasn't.
#   - Parallel per-partition (`schedule_partition_full!`): taken only for a
#     single memory space (needs `-t >= 4`, `-p 0`, and enough tasks to
#     partition -- see `HIER_TASKS_PER_PARTITION`). This strategy does *not*
#     participate in cross-region persistence (see the same N.B. for why);
#     it force-drains around itself instead, so the second testset only
#     checks that chaining `sync=false` regions through it still produces the
#     right answer -- not that it deferred anything, since by design it
#     doesn't.

using Distributed, Random, Test
using Dagger: In, Out, InOut

# Must be defined on every worker (`@everywhere`), not just the process
# running this file: the compute tasks below are scoped onto other processes,
# and a plain top-level definition is only ever serialized as a closure over
# an unresolvable `Main` binding for those.
@everywhere hier_async_add1!(X) = (X .+= 1; nothing)
@everywhere hier_async_scale2!(X) = (X .*= 2; nothing)

@testset "Hierarchical Async (Phase 7b)" begin

@testset "Shared-state path: two deferred regions pipeline correctly" begin
    if nprocs() < 4
        @test_skip "Needs 4 processes (-p 3) for the shared-state hierarchical path"
    else
        all_procs = [only(Dagger.get_processors(Dagger.OSProc(pid))) for pid in 1:4]
        rank_scope(i) = Dagger.ExactScope(all_procs[i])

        Random.seed!(6100)
        A = rand(8, 8)
        ref = copy(A)

        @test Dagger.issynchronized()

        # A is created here (process 1), so this is its origin. Both compute
        # tasks run on *other* processes, so the deferred write-back this
        # region records is a genuine cross-process copy, not a no-op.
        Dagger.spawn_datadeps(hierarchical=true, sync=false) do
            Dagger.@spawn scope=rank_scope(2) hier_async_add1!(InOut(A))
        end
        # Still deferred: nothing has been waited on, written back, or freed.
        # A silent fallback to sync=true would already show `A == ref .+ 1`
        # here -- this is the negative check that rules that out.
        @test !Dagger.issynchronized()
        @test A == ref

        Dagger.spawn_datadeps(hierarchical=true, sync=false) do
            Dagger.@spawn scope=rank_scope(3) hier_async_scale2!(InOut(A))
        end
        @test !Dagger.issynchronized()
        @test A == ref

        Dagger.synchronize()
        @test Dagger.issynchronized()

        ref .+= 1
        ref .*= 2
        @test A ≈ ref
    end
end

@testset "Shared-state path: three-region pipeline including a same-space write" begin
    if nprocs() < 4
        @test_skip "Needs 4 processes (-p 3) for the shared-state hierarchical path"
    else
        all_procs = [only(Dagger.get_processors(Dagger.OSProc(pid))) for pid in 1:4]
        rank_scope(i) = Dagger.ExactScope(all_procs[i])

        Random.seed!(6200)
        B = rand(6, 6)
        ref = copy(B)

        Dagger.spawn_datadeps(hierarchical=true, sync=false) do
            Dagger.@spawn scope=rank_scope(2) hier_async_add1!(InOut(B))
        end
        Dagger.spawn_datadeps(hierarchical=true, sync=false) do
            Dagger.@spawn scope=rank_scope(3) hier_async_add1!(InOut(B))
        end
        # Final write happens back on the origin process itself: exercises the
        # write-back elision path (`origin_space in arg_current[arg_w]` should
        # end up false here, since the *previous* region's writer left the
        # data on process 3, not process 1 -- so this must still be a real
        # copy, just planned against the persisted state's bookkeeping).
        Dagger.spawn_datadeps(hierarchical=true, sync=false) do
            Dagger.@spawn scope=rank_scope(1) hier_async_scale2!(InOut(B))
        end
        @test !Dagger.issynchronized()
        Dagger.synchronize()
        @test Dagger.issynchronized()

        ref .+= 1; ref .+= 1; ref .*= 2
        @test B ≈ ref
    end
end

@testset "Single-memory-space (parallel per-partition) path stays correct" begin
    # This strategy (`schedule_partition_full!`) only runs for a single memory
    # space with enough tasks to partition (`HIER_TASKS_PER_PARTITION == 16`,
    # capped at half the processor count) -- needs a single process and
    # several threads, so it's skipped under the standard `-p 3 -t 1` suite
    # invocation and must be checked separately (e.g. `-p 0 -t 4`).
    if nprocs() != 1 || Threads.nthreads() < 4
        @test_skip "Needs a single process with >=4 threads for the single-space partitioned path"
    else
        Random.seed!(6300)
        n = 64
        xs = [fill(Float64(i), 4, 4) for i in 1:n]
        ref = [copy(x) for x in xs]

        # By design this strategy does not defer across regions (it force-
        # drains before and after itself -- see the carry-in/publish-back
        # N.B.), so this only checks that chaining `sync=false` regions
        # through it still yields the right answer, not that anything was
        # actually deferred.
        Dagger.spawn_datadeps(hierarchical=true, sync=false) do
            for x in xs
                Dagger.@spawn hier_async_add1!(InOut(x))
            end
        end
        Dagger.spawn_datadeps(hierarchical=true, sync=false) do
            for x in xs
                Dagger.@spawn hier_async_scale2!(InOut(x))
            end
        end
        Dagger.synchronize()
        @test Dagger.issynchronized()

        for i in 1:n
            @test xs[i] ≈ (ref[i] .+ 1) .* 2
        end
    end
end

end # @testset "Hierarchical Async (Phase 7b)"
