# Coverage for targeted `Dagger.synchronize(args...)`.
#
# The `args` form used to validate its arguments and then perform the identical
# full drain -- it narrowed nothing. That is safe but useless, and worse, it is
# indistinguishable from a narrowing that silently matches nothing. So these
# tests are written to fail in *both* directions:
#
#   - "still correct": the named data really is usable afterwards. A narrowing
#     that waits for too little shows up here.
#   - "actually narrower": work the caller did *not* name is still in flight
#     afterwards. A narrowing that degenerates into a full drain shows up here,
#     which is the regression the old stub would have sailed through.
#
# The second kind needs a way to observe "still running" without racing, so the
# unnamed half of each pipeline is gated on a `Base.Event` this file controls:
# it cannot finish until the test says so, so if `synchronize(named)` returned
# while it was still blocked, the narrowing is real.

using Test
using Dagger
using Dagger: In, Out, InOut, Blocks

const SYNC_GATE = Ref{Base.Event}()
const SYNC_RAN = Threads.Atomic{Int}(0)

# Every gated region below is pinned to worker 1. `SYNC_GATE`/`SYNC_RAN` are
# ordinary globals in *this process*, so a gated task scheduled onto a worker
# would wait on an Event nobody can ever notify -- the whole suite then hangs
# rather than failing. This passed under `-p 3` by luck for a while (the gated
# task happened to land locally); adding a testset shifted the task counts and
# it stopped being lucky. Pinning is the fix rather than `@everywhere`: the
# point of the gate is that *this* task decides when the region may finish.
const LOCAL_SCOPE = Dagger.scope(worker=1)

# A processor that is *not* the one a gated task occupies.
#
# Two tests below deliberately `fetch` a plain task while the gated region task
# is still blocked, to observe whether that plain task was made to wait. That
# only means anything if a free processor exists: under `-p 3` each worker has
# a single thread, the gated task holds worker 1's, and a plain task over data
# that also lives on worker 1 gets placed there by locality -- queued behind a
# task that cannot finish until after the `fetch`. Self-inflicted deadlock, and
# it hangs the whole suite rather than failing. Under `-t 4` there were spare
# threads, so it passed and hid the problem.
const OTHER_PROC = let
    all_procs = collect(Dagger.all_processors())
    gated = first(filter(p -> Dagger.root_worker_id(p) == myid(), all_procs))
    others = filter(!=(gated), all_procs)
    isempty(others) ? nothing : first(others)
end
const GATED_SCOPE = Dagger.ExactScope(first(filter(p -> Dagger.root_worker_id(p) == myid(),
                                                   collect(Dagger.all_processors()))))

# Defined at top level (not a closure) so it survives serialization to a
# worker, and reads the gate through a `Ref` so the same function body works
# across the several regions below.
function sync_gated_add!(X)
    wait(SYNC_GATE[])
    X .+= 1
    Threads.atomic_add!(SYNC_RAN, 1)
    return
end
sync_add1!(X) = (X .+= 1; nothing)
sync_fail!(X) = error("deliberate failure")

@testset "Targeted synchronize" begin

@testset "Bare synchronize with no context is a no-op" begin
    # Precondition for everything below: a stray context from another testset
    # would make "still in flight" checks meaningless.
    @test Dagger.synchronize() === nothing
    @test Dagger.issynchronized()
end

@testset "Named data is usable, unnamed work keeps running" begin
    SYNC_GATE[] = Base.Event()
    SYNC_RAN[] = 0
    A = zeros(Int, 4)
    B = zeros(Int, 4)

    Dagger.spawn_datadeps(; sync=false) do
        Dagger.@spawn sync_add1!(InOut(A))
    end
    Dagger.with_options(;scope=GATED_SCOPE) do
        Dagger.spawn_datadeps(; sync=false) do
            Dagger.@spawn sync_gated_add!(InOut(B))
        end
    end

    # Narrow to `A`. `B`'s task is blocked on a gate this task has not
    # notified, so *returning from this call at all* is the proof that it
    # didn't wait for `B` -- a full drain here would hang until the suite
    # timed out, not fail an assertion.
    Dagger.synchronize(A)
    @test all(==(1), A)
    # And the direct observation, so a future change that makes the drain
    # non-blocking for some other reason can't quietly satisfy the above:
    # `B`'s task has not run its body. (`issynchronized()` would be a weaker
    # check -- it is false after *any* targeted call, since frees always stay
    # pending -- so it proves nothing about narrowing.)
    @test SYNC_RAN[] == 0

    notify(SYNC_GATE[])
    Dagger.synchronize()
    @test all(==(1), B)
    @test SYNC_RAN[] == 1
    @test Dagger.issynchronized()
end

@testset "Targeted synchronize waits for a full dependency chain" begin
    # Three regions chained on the same array: narrowing to `A` must wait for
    # *all* of them, not just the most recent, and not just the last writer's
    # own task.
    A = zeros(Int, 8)
    for _ in 1:3
        Dagger.spawn_datadeps(; sync=false) do
            Dagger.@spawn sync_add1!(InOut(A))
        end
    end
    Dagger.synchronize(A)
    @test all(==(3), A)
    Dagger.synchronize()
end

@testset "Targeted synchronize covers readers, not just the writer" begin
    # After `synchronize(A)` the caller may *write* `A`, so an in-flight reader
    # of any replica would be a race. Reading into `B` keeps a reader of `A`
    # outstanding at the point we narrow.
    A = ones(Int, 8)
    B = zeros(Int, 8)
    Dagger.spawn_datadeps(; sync=false) do
        Dagger.@spawn copyto!(Out(B), In(A))
    end
    Dagger.synchronize(A)
    # The reader has completed, so mutating `A` here cannot corrupt `B`.
    A .= 99
    Dagger.synchronize()
    @test all(==(1), B)
end

@testset "A DArray resolves to its chunks" begin
    # The dangerous half of the old stub: Datadeps tracks a DArray's individual
    # `.chunks`, never the DArray object, so a resolution that looked up the
    # DArray itself would find nothing and (before the fallback rule) silently
    # synchronize nothing at all.
    DA = zeros(Blocks(2, 2), Int, 4, 4)
    Dagger.spawn_datadeps(; sync=false) do
        for chunk in DA.chunks
            Dagger.@spawn sync_add1!(InOut(chunk))
        end
    end
    Dagger.synchronize(DA)
    @test all(==(1), collect(DA))
    Dagger.synchronize()
end

@testset "Untracked arguments fall back to a full drain" begin
    # Resolution failure must never be read as "nothing to do". `C` is a plain
    # array this context has never seen; naming it has to drain everything
    # rather than narrow to the empty set and return immediately.
    SYNC_GATE[] = Base.Event()
    A = zeros(Int, 4)
    C = zeros(Int, 4)
    Dagger.spawn_datadeps(; sync=false) do
        Dagger.@spawn sync_add1!(InOut(A))
    end
    notify(SYNC_GATE[])   # so the fallback's full drain can complete
    Dagger.synchronize(C)
    # Full drain: `A`'s region is done and the context is reset, even though
    # nothing named `A`.
    @test all(==(1), A)
    @test Dagger.issynchronized()
end

@testset "Targeted synchronize leaves the context alive and reusable" begin
    # A full drain resets `ddctx.state`; the targeted form must not, or the
    # next region would re-copy data it already has in place.
    A = zeros(Int, 4)
    B = zeros(Int, 4)
    Dagger.spawn_datadeps(; sync=false) do
        Dagger.@spawn sync_add1!(InOut(A))
        Dagger.@spawn sync_add1!(InOut(B))
    end
    Dagger.synchronize(A)
    @test all(==(1), A)
    # Context still live and still tracking `B`; a further region plans fine.
    Dagger.spawn_datadeps(; sync=false) do
        Dagger.@spawn sync_add1!(InOut(B))
    end
    Dagger.synchronize()
    @test all(==(2), B)
    @test Dagger.issynchronized()
end

@testset "A failure in the targeted set is reported and stays poisoned" begin
    A = zeros(Int, 4)
    Dagger.spawn_datadeps(; sync=false) do
        Dagger.@spawn sync_fail!(InOut(A))
    end
    # Reported to the caller...
    @test_throws Dagger.DataDepsRegionError Dagger.synchronize(A)
    # ...but a partial drain has not established that the pipeline is healthy,
    # so planning on top of it is still refused until a full drain clears it.
    @test_throws Dagger.DataDepsPoisonedError Dagger.spawn_datadeps(; sync=false) do
        Dagger.@spawn sync_add1!(InOut(A))
    end
    try
        Dagger.synchronize()
    catch
    end
    @test Dagger.issynchronized()
end

end # @testset "Targeted synchronize"

@testset "State-size backpressure" begin
    # `DATADEPS_STATE_LIMIT` bounds how much planning state a `sync=false`
    # pipeline may carry before a drain is forced. Measured motivation is in
    # the constant's docstring: a pipeline introducing fresh data every region
    # grows `ainfos_lookup` without bound, and planning degrades with it
    # (2.73x over 2000 regions).
    old_limit = Dagger.DATADEPS_STATE_LIMIT[]
    try
        # Deliberately tiny, so a handful of regions trips it.
        Dagger.DATADEPS_STATE_LIMIT[] = 4
        for i in 1:12
            # Fresh arrays each region: the growth case.
            A = zeros(Int, 8)
            B = zeros(Int, 8)
            Dagger.spawn_datadeps(; sync=false) do
                Dagger.@spawn sync_add1!(InOut(A))
                Dagger.@spawn sync_add1!(InOut(B))
            end
            @test all(==(1), A) || !Dagger.issynchronized()
        end
        Dagger.synchronize()
        # The point of the valve: state does not keep growing across regions.
        # After the final drain there is nothing carried over at all.
        @test Dagger.issynchronized()
    finally
        Dagger.DATADEPS_STATE_LIMIT[] = old_limit
        try; Dagger.synchronize(); catch; end
    end
end

@testset "State-size backpressure is off by default for normal regions" begin
    # A bounded working set must never trip the valve: its ainfos are created
    # once and reused, so the count plateaus (measured flat over 2000 regions).
    A = zeros(Int, 8)
    for _ in 1:20
        Dagger.spawn_datadeps(; sync=false) do
            Dagger.@spawn sync_add1!(InOut(A))
        end
    end
    Dagger.synchronize()
    @test all(==(20), A)
end

@testset "Interop: plain @spawn sees tracked data" begin
    # The documented `sync=false` hazard: a region returns before its work has
    # run or been written back, and a plain `Dagger.@spawn` downstream knows
    # nothing about it. `maybe_add_interop_deps!` closes this for plain *Dagger
    # tasks* (it cannot do anything about plain Julia code).
    #
    # Deterministic by construction rather than by luck: the region's task is
    # gated, so at the moment the plain task is submitted the region has
    # definitely not finished. A plain task with no syncdeps runs immediately
    # and reads zeros; one that was given syncdeps cannot start until the
    # write-back lands.
    @testset "interop on (default)" begin
        SYNC_GATE[] = Base.Event()
        A = zeros(Int, 8)
        Dagger.with_options(;scope=GATED_SCOPE) do
            Dagger.spawn_datadeps(; sync=false) do
                Dagger.@spawn sync_gated_add!(InOut(A))
            end
        end
        t = Dagger.@spawn sum(A)
        notify(SYNC_GATE[])
        @test fetch(t) == 8
        Dagger.synchronize()
    end

    @testset "interop off reproduces the hazard" begin
        # The control. If this ever starts passing, the test above has stopped
        # proving anything and the gate is no longer doing its job.
        if OTHER_PROC === nothing
            @test_skip "needs a second processor to observe a blocked one"
        else
            old = Dagger.DATADEPS_INTEROP[]
            try
                Dagger.DATADEPS_INTEROP[] = false
                SYNC_GATE[] = Base.Event()
                A = zeros(Int, 8)
                Dagger.with_options(;scope=GATED_SCOPE) do
                    Dagger.spawn_datadeps(; sync=false) do
                        Dagger.@spawn sync_gated_add!(InOut(A))
                    end
                end
                t = Dagger.with_options(;scope=Dagger.ExactScope(OTHER_PROC)) do
                    Dagger.@spawn sum(A)
                end
                got = fetch(t)
                notify(SYNC_GATE[])
                Dagger.synchronize()
                @test got == 0
            finally
                Dagger.DATADEPS_INTEROP[] = old
                try; Dagger.synchronize(); catch; end
            end
        end
    end

    @testset "untracked arguments are left alone" begin
        # A plain task over data no region ever touched must not acquire
        # syncdeps on unrelated in-flight work, or every `@spawn` after any
        # region becomes serialized behind it.
        if OTHER_PROC === nothing
            @test_skip "needs a second processor to observe a blocked one"
        else
            SYNC_GATE[] = Base.Event()
            A = zeros(Int, 8)
            C = fill(3, 8)
            Dagger.with_options(;scope=GATED_SCOPE) do
                Dagger.spawn_datadeps(; sync=false) do
                    Dagger.@spawn sync_gated_add!(InOut(A))
                end
            end
            # Pinned off the gated processor (see `OTHER_PROC`), so the only
            # way this can fail to complete is interop having wrongly made it
            # wait on the gated task.
            t = Dagger.with_options(;scope=Dagger.ExactScope(OTHER_PROC)) do
                Dagger.@spawn sum(C)
            end
            @test fetch(t) == 24
            notify(SYNC_GATE[])
            Dagger.synchronize()
        end
    end

    @testset "tasks inside a region are not intercepted" begin
        # Datadeps derives the full dependency structure itself; interop must
        # stay out of the way, or a copy task could end up waiting on a task
        # that is waiting on it.
        A = zeros(Int, 8)
        B = zeros(Int, 8)
        Dagger.spawn_datadeps() do
            Dagger.@spawn sync_add1!(InOut(A))
            Dagger.@spawn sync_add1!(InOut(B))
        end
        @test all(==(1), A)
        @test all(==(1), B)
    end
end
