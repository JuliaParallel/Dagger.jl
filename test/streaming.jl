const ACCUMULATOR = Dict{Int,Vector{Real}}()
const ACCUMULATOR_LOCK = ReentrantLock()
@everywhere function accumulator(x=0)
    tid = Dagger.task_id()
    remotecall_wait(1, tid, x) do tid, x
        lock(ACCUMULATOR_LOCK) do
            acc = get!(Vector{Real}, ACCUMULATOR, tid)
            push!(acc, x)
        end
    end
    return
end
function take_accumulator!()
    lock(ACCUMULATOR_LOCK) do
        values = copy(ACCUMULATOR)
        empty!(ACCUMULATOR)
        return values
    end
end
@everywhere accumulator(xs...) = accumulator(sum(xs))
@everywhere accumulator(::Nothing) = accumulator(0)

function catch_interrupt(f)
    try
        f()
    catch err
        if err isa Dagger.DTaskFailedException && err.ex isa InterruptException
            return
        elseif err isa Dagger.Sch.SchedulingException
            return
        end
        rethrow()
    end
end

function merge_testset!(inner::Test.DefaultTestSet)
    outer = Test.get_testset()
    append!(outer.results, inner.results)
    @static if VERSION >= v"1.13-"
        @atomic outer.n_passed += inner.n_passed
    else
        outer.n_passed += inner.n_passed
    end
end

# `ignore_timeout=true` is used for tests that are *supposed* to run forever and
# be stopped by the timeout, so those want a short budget. Tests that are
# expected to finish only use the timeout as a hang detector; there a tight
# budget is fragile, because the first cold run of a given streaming topology
# pays for compilation, which can blow past 10s on a slow/loaded CI runner even
# though the work itself completes in well under a second. Give finishing tests
# plenty of headroom so cold-compile latency isn't misreported as a hang.
# Report why a streaming test hung, while the evidence still exists.
#
# The two questions worth separating are "did the task never get scheduled?"
# and "did it start and then block?", which `running`/`running_on` answer
# directly, and "is some drain task dead or missing?", which the errormonitor
# registry answers by name (`streaming input:`/`streaming output:` entries are
# registered per stream edge). Everything is wrapped defensively: a diagnostic
# that throws must never turn a reported failure into an unreported one.
function dump_streaming_state()
    io = IOBuffer()
    println(io, "streaming hang diagnostics")
    try
        state = Dagger.Sch.EAGER_STATE[]
        if state === nothing
            println(io, "  scheduler: <no eager state>")
        else
            println(io, "  scheduler: running_count=", state.running_count[])
            # NEVER block on the scheduler lock here. The case this runs in is
            # "something is wedged", and if what's wedged is holding this lock,
            # waiting for it converts a reported failure into a silent hang.
            # Take it opportunistically and say so if we couldn't.
            got_lock = false
            for _ in 1:50
                got_lock = trylock(state.lock)
                got_lock && break
                sleep(0.1)
            end
            if !got_lock
                println(io, "  live thunks: <scheduler lock held for >5s -- ",
                            "it is probably the wedged party>")
            else
                try
                    println(io, "  live thunks: ", length(state.strong_thunks))
                    for th in state.strong_thunks
                        running = @atomic th.running
                        finished = @atomic th.finished
                        errored = @atomic th.errored
                        pending = @atomic th.pending_deps
                        println(io, "    thunk ", th.id,
                                ": running=", running,
                                " finished=", finished,
                                " errored=", errored,
                                " pending_deps=", pending,
                                " on=", something(th.running_on, "<unscheduled>"))
                    end
                finally
                    unlock(state.lock)
                end
            end
        end
    catch err
        println(io, "  <scheduler dump failed: ", sprint(showerror, err), ">")
    end
    try
        lock(Dagger.Sch.ERRORMONITOR_TRACKED) do tracked
            streamers = filter(p->startswith(first(p), "streaming "), tracked)
            println(io, "  streaming drain tasks: ", length(streamers))
            for (name, task) in streamers
                println(io, "    ", name,
                        ": started=", istaskstarted(task),
                        " done=", istaskdone(task),
                        " failed=", istaskfailed(task),
                        " sticky=", task.sticky,
                        " tid=", Threads.threadid(task),
                        " pool=", Threads.threadpool(task),
                        " queued=", task.queue !== nothing)
            end
        end
    catch err
        println(io, "  <drain-task dump failed: ", sprint(showerror, err), ">")
    end
    println(io, "  threads: default=", Threads.threadpoolsize(:default),
            " interactive=", Threads.threadpoolsize(:interactive),
            " cpus=", Sys.CPU_THREADS)
    @warn String(take!(io))
    # The state above says *which* tasks are wedged but not *where*. Several
    # drain tasks are reported as never even started, which no amount of
    # buffer/thunk state explains -- the question is what every OS thread is
    # actually doing at that moment. This prints a backtrace for every live task
    # on every thread (the same dump `SIGINFO` produces, but it also works on
    # Windows, which is the only platform that reproduces this).
    try
        flush(stderr)
        ccall(:jl_print_task_backtraces, Cvoid, (Cint,), 0)
        flush(stderr)
    catch err
        @warn "task backtrace dump failed" exception=err
    end
    return
end

function test_finishes(f, message::String; ignore_timeout=false, timeout=(ignore_timeout ? 10 : 120), max_evals=10)
    t = @eval Threads.@spawn begin
        tset = nothing
        try
            @testset $message begin
                try
                    @testset $message begin
                        Dagger.with_options(;stream_max_evals=$max_evals) do
                            catch_interrupt($f)
                        end
                    end
                finally
                    tset = Test.get_testset()
                end
            end
        catch
        end
        return tset
    end

    timed_out = timedwait(()->istaskdone(t), timeout) == :timed_out
    if timed_out
        if !ignore_timeout
            @warn "Testing task timed out: $message"
            # Dump the scheduler and drain-task state *before* the cancellation
            # below tears it down. A streaming hang otherwise leaves nothing to
            # go on: the exception objects printed by the failing `fetch`es are
            # rendered after teardown has already emptied the stores, so they
            # describe the corpse rather than the crime.
            dump_streaming_state()
        end
        # Cancel in a loop rather than once. The budget can expire while the
        # testing task is still *inside* its first `Dagger.@spawn`: on a cold,
        # coverage-instrumented CI runner, compiling the streaming submission
        # path alone takes tens of seconds, well past the 10s `ignore_timeout`
        # budget. A single `cancel!` then finds nothing to cancel and halts the
        # scheduler; the task the body goes on to submit afterwards lands in the
        # *replacement* scheduler with nobody left to stop it, runs forever, and
        # the `fetch(t)` below hangs the entire testsuite. Keep cancelling until
        # the testing task has actually finished.
        for attempt in 1:30
            Dagger.cancel!(;halt_sch=true, graceful=false)
            @everywhere GC.gc()
            fetch(Dagger.@spawn 1+1)
            timedwait(()->istaskdone(t), 10) == :timed_out || break
            if attempt == 30
                @warn "Testing task did not stop after $attempt cancellations: $message"
            end
        end
    end

    if !istaskdone(t)
        # Nothing above could stop it. `fetch` would block the whole testsuite
        # here with no output at all; fail loudly instead.
        error("Testing task could not be stopped: $message")
    end
    tset = fetch(t)::Test.DefaultTestSet
    merge_testset!(tset)
    return !timed_out
end

# `ProcessRingBuffer` polls `task_may_cancel!` while spinning, which needs DTask
# TLS. These tasks aren't DTasks, so hand them a never-cancelled token. Run the
# body on its own task so the fake TLS stays task-local and can't leak into the
# rest of the suite.
function in_fake_task(f)
    return Threads.@spawn begin
        Dagger.set_tls!((; processor = Dagger.ThreadProc(1, Threads.threadid()),
                           sch_uid = UInt(0),
                           sch_handle = nothing,
                           task_spec = nothing,
                           cancel_token = Dagger.CancelToken(),
                           logging_enabled = false,
                           acceleration = Dagger.DistributedAcceleration()))
        f()
    end
end

@testset "ProcessRingBuffer" begin
    @testset "SPSC ordering" begin
        # A full buffer is the dangerous case: the slot the producer writes
        # next is the one the consumer is reading. If either side publishes its
        # index movement before it is done with the slot, values are silently
        # lost or duplicated. Keep the buffer tiny so it is nearly always full.
        N = 100_000
        rb = Dagger.ProcessRingBuffer{Int}(2)
        producer = in_fake_task() do
            for i in 1:N
                put!(rb, i)
            end
        end
        consumer = in_fake_task() do
            bad = 0
            for i in 1:N
                take!(rb) == i || (bad += 1)
            end
            return bad
        end
        done = ()->istaskdone(producer) && istaskdone(consumer)
        @test timedwait(done, 120) == :ok
        if done()
            @test !istaskfailed(producer)
            @test fetch(consumer) == 0
            @test isempty(rb)
        end
    end

    @testset "collect! drains a snapshot" begin
        rb = Dagger.ProcessRingBuffer{Int}(4)
        drained = fetch(in_fake_task() do
            for i in 1:3
                put!(rb, i)
            end
            drained = Dagger.collect!(rb)
            return (drained, Dagger.collect!(rb))
        end)
        @test drained == ([1, 2, 3], Int[])
        @test isempty(rb)
    end

    @testset "wraps around the backing vector" begin
        rb = Dagger.ProcessRingBuffer{Int}(3)
        taken = fetch(in_fake_task() do
            [(put!(rb, i); take!(rb)) for i in 1:10]
        end)
        @test taken == collect(1:10)
        @test isempty(rb)
    end

    @testset "a blocked take! does not starve spawned tasks" begin
        # Julia permanently marks a task `sticky` once it schedules any sticky
        # task (`Base.enq_work`: "XXX: Ideally we would be able to unset this"),
        # which the streaming transport does. A sticky task re-enqueues itself
        # into its *thread-local* workqueue on `yield()`, and `trypoptask`
        # drains that queue before it ever reaches the multiqueue holding
        # `Threads.@spawn`ed tasks. So a sticky task blocked in `take!` must
        # not spin on `yield()` forever: with one on every default thread,
        # nothing spawned can ever start, and the drain tasks a stream spawns
        # to move its values never run at all.
        #
        # Note the pool layout -- `enq_work` places default threads at
        # `threadpoolsize(:interactive)+1 : end`, so pinning to tids `1:nd`
        # would leave a default thread free and hide the starvation entirely.
        ni = Threads.threadpoolsize(:interactive)
        nd = Threads.threadpoolsize(:default)
        if nd < 2
            @test_skip "needs at least 2 default threads"
        else
            rb = Dagger.ProcessRingBuffer{Int}(1)  # empty: every take! blocks
            blocked = Task[]
            for tid in (ni+1):(ni+nd)
                t = Task() do
                    Dagger.set_tls!((; processor = Dagger.ThreadProc(1, Threads.threadid()),
                                       sch_uid = UInt(0),
                                       sch_handle = nothing,
                                       task_spec = nothing,
                                       cancel_token = Dagger.CancelToken(),
                                       logging_enabled = false,
                                       acceleration = Dagger.DistributedAcceleration()))
                    try
                        take!(rb)
                    catch err
                        err isa InvalidStateException || rethrow()
                    end
                end
                t.sticky = true  # exactly what `enq_work` does to a spawned task
                ccall(:jl_set_task_tid, Cint, (Any, Cint), t, tid-1)
                schedule(t)
                push!(blocked, t)
            end
            try
                sleep(1.0)  # let every pinned task reach the wait loop
                spawned = Threads.@spawn nothing
                @test timedwait(()->istaskstarted(spawned), 20.0) == :ok
            finally
                close(rb)
                foreach(wait, blocked)
            end
        end
    end
end

# A buffer that announces when `StreamStore.put!` has parked on it. Reaching
# the wait loop proves `put!` has already snapshotted its output list, which is
# the moment the test needs to mutate that list.
mutable struct ParkSignalBuffer{T}
    inner::Dagger.ProcessRingBuffer{T}
    parked::Threads.Atomic{Bool}
    ParkSignalBuffer{T}(len::Int) where T =
        new{T}(Dagger.ProcessRingBuffer{T}(len), Threads.Atomic{Bool}(false))
end
function Dagger.isfull(b::ParkSignalBuffer)
    full = Dagger.isfull(b.inner)
    full && (b.parked[] = true)
    return full
end
Base.isempty(b::ParkSignalBuffer) = isempty(b.inner)
Base.length(b::ParkSignalBuffer) = length(b.inner)
Base.isopen(b::ParkSignalBuffer) = isopen(b.inner)
Base.close(b::ParkSignalBuffer) = close(b.inner)
Base.put!(b::ParkSignalBuffer, x) = put!(b.inner, x)
Base.take!(b::ParkSignalBuffer) = take!(b.inner)

@testset "StreamStore.put! snapshots its outputs" begin
    # `put!` blocks on a full output buffer, dropping `store.lock` while it
    # waits, and waiters can be added under that lock meanwhile. Iterating the
    # live `output_streams` Dict across that gap is unsafe: Julia's Dict
    # iteration does not detect a concurrent insert, so a rehash silently
    # revisits some entries and skips others. Wire the outputs up by hand so
    # `initialize_output_stream!` (which spawns real drain tasks) stays out of
    # it, and drive the mutation directly the way `add_waiters!` would.
    store = Dagger.StreamStore{Int,ParkSignalBuffer{Int}}(UInt(1), 1, 1)
    original_uids = UInt[10, 11]
    for uid in original_uids
        store.output_streams[uid] = nothing
        # Capacity 1 and pre-filled below, so `put!` must wait on both.
        store.output_buffers[uid] = ParkSignalBuffer{Int}(1)
        push!(store.waiters, Int(uid))
    end
    fetch(in_fake_task() do
        for uid in original_uids
            put!(store.output_buffers[uid], -1)
        end
    end)

    putter = in_fake_task() do
        put!(store, 42)
    end

    # Once any buffer reports a park, `put!` holds a snapshot and is on its way
    # into `wait`. Taking the lock here cannot succeed until it gets there, so
    # the insertions below are guaranteed to land mid-iteration. 100 of them is
    # far more than enough to force the Dict to rehash.
    parked = ()->any(uid->store.output_buffers[uid].parked[], original_uids)
    @test timedwait(parked, 30) == :ok
    added_uids = UInt[]
    @lock store.lock begin
        for uid in UInt(100):UInt(199)
            store.output_streams[uid] = nothing
            store.output_buffers[uid] = ParkSignalBuffer{Int}(4)
            push!(store.waiters, Int(uid))
            push!(added_uids, uid)
        end
    end

    # Drain the pre-filled values so `put!` can make progress. Notify on every
    # poll rather than once: `put!` waits on each output in turn, so it needs
    # waking more than once, and a notify sent before it parks is simply lost.
    drained = fetch(in_fake_task() do
        [take!(store.output_buffers[uid]) for uid in original_uids]
    end)
    @test drained == [-1, -1]
    @test timedwait(60) do
        @lock store.lock notify(store.lock)
        istaskdone(putter)
    end == :ok

    if istaskdone(putter)
        @test !istaskfailed(putter)
        # Every output present when `put!` started gets the value exactly once.
        @test all(uid->length(store.output_buffers[uid]) == 1, original_uids)
        got = fetch(in_fake_task() do
            [take!(store.output_buffers[uid]) for uid in original_uids]
        end)
        @test got == [42, 42]
        # Outputs added mid-`put!` start at the *next* value, deterministically.
        @test all(uid->isempty(store.output_buffers[uid]), added_uids)
    end
end

all_scopes = [Dagger.ExactScope(proc) for proc in Dagger.all_processors()]
for idx in 1:5
    if idx == 1
        scopes = [Dagger.scope(worker = 1, thread = 1)]
        scope_str = "Worker 1"
    elseif idx == 2 && nprocs() > 1
        scopes = [Dagger.scope(worker = 2, thread = 1)]
        scope_str = "Worker 2"
    else
        scopes = all_scopes
        scope_str = "All Workers"
    end

    @testset "Single Task Control Flow ($scope_str)" begin
        @test !test_finishes("Single task running forever"; max_evals=1_000_000, ignore_timeout=true) do
            local x
            Dagger.spawn_streaming(;teardown=false) do
                x = Dagger.@spawn scope=rand(scopes) () -> begin
                    y = rand()
                    sleep(1)
                    return y
                end
            end
            @test_throws_unwrap InterruptException fetch(x)
        end

        @test test_finishes("Single task without result") do
            local x
            Dagger.spawn_streaming(;teardown=false) do
                x = Dagger.@spawn scope=rand(scopes) rand()
            end
            @test fetch(x) === nothing
        end

        @test test_finishes("Single task with result"; max_evals=1_000_000) do
            local x
            Dagger.spawn_streaming(;teardown=false) do
                x = Dagger.@spawn scope=rand(scopes) () -> begin
                   x = rand()
                    if x < 0.1
                        return Dagger.finish_stream(x; result=123)
                    end
                    return x
                end
            end
            @test fetch(x) == 123
        end
    end

    @testset "Non-Streaming Inputs ($scope_str)" begin
        @test test_finishes("() -> A") do
            local A
            Dagger.spawn_streaming(;teardown=false) do
                A = Dagger.@spawn scope=rand(scopes) accumulator()
            end
            @test fetch(A) === nothing
            values = take_accumulator!()
            A_tid = Dagger.task_id(A)
            @test length(values[A_tid]) == 10
            @test all(==(0), values[A_tid])
        end
        @test test_finishes("42 -> A") do
            local A
            Dagger.spawn_streaming(;teardown=false) do
                A = Dagger.@spawn scope=rand(scopes) accumulator(42)
            end
            @test fetch(A) === nothing
            values = take_accumulator!()
            A_tid = Dagger.task_id(A)
            @test length(values[A_tid]) == 10
            @test all(==(42), values[A_tid])
        end
        @test test_finishes("(42, 43) -> A") do
            local A
            Dagger.spawn_streaming(;teardown=false) do
                A = Dagger.@spawn scope=rand(scopes) accumulator(42, 43)
            end
            @test fetch(A) === nothing
            values = take_accumulator!()
            A_tid = Dagger.task_id(A)
            @test length(values[A_tid]) == 10
            @test all(==(42 + 43), values[A_tid])
        end
    end

    @testset "Non-Streaming Outputs ($scope_str)" begin
        @test test_finishes("x -> A") do
            local x, A
            Dagger.spawn_streaming(;teardown=false) do
                x = Dagger.@spawn scope=rand(scopes) rand()
            end
            Dagger._without_options() do
                A = Dagger.@spawn accumulator(x)
            end
            @test fetch(x) === nothing
            @test fetch(A) === nothing
            values = take_accumulator!()
            A_tid = Dagger.task_id(A)
            @test length(values[A_tid]) == 1
            @test all(v -> 0 <= v <= 10, values[A_tid])
        end

        @test test_finishes("x -> (A, B)") do
            local x, A, B
            Dagger.spawn_streaming(;teardown=false) do
                x = Dagger.@spawn scope=rand(scopes) rand()
            end
            Dagger._without_options() do
                A = Dagger.@spawn accumulator(x)
                B = Dagger.@spawn accumulator(x)
            end
            @test fetch(x) === nothing
            @test fetch(A) === nothing
            @test fetch(B) === nothing
            values = take_accumulator!()
            A_tid = Dagger.task_id(A)
            @test length(values[A_tid]) == 1
            @test all(v -> 0 <= v <= 10, values[A_tid])
            B_tid = Dagger.task_id(B)
            @test length(values[B_tid]) == 1
            @test all(v -> 0 <= v <= 10, values[B_tid])
        end
    end

    @testset "Teardown" begin
        # N.B. No `ignore_timeout`: this one is asserted to *finish*, so it wants
        # the generous hang-detector budget rather than the 10s budget meant for
        # tests that are supposed to run forever (see `test_finishes`).
        @test test_finishes("teardown=true"; max_evals=1_000_000) do
            local x, y
            Dagger.spawn_streaming(;teardown=true) do
                x = Dagger.@spawn scope=rand(scopes) () -> begin
                    sleep(0.1)
                    return rand()
                end
                y = Dagger.with_options(;stream_max_evals=10) do
                    Dagger.@spawn scope=rand(scopes) identity(x)
                end
            end
            @test fetch(y) === nothing
            # Wait for teardown. Measured at ~0.6s warm but 1.2-1.5s in a cold
            # process, so the `sleep(1)` this replaces had a negative margin
            # exactly when the suite runs first -- and failed twice over, since
            # the `fetch(x)` below then blocked until the whole test's budget
            # expired.
            @test timedwait(()->istaskdone(x), 30) == :ok
            fetch(x)
        end
        @test !test_finishes("teardown=false"; max_evals=1_000_000, ignore_timeout=true) do
            local x, y
            Dagger.spawn_streaming(;teardown=false) do
                x = Dagger.@spawn scope=rand(scopes) () -> begin
                    sleep(0.1)
                    return rand()
                end
                y = Dagger.with_options(;stream_max_evals=10) do
                    Dagger.@spawn scope=rand(scopes) identity(x)
                end
            end
            @test fetch(y) === nothing
            sleep(1) # Wait to ensure `x` task is still running
            @test !istaskdone(x)
            @test_throws_unwrap InterruptException fetch(x)
        end
    end

    @testset "Multiple Tasks ($scope_str)" begin
        @test test_finishes("x -> A") do
            local x, A
            Dagger.spawn_streaming(;teardown=false) do
                x = Dagger.@spawn scope=rand(scopes) rand()
                A = Dagger.@spawn scope=rand(scopes) accumulator(x)
            end
            @test fetch(x) === nothing
            @test fetch(A) === nothing
            values = take_accumulator!()
            A_tid = Dagger.task_id(A)
            @test length(values[A_tid]) == 10
            @test all(v -> 0 <= v <= 1, values[A_tid])
        end

        @test test_finishes("(x, A)") do
            local x, A
            Dagger.spawn_streaming(;teardown=false) do
                x = Dagger.@spawn scope=rand(scopes) rand()
                A = Dagger.@spawn scope=rand(scopes) accumulator(1.0)
            end
            @test fetch(x) === nothing
            @test fetch(A) === nothing
            values = take_accumulator!()
            A_tid = Dagger.task_id(A)
            @test length(values[A_tid]) == 10
            @test all(v -> v == 1, values[A_tid])
        end

        @test test_finishes("x -> y -> A") do
            local x, y, A
            Dagger.spawn_streaming(;teardown=false) do
                x = Dagger.@spawn scope=rand(scopes) rand()
                y = Dagger.@spawn scope=rand(scopes) x+1
                A = Dagger.@spawn scope=rand(scopes) accumulator(y)
            end
            @test fetch(x) === nothing
            @test fetch(y) === nothing
            @test fetch(A) === nothing
            values = take_accumulator!()
            A_tid = Dagger.task_id(A)
            @test length(values[A_tid]) == 10
            @test all(v -> 1 <= v <= 2, values[A_tid])
        end

        @test test_finishes("x -> (y, A)") do
            local x, y, A
            Dagger.spawn_streaming(;teardown=false) do
                x = Dagger.@spawn scope=rand(scopes) rand()
                y = Dagger.@spawn scope=rand(scopes) x+1
                A = Dagger.@spawn scope=rand(scopes) accumulator(x)
            end
            @test fetch(x) === nothing
            @test fetch(y) === nothing
            @test fetch(A) === nothing
            values = take_accumulator!()
            A_tid = Dagger.task_id(A)
            @test length(values[A_tid]) == 10
            @test all(v -> 0 <= v <= 1, values[A_tid])
        end

        @test test_finishes("(x, y) -> A") do
            local x, y, A
            Dagger.spawn_streaming(;teardown=false) do
                x = Dagger.@spawn scope=rand(scopes) rand()
                y = Dagger.@spawn scope=rand(scopes) rand()
                A = Dagger.@spawn scope=rand(scopes) accumulator(x, y)
            end
            @test fetch(x) === nothing
            @test fetch(y) === nothing
            @test fetch(A) === nothing
            values = take_accumulator!()
            A_tid = Dagger.task_id(A)
            @test length(values[A_tid]) == 10
            @test all(v -> 0 <= v <= 2, values[A_tid])
        end

        @test test_finishes("(x, y) -> z -> A") do
            local x, y, z, A
            Dagger.spawn_streaming(;teardown=false) do
                x = Dagger.@spawn scope=rand(scopes) rand()
                y = Dagger.@spawn scope=rand(scopes) rand()
                z = Dagger.@spawn scope=rand(scopes) x + y
                A = Dagger.@spawn scope=rand(scopes) accumulator(z)
            end
            @test fetch(x) === nothing
            @test fetch(y) === nothing
            @test fetch(z) === nothing
            @test fetch(A) === nothing
            values = take_accumulator!()
            A_tid = Dagger.task_id(A)
            @test length(values[A_tid]) == 10
            @test all(v -> 0 <= v <= 2, values[A_tid])
        end

        @test test_finishes("x -> (y, z) -> A") do
            local x, y, z, A
            Dagger.spawn_streaming(;teardown=false) do
                x = Dagger.@spawn scope=rand(scopes) rand()
                y = Dagger.@spawn scope=rand(scopes) x + 1
                z = Dagger.@spawn scope=rand(scopes) x + 2
                A = Dagger.@spawn scope=rand(scopes) accumulator(y, z)
            end
            @test fetch(x) === nothing
            @test fetch(y) === nothing
            @test fetch(z) === nothing
            @test fetch(A) === nothing
            values = take_accumulator!()
            A_tid = Dagger.task_id(A)
            @test length(values[A_tid]) == 10
            @test all(v -> 3 <= v <= 5, values[A_tid])
        end

        @test test_finishes("(x, y) -> z -> (A, B)") do
            local x, y, z, A, B
            Dagger.spawn_streaming(;teardown=false) do
                x = Dagger.@spawn scope=rand(scopes) rand()
                y = Dagger.@spawn scope=rand(scopes) rand()
                z = Dagger.@spawn scope=rand(scopes) x + y
                A = Dagger.@spawn scope=rand(scopes) accumulator(z)
                B = Dagger.@spawn scope=rand(scopes) accumulator(z)
            end
            @test fetch(x) === nothing
            @test fetch(y) === nothing
            @test fetch(z) === nothing
            @test fetch(A) === nothing
            @test fetch(B) === nothing

            values = take_accumulator!()
            A_tid = Dagger.task_id(A)
            @test length(values[A_tid]) == 10
            @test all(v -> 0 <= v <= 2, values[A_tid])
            B_tid = Dagger.task_id(B)
            @test length(values[B_tid]) == 10
            @test all(v -> 0 <= v <= 2, values[B_tid])
        end

        for T in (Float64, Int32, BigFloat)
            @test test_finishes("Stream eltype $T") do
                local x, A
                Dagger.spawn_streaming(;teardown=false) do
                    x = Dagger.@spawn scope=rand(scopes) rand(T)
                    A = Dagger.@spawn scope=rand(scopes) accumulator(x)
                end
                @test fetch(x) === nothing
                @test fetch(A) === nothing
                values = take_accumulator!()
                A_tid = Dagger.task_id(A)
                @test length(values[A_tid]) == 10
                @test all(v -> v isa T, values[A_tid])
            end
        end
    end

    @testset "Max Evals ($scope_str)" begin
        @test test_finishes("max_evals=0"; max_evals=0) do
            @test_throws ArgumentError Dagger.spawn_streaming(;teardown=false) do
                A = Dagger.@spawn scope=rand(scopes) accumulator()
            end
        end
        @test test_finishes("max_evals=1"; max_evals=1) do
            local A
            Dagger.spawn_streaming(;teardown=false) do
                A = Dagger.@spawn scope=rand(scopes) accumulator()
            end
            @test fetch(A) === nothing
            values = take_accumulator!()
            A_tid = Dagger.task_id(A)
            @test length(values[A_tid]) == 1
        end
        @test test_finishes("max_evals=100"; max_evals=100) do
            local A
            Dagger.spawn_streaming(;teardown=false) do
                A = Dagger.@spawn scope=rand(scopes) accumulator()
            end
            @test fetch(A) === nothing
            values = take_accumulator!()
            A_tid = Dagger.task_id(A)
            @test length(values[A_tid]) == 100
        end
    end

    # FIXME: Varying buffer amounts

    #= TODO: Zero-allocation test
    # First execution of a streaming task will almost guaranteed allocate (compiling, setup, etc.)
    # BUT, second and later executions could possibly not allocate any further ("steady-state")
    # We want to be able to validate that the steady-state execution for certain tasks is non-allocating
    =#
end
