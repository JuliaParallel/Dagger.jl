# Runs `f` on its own task and gives up after `secs`, so a broken loop is a failed test and not a hang
function bounded(f, secs=120)
    t = Threads.@spawn f()
    timedwait(() -> istaskdone(t), secs) === :ok || return :timeout
    return fetch(t)
end
# The exception `f` throws, or `nothing`; a hang is reported as `:timeout` instead of blocking the suite
throws(f, secs=20) = bounded(() -> try; f(); nothing; catch e; e; end, secs)

# Two workers, one index each at the start (chunk 1): each waits (spinning, without yielding its thread) until the other has
# arrived. They can only both arrive if they run at the same time, which is only possible on two different threads. Returns the
# thread of each worker's first index, or `nothing` if the other never arrived.
function rendezvous_loop(args...; kwargs...)
    arrived = Threads.Atomic{Int}(0)
    tids = zeros(Int, 2)
    met = Threads.Atomic{Bool}(true)
    function f(i, _...)
        if i <= 2
            tids[i] = Threads.threadid()
            Threads.atomic_add!(arrived, 1)
            t0 = time_ns()
            while arrived[] < 2
                if time_ns() - t0 > 1_000_000_000
                    met[] = false
                    break
                end
            end
        end
        return nothing
    end
    counts = Dagger.dynamic_for(f, 50, args...; workers=2, chunk=1, kwargs...)
    return met[] ? (tids, counts) : nothing
end

@testset "Dynamic loop" begin
    @testset "every index exactly once" begin
        n = 5000
        hits = zeros(Int, n); acc = Threads.Atomic{Int}(0)
        @test bounded(() -> Dagger.dynamic_for(i -> (hits[i] += 1; Threads.atomic_add!(acc, i)), n)) !== :timeout
        @test all(==(1), hits)
        @test acc[] == n * (n + 1) ÷ 2
    end
    @testset "edge cases" begin
        c = Threads.Atomic{Int}(0)
        Dagger.dynamic_for(i -> Threads.atomic_add!(c, 1), 3)                   # fewer iterations than workers
        Dagger.dynamic_for(i -> Threads.atomic_add!(c, 1), 1; workers=8)
        Dagger.dynamic_for(i -> Threads.atomic_add!(c, 1), 10; chunk=100)       # chunk larger than the loop
        Dagger.dynamic_for(i -> Threads.atomic_add!(c, 1), 0)
        @test c[] == 14
        # (a worker that was started with a chunk of 0 would never finish; keep it off the driver's thread so a missing check is a
        # failed test and not a hang)
        away = Dagger.ExactScope(Dagger.ThreadProc(myid(), Threads.nthreads()))
        @test throws(() -> Dagger.dynamic_for(identity, 10; chunk=0, scope=away)) isa ArgumentError
        @test throws(() -> Dagger.dynamic_for(identity, 10; chunk=-3, scope=away)) isa ArgumentError
        @test throws(() -> Dagger.dynamic_for(identity, 10; workers=0)) isa ArgumentError
        @test throws(() -> Dagger.dynamic_reduce(identity, +, 10; chunk=0)) isa ArgumentError
        @test throws(() -> Dagger.dynamic_reduce(identity, +, 10; workers=0)) isa ArgumentError
        # a negative count is an empty range, like `1:n`
        @test sum(Dagger.dynamic_for(i -> error("never"), -5)) == 0
        @test Dagger.dynamic_reduce(i -> error("never"), +, -5; init=3) == 3
        @test throws(() -> Dagger.dynamic_reduce(identity, +, -5)) isa ArgumentError
    end
    @testset "failure propagates" begin
        @test_throws Exception Dagger.dynamic_for(i -> i == 7 ? error("boom") : nothing, 100)
    end
    # The workers of a loop are bound one per thread. Placed by load and then stolen, the second of two workers could run behind the
    # first on one thread, so one thread took every index
    @testset "workers run concurrently, one per thread" begin
        if Threads.nthreads() >= 2
            sc = Dagger.loop_worker_scopes(Dagger.DistributedAcceleration(), Dagger.scope(worker=myid()), 2)
            @test sc isa AbstractVector && all(s -> s isa Dagger.ExactScope, sc)     # one named thread per worker
            @test length(unique(s.processor.tid for s in sc)) == length(sc) == Threads.nthreads()
            # a scope that is not several thread processors of one worker is passed on unchanged
            one = Dagger.ExactScope(Dagger.ThreadProc(myid(), 1))
            @test Dagger.loop_worker_scopes(Dagger.DistributedAcceleration(), one, 2) === one
            if length(procs()) > 1                  # processors of several workers: no single process to bind within
                several = Dagger.scope(workers=procs())
                @test Dagger.loop_worker_scopes(Dagger.DistributedAcceleration(), several, 2) === several
            end
            for rep in 1:10
                r = bounded(() -> rendezvous_loop())
                @test r !== :timeout && r !== nothing            # both workers were running at once
                r isa Tuple || break
                tids, counts = r
                @test tids[1] != tids[2] && all(>(0), tids)      # on two different threads
                @test sum(counts) == 50
            end
            # one thread each when started from inside a task, and with a DTask argument
            r = fetch(Dagger.@spawn(scope=Dagger.scope(worker=myid()), rendezvous_loop()))
            @test r !== nothing && r[1][1] != r[1][2]
            v = Dagger.@spawn identity(1)
            r = bounded(() -> rendezvous_loop(v))
            @test r !== :timeout && r !== nothing && r[1][1] != r[1][2]
        else
            @test_skip false
        end
    end
    @testset "default chunk sizes" begin
        # `dynamic_for`: chunk = cld(n, 4workers): with n = 80 and 2 workers that is 10, so each block of 10 consecutive indices is
        # taken by one worker
        owner = zeros(UInt, 80)
        Dagger.dynamic_for(i -> (owner[i] = objectid(current_task()); sleep(0.001)), 80; workers=2)
        @test all(b -> length(unique(owner[(10b + 1):(10b + 10)])) == 1, 0:7)
        # `dynamic_reduce`: chunk = cld(n, 64), whatever the number of workers (n = 130: chunks of 3). A reduction that records its
        # bracketing tells chunk sizes apart.
        tree(i) = string(i)
        join_(a, b) = string("(", a, " ", b, ")")
        by_chunk(c) = Dagger.dynamic_reduce(tree, join_, 130; chunk=c, workers=2)
        @test by_chunk(3) != by_chunk(4) && by_chunk(3) != by_chunk(2)
        for w in (1, 2, 5)
            @test Dagger.dynamic_reduce(tree, join_, 130; workers=w) == by_chunk(3)
        end
    end
    @testset "per-worker counts" begin
        counts = Dagger.dynamic_for(i -> nothing, 1000; workers=3, chunk=7)
        @test length(counts) == 3 && sum(counts) == 1000
        @test sum(fetch.(Dagger.dynamic_for_async(i -> nothing, 55; workers=2))) == 55
        @test sum(Dagger.dynamic_for(i -> nothing, 0)) == 0     # no iterations: nothing taken
    end
    @testset "async form returns the workers" begin
        c = Threads.Atomic{Int}(0)
        tasks = Dagger.dynamic_for_async(i -> Threads.atomic_add!(c, 1), 200; workers=2)
        @test length(tasks) == 2
        foreach(fetch, tasks)
        @test c[] == 200
    end
    @testset "task arguments: the loop waits for DTasks and chunks" begin
        # `f(i, args...)`; the first argument is produced by a task that waits for `gate`, so no worker can start before it
        gate = Base.Event()
        hold_range(g, n) = (wait(g); collect(1:n))
        scale(i, v, w, out) = (out[i] = v[i] * w; nothing)
        n = 400
        out = zeros(Int, n)
        v = Dagger.@spawn hold_range(gate, n)               # a DTask argument
        w = Dagger.tochunk(3)                               # a Chunk argument
        started = bounded(() -> Dagger.dynamic_for_async(scale, n, v, w, out; workers=2), 20)   # returns without waiting for `v`
        started === :timeout && notify(gate)
        @test started !== :timeout
        tasks = started === :timeout ? Dagger.dynamic_for_async(scale, n, v, w, out; workers=2) : started
        sleep(0.3)
        started === :timeout || @test !any(istaskdone, tasks) && all(==(0), out)    # nothing ran: the workers are waiting for `v`
        notify(gate)
        foreach(fetch, tasks)
        @test out == 3 .* (1:n)
        out2 = zeros(Int, n)
        Dagger.dynamic_for(scale, n, v, 5, out2)            # blocking form, plain value argument
        @test out2 == 5 .* (1:n)
    end
    @testset "called from inside tasks, with every thread busy" begin
        # each outer task holds a processor while its loop runs: the loop must still finish
        c = Threads.Atomic{Int}(0)
        outer(c) = (Dagger.dynamic_for(i -> Threads.atomic_add!(c, 1), 50; workers=2); nothing)
        # (on this process: `c` is a plain Atomic, a copy on another worker would not be counted)
        ts = [Dagger.@spawn(scope=Dagger.scope(worker=myid()), outer(c)) for _ in 1:max(2, Threads.nthreads())]
        @test all(t -> fetch(t) === nothing, ts)
        @test c[] == 50 * length(ts)
    end
    @testset "a vector of scopes: one per worker" begin
        n = 40
        out = zeros(Int, n)
        sc = [Dagger.scope(worker=myid()), Dagger.scope(worker=myid())]
        Dagger.dynamic_for((i, o) -> (o[i] = i; nothing), n, out; workers=3, scope=sc)   # cycled over the workers
        @test out == collect(1:n)
        @test Dagger.dynamic_reduce((i, v) -> v[i], +, n, collect(1:n); workers=2, scope=sc) == sum(1:n)
        if Threads.nthreads() >= 2
            # worker w runs on the thread its own scope names: two workers on threads 1 and 2 are concurrent, which two workers on
            # one thread cannot be
            two = Dagger.AbstractScope[Dagger.ExactScope(Dagger.ThreadProc(myid(), t)) for t in 1:2]
            r = bounded(() -> rendezvous_loop(scope=two))
            @test r !== :timeout && r !== nothing
            r isa Tuple && @test sort(r[1]) == [1, 2]
            # the vector is cycled: one scope for every worker
            seen = zeros(Int, Threads.maxthreadid())
            Dagger.dynamic_for(i -> (seen[Threads.threadid()] += 1; nothing), 60; workers=3, chunk=1,
                               scope=Dagger.AbstractScope[Dagger.ExactScope(Dagger.ThreadProc(myid(), 2))])
            @test seen[2] == 60 && sum(seen) == 60
        end
    end
    @testset "dynamic_reduce: fixed combination order" begin
        # floating-point sum: bitwise the same for every worker count and run, and equal to the chunked fold in order
        x = [1 / (1 + i)^1.1 * (iseven(i) ? -1 : 1) for i in 1:5000]
        ref(chunk) = begin
            parts = [foldl(+, (x[i] for i in (lo + 2):min(lo + chunk, 5000)); init=x[lo + 1]) for lo in 0:chunk:4999]
            foldl(+, parts)
        end
        for chunk in (1, 7, 100)
            results = [Dagger.dynamic_reduce((i, v) -> v[i], +, 5000, x; chunk, workers=w) for w in (1, 2, 8, 8)]
            @test all(r -> r === ref(chunk), results)       # same bits, not just approximately equal
        end
        @test Dagger.dynamic_reduce((i, v) -> v[i], +, 5000, x) === Dagger.dynamic_reduce((i, v) -> v[i], +, 5000, x; workers=3)
        # not commutative: the order of the indices is kept
        @test Dagger.dynamic_reduce(i -> string(i, ","), *, 40; chunk=6, workers=4) == join(string.(1:40, ","))
        # init, empty range, a DTask argument, a failure
        @test Dagger.dynamic_reduce(i -> i, +, 100; init=1000) == 1000 + 5050
        @test Dagger.dynamic_reduce(i -> i, +, 0; init=7) == 7
        @test_throws ArgumentError Dagger.dynamic_reduce(i -> i, +, 0)
        slow_range(n) = (sleep(0.2); collect(1:n))
        v = Dagger.@spawn slow_range(100)
        @test Dagger.dynamic_reduce((i, vec) -> vec[i], +, 100, v) == 5050
        @test_throws Exception Dagger.dynamic_reduce(i -> i == 7 ? error("boom") : 1, +, 100)
    end
end
