@testset "Dynamic loop" begin
    @testset "every index exactly once" begin
        n = 5000
        hits = zeros(Int, n); acc = Threads.Atomic{Int}(0)
        Dagger.dynamic_for(i -> (hits[i] += 1; Threads.atomic_add!(acc, i)), n)
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
        @test_throws ArgumentError Dagger.dynamic_for(identity, 10; chunk=0)
    end
    @testset "failure propagates" begin
        @test_throws Exception Dagger.dynamic_for(i -> i == 7 ? error("boom") : nothing, 100)
    end
    # The workers of a loop are bound one per thread. Placed by load and then stolen, the second of two workers ran behind the first on
    # one thread in a third of the loops, so one thread took every index
    @testset "workers run concurrently" begin
        if Threads.nthreads() >= 2
            sc = Dagger.loop_worker_scopes(Dagger.DistributedAcceleration(), Dagger.scope(worker=1), 2)
            @test sc isa AbstractVector && all(s -> s isa Dagger.ExactScope, sc)     # one named thread per worker
            @test length(unique(s.processor.tid for s in sc)) == length(sc) >= 2
            spin(i) = (t0 = time_ns(); while time_ns() - t0 < 100_000; end; nothing)
            skewed(reps) = count(1:reps) do _
                counts = Dagger.dynamic_for(spin, 200; workers=2, chunk=1)
                maximum(counts) == 200
            end
            skewed(5)  # compile
            @test skewed(100) <= 3
        else
            @test_skip false
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
        tasks = Dagger.dynamic_for_async(scale, n, v, w, out; workers=2)   # returns without waiting for `v`
        sleep(0.3)
        @test !any(istaskdone, tasks) && all(==(0), out)    # nothing ran: the workers are waiting for `v`
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
    end
end
