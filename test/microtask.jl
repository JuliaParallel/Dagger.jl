using Dagger, Test, Random

# Light tasks (@light): protocol stress test (random DAGs built concurrently, multiple waiters, nested spawns, errors) and DTask interop.
# Count-bounded on purpose (AGENTS.md lessons 12, 27); with 300 rounds an early-launch ordering bug fails the stress part 3 of 3 times at 8 threads, with 40 rounds it slipped through (lesson 136).
@testset "Light tasks" begin
    ITER = 300
    busy(k) = (s = 0; for i in 1:k; s += i; end; s)
    node(xs...) = 1 + sum(xs; init = 0)

    function build_dag(rng, n; maxfan = 8)
        deps = [Int[] for _ in 1:n]
        for i in 2:n
            k = rand(rng, 0:min(maxfan, i - 1))
            deps[i] = unique([rand(rng) < 0.7 ? max(1, i - rand(rng, 1:6)) : rand(rng, 1:i-1) for _ in 1:k])
        end
        return deps
    end
    function sequential(deps)
        v = zeros(Int, length(deps))
        for i in eachindex(deps); v[i] = 1 + sum((v[d] for d in deps[i]); init = 0); end
        return v
    end
    function run_dag(deps, jitter)
        ts = Vector{LightTask}(undef, length(deps))
        for i in eachindex(deps)
            k = jitter[i]
            ts[i] = @light ((xs...) -> (busy(k); node(xs...)))((ts[d] for d in deps[i])...)
        end
        return [fetch(t) for t in ts]
    end

    @testset "basics" begin
        @test fetch(@light +(1, 2)) == 3
        a = @light +(1, 2); b = @light *(a, 10)
        @test fetch(b) == 30
        @test istaskdone(a)
        @test_throws ArgumentError macroexpand(@__MODULE__, :(@light 1))
    end

    @testset "stress" begin
        bad = Threads.Atomic{Int}(0)
        @sync for th in 1:max(2, Threads.nthreads())
            Threads.@spawn begin
                rng = MersenneTwister(1000 + th)
                for it in 1:ITER
                    n = rand(rng, 20:200); deps = build_dag(rng, n); jitter = rand(rng, (0, 0, 0, 1, 10, 200), n)
                    run_dag(deps, jitter) == sequential(deps) || Threads.atomic_add!(bad, 1)
                end
            end
        end
        @test bad[] == 0

        for _ in 1:ITER   # many waiters, before and after the task finishes
            t = @light (() -> (busy(rand(0:500)); 42))()
            rs = Vector{Int}(undef, 8)
            @sync for i in 1:8; Threads.@spawn (rs[i] = fetch(t)); end
            @test all(==(42), rs)
        end

        function tree(depth)   # spawned from inside tasks, awaited from inside tasks
            depth == 0 && return 1
            a = @light tree(depth - 1); b = @light tree(depth - 1)
            return fetch(a) + fetch(b)
        end
        @test fetch(@light tree(8)) == 2^8
    end

    @testset "errors propagate" begin
        for it in 1:ITER
            n = 60; deps = build_dag(MersenneTwister(it), n); poison = rand(MersenneTwister(10_000 + it), 2:n)
            ts = Vector{LightTask}(undef, n)
            for i in 1:n
                f = i == poison ? ((xs...) -> error("boom")) : node
                ts[i] = @light f((ts[d] for d in deps[i])...)
            end
            down = falses(n); down[poison] = true
            for i in 1:n; any(d -> down[d], deps[i]) && (down[i] = true); end
            seq = zeros(Int, n)
            for i in 1:n; down[i] || (seq[i] = 1 + sum((seq[d] for d in deps[i]); init = 0)); end
            for i in 1:n
                if down[i]
                    @test_throws CapturedException fetch(ts[i])
                else
                    @test fetch(ts[i]) == seq[i]
                end
            end
        end
    end

    @testset "DTask interop" begin
        # DTask -> light task, DTask already finished and still running
        d1 = Dagger.@spawn (() -> (sleep(0.05); 20))()
        l1 = @light +(d1, 1)
        @test fetch(l1) == 21
        d2 = Dagger.@spawn identity(5); wait(d2)
        @test fetch(@light *(d2, 2)) == 10
        # mixed light + DTask arguments
        @test fetch(@light +(d1, l1, 100)) == 20 + 21 + 100
        # light task -> DTask (slow direction), finished and unfinished
        l2 = @light (() -> (sleep(0.05); 7))()
        @test fetch(Dagger.@spawn +(Dagger.light_to_dtask(l2), 1)) == 8
        @test fetch(Dagger.light_to_dtask(l2)) == 7
        # failures cross the boundary in both directions
        dbad = Dagger.@spawn error("dtask boom")
        @test_throws Exception fetch(@light identity(dbad))
        lbad = @light error("light boom")
        @test_throws Exception fetch(Dagger.light_to_dtask(lbad))
        # chain through both kinds, compared with the sequential value
        acc = @light identity(0)
        for i in 1:20
            acc = isodd(i) ? @light(+(acc, i)) : Dagger.light_to_dtask(@light(+(acc, i)))
            acc isa DTask && (acc = @light identity(acc))
        end
        @test fetch(acc) == sum(1:20)
    end
end

@testset "Light tasks: a continuation calls functions defined after its predecessor started" begin
    # A task released by another light task is spawned from that task and inherits its world age; a function defined (eval'd) after the predecessor
    # started would be "too new" for it without `invokelatest`.
    gate = Base.Event()
    pred = Dagger.light_spawn(() -> (wait(gate); nothing))
    f = Core.eval(Main, :(world_age_probe_fn(x, y) = (x, y)))        # a new method, defined after `pred` started running
    l = Dagger.light_spawn(f, 1, pred)
    notify(gate)
    @test fetch(l) == (1, nothing)
end
