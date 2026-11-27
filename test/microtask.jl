using Dagger, Test, Random

# Light tasks (@light): protocol stress test (random DAGs built concurrently, multiple waiters, nested spawns, errors) and DTask interop.
# Count-bounded on purpose (AGENTS.md lessons 12, 27); with 300 rounds an early-launch ordering bug fails the stress part 3 of 3 times at 8 threads, with 40 rounds it slipped through (lesson 136).
@testset "Light tasks" begin
    ITER = 300
    busy(k) = (s = 0; for i in 1:k; s += i; end; s)
    node(xs...) = 1 + sum(xs; init = 0)

    # A protocol bug usually shows as a task that never runs, so every wait in the stress parts has a bound: a hang becomes a failed assertion.
    struct LightTimeout <: Exception end
    # After the first timeout the protocol is known to be broken (and tasks are stuck): later waits fail at once instead of adding 5 s each.
    stuck = Threads.Atomic{Bool}(false)
    function await(t; secs = 5)
        stuck[] && throw(LightTimeout())
        timedwait(() -> istaskdone(t), secs; pollint = 0.001) === :ok && return t
        stuck[] = true
        throw(LightTimeout())
    end
    tfetch(t; secs = 5) = fetch(await(t; secs))

    # Widens the race windows of the protocol (`Dagger.light_point`): yield, spin or sleep at random.  Task-local RNG, no shared state.
    function perturb(::Symbol)
        r = rand(UInt8)
        r < 40 ? yield() : r < 64 ? busy(rand(1:400)) : r == 255 ? sleep(1e-4) : nothing
        return nothing
    end
    function perturbed(f)
        Dagger.LIGHT_HOOK[] = perturb
        try
            return f()
        finally
            Dagger.LIGHT_HOOK[] = nothing
        end
    end

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
        return [tfetch(t) for t in ts]
    end

    # Random DAGs built concurrently from every thread; returns the number of rounds that gave a wrong value, an error or a timeout.
    function stress_dags(iter)
        bad = Threads.Atomic{Int}(0)
        @sync for th in 1:max(2, Threads.nthreads())
            Threads.@spawn begin
                rng = MersenneTwister(1000 + th)
                for it in 1:iter
                    bad[] > 0 && break
                    n = rand(rng, 20:200); deps = build_dag(rng, n); jitter = rand(rng, (0, 0, 0, 1, 10, 200), n)
                    ok = try run_dag(deps, jitter) == sequential(deps) catch; false end
                    ok || Threads.atomic_add!(bad, 1)
                end
            end
        end
        return bad[]
    end

    # Several waiters on one task, before and after it finishes.
    function stress_waiters(iter)
        for _ in 1:iter
            t = @light (() -> (busy(rand(0:500)); 42))()
            ws = [Threads.@spawn(try tfetch(t) catch; nothing end) for _ in 1:8]
            all(==(42), fetch.(ws)) || return false
        end
        return true
    end

    function tree(depth)   # spawned from inside tasks, awaited from inside tasks
        depth == 0 && return 1
        a = @light tree(depth - 1); b = @light tree(depth - 1)
        return fetch(a) + fetch(b)   # bounded from outside by `tfetch` of the root
    end

    @testset "basics" begin
        @test tfetch(@light +(1, 2)) == 3
        a = @light +(1, 2); b = @light *(a, 10)
        @test tfetch(b) == 30
        @test istaskdone(a)
        c = @light identity(3)                     # the same dependency several times: one registration and one release each
        @test tfetch(@light +(c, c, c)) == 9
        d = Dagger.@spawn identity(4)
        @test tfetch(@light +(d, d)) == 8
        f = @light error("boom")
        await(f)                                    # `wait` does not throw, `fetch` does
        @test istaskdone(f)
        @test_throws CapturedException tfetch(f)
        @test_throws CapturedException tfetch(f)   # and keeps doing so
        @test_throws ArgumentError macroexpand(@__MODULE__, :(@light 1))
        @test_throws ArgumentError macroexpand(@__MODULE__, :(@light f(1; k = 2)))
    end

    @testset "no inherited dynamic scope" begin
        # The task that releases a light task is arbitrary, so the body must not see the options of whoever created or released it.
        inner = @light Dagger.get_options()
        outer = Dagger.with_options(; scope = Dagger.scope(worker = 1, thread = 1)) do
            (@light(Dagger.get_options()), @light((_ -> Dagger.get_options())(inner)))
        end
        @test tfetch(outer[1]) == NamedTuple()
        @test tfetch(outer[2]) == NamedTuple()
    end

    @testset "stress" begin
        @test stress_dags(ITER) == 0
        @test stress_waiters(ITER)
        @test tfetch(@light(tree(8)); secs = 30) == 2^8
    end

    @testset "stress, perturbed schedule" begin
        perturbed() do
            @test stress_dags(ITER ÷ 3) == 0
            @test stress_waiters(ITER ÷ 3)
            @test tfetch(@light(tree(6)); secs = 30) == 2^6
        end
    end

    @testset "errors propagate" begin
        for it in 1:ITER
            n = 60; deps = build_dag(MersenneTwister(it), n); poison = rand(MersenneTwister(10_000 + it), 2:n)
            ts = Vector{LightTask}(undef, n)
            for i in 1:n
                f = i == poison ? ((xs...) -> error("boom")) : node
                ts[i] = @light f((ts[d] for d in deps[i])...)
            end
            finished = !stuck[] && timedwait(() -> all(istaskdone, ts), 5; pollint = 0.001) === :ok
            finished || (stuck[] = true)
            @test finished
            finished || break
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
        ltd(l) = Dagger.light_to_dtask(await(l))   # `light_to_dtask` waits for `l`: bound that wait
        # DTask -> light task, DTask already finished and still running
        d1 = Dagger.@spawn (() -> (sleep(0.05); 20))()
        l1 = @light +(d1, 1)
        @test tfetch(l1) == 21
        d2 = Dagger.@spawn identity(5); wait(d2)
        @test tfetch(@light *(d2, 2)) == 10
        # mixed light + DTask arguments
        @test tfetch(@light +(d1, l1, 100)) == 20 + 21 + 100
        # light task -> DTask (slow direction), finished and unfinished
        l2 = @light (() -> (sleep(0.05); 7))()
        @test tfetch(Dagger.@spawn +(ltd(l2), 1)) == 8
        @test tfetch(ltd(l2)) == 7
        # failures cross the boundary in both directions
        dbad = Dagger.@spawn error("dtask boom")
        @test_throws Exception tfetch(@light identity(dbad))
        lbad = @light error("light boom")
        @test_throws Exception tfetch(ltd(lbad))
        # chain through both kinds, compared with the sequential value
        acc = @light identity(0)
        for i in 1:20
            acc = isodd(i) ? @light(+(acc, i)) : ltd(@light(+(acc, i)))
            acc isa DTask && (acc = @light identity(acc))
        end
        @test tfetch(acc) == sum(1:20)
    end

    @testset "a DTask waits on a light task that depends on a task needing the same processor" begin
        # Everything runs on one processor: the waiting DTask must hand the processor back while it
        # waits, or the task its light task depends on can never start (no matter how many threads).
        Dagger.with_options(; scope=Dagger.scope(worker=1, thread=1)) do
            b  = Dagger.@spawn (() -> (sleep(0.2); 1))()
            d2 = Dagger.@spawn (x -> x + 20)(b)
            l  = @light (x -> 2x)(d2)
            d1 = Dagger.@spawn (l -> fetch(l))(l)
            t = @async fetch(d1)
            @test timedwait(() -> istaskdone(t), 30) === :ok
            istaskdone(t) && @test fetch(t) == 42
        end
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
    @test timedwait(() -> istaskdone(l), 10) === :ok
    @test fetch(l) == (1, nothing)
end
