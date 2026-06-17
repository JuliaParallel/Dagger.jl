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
        end
        Dagger.cancel!(;halt_sch=true, graceful=false)
        @everywhere GC.gc()
        fetch(Dagger.@spawn 1+1)
    end

    tset = fetch(t)::Test.DefaultTestSet
    merge_testset!(tset)
    return !timed_out
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
        @test test_finishes("teardown=true"; max_evals=1_000_000, ignore_timeout=true) do
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
            sleep(1) # Wait for teardown
            @test istaskdone(x)
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
