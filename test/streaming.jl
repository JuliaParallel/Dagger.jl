@everywhere function rand_finite()
    x = rand()
    if x < 0.1
        return Dagger.finish_stream(x)
    end
    return x
end

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

function test_finishes(f, message::String; ignore_timeout=false)
    t = @eval Threads.@spawn @testset $message catch_interrupt($f)
    if timedwait(()->istaskdone(t), 20) == :timed_out
        if !ignore_timeout
            @warn "Testing task timed out: $message"
        end
        Dagger.cancel!(;halt_sch=true, force=true)
        fetch(Dagger.@spawn 1+1)
        return false
    end
    return true
end

@testset "Basics" begin
    @test test_finishes("Single task") do
        local x
        Dagger.spawn_streaming() do
            x = Dagger.@spawn rand_finite()
        end
        @test fetch(x) === nothing
    end

    @test !test_finishes("Single task running forever"; ignore_timeout=true) do
        local x
        Dagger.spawn_streaming() do
            x = Dagger.spawn() do
                y = rand()
                sleep(1)
                return y
            end
        end
        fetch(x)
    end

    @test test_finishes("Max evaluations") do
        counter = 0
        function incrementer()
            counter += 1
        end

        x = Dagger.with_options(; stream_max_evals=10) do
            Dagger.spawn_streaming() do
                Dagger.@spawn incrementer()
            end
        end
        wait(x)
        @test counter == 10
    end

    @test test_finishes("Two tasks (sequential)") do
        local x, y
        @warn "\n\n\nStart streaming\n\n\n"
        Dagger.spawn_streaming() do
            x = Dagger.@spawn rand_finite()
            y = Dagger.@spawn x+1
        end
        @test fetch(x) === nothing
        @test_throws Dagger.DTaskFailedException fetch(y)
    end

    # TODO: Two tasks (parallel)

    # TODO: Three tasks (2 -> 1) and (1 -> 2)
    # TODO: Four tasks (diamond)

    # TODO: With pass-through/Without result
    # TODO: With pass-through/With result
    # TODO: Without pass-through/Without result

    @test test_finishes("Without pass-through/With result") do
        local x
        Dagger.spawn_streaming() do
            x = Dagger.spawn() do
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
# TODO: Custom stream buffers/buffer amounts
# TODO: Cross-worker streaming
# TODO: Different stream element types (immutable and mutable)

# TODO: Zero-allocation examples
# FIXME: Streaming across threads
