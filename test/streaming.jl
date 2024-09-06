import MemPool: access_ref

@everywhere begin
    """
    A functor to produce a certain number of outputs.

    Note: always use this like `Dagger.spawn(Producer())` rather than
    `Dagger.@spawn Producer()`. The macro form will just create fresh objects
    every time and stream forever.
    """
    mutable struct Producer
        N::Union{Int, Float64}
        count::Int
        mailbox::Union{RemoteChannel, Nothing}

        Producer(N=5, mailbox=nothing) = new(N, 0, mailbox)
    end

    function (self::Producer)()
        self.count += 1

        # Sleeping will make the loop yield (handy for single-threaded
        # processes), and stops Dagger from being too spammy in debug mode.
        if self.N == Inf
            sleep(0.1)
        end

        # Check if there are any instructions for us
        if !isnothing(self.mailbox) && isready(self.mailbox)
            msg = take!(self.mailbox)
            if msg === :exit
                put!(self.mailbox, self.count)
                return Dagger.finish_stream(self.count)
            else
                error("Unrecognized Producer message: $msg")
            end
        end

        self.count >= self.N ? Dagger.finish_stream(self.count) : self.count
    end
end

function test_in_task(f, message, parent_testsets)
    task_local_storage(:__BASETESTNEXT__, parent_testsets)

    @testset "$message" begin
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
end

function test_finishes(f, message::String; ignore_timeout=false)
    # We sneakily pass a magic variable from the current TLS into the new
    # task. It's used by the Test stdlib to hold a list of the current
    # testsets, so we need it to be able to record the tests from the new
    # task in the original testset that we're currently running under.
    parent_testsets = get(task_local_storage(), :__BASETESTNEXT__, [])
    t = Threads.@spawn test_in_task(f, message, parent_testsets)

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
    master_scope = Dagger.scope(worker=myid())

    @test test_finishes("Migration") do
        if nprocs() == 1
            @warn "Skipping migration test because it requires at least 1 extra worker"
            return
        end

        # Start streaming locally
        mailbox = RemoteChannel()
        producer = Producer(Inf, mailbox)
        x = Dagger.spawn_streaming() do
            Dagger.spawn(producer, Dagger.Options(; scope=master_scope))
        end

        # Wait for the stream to get started
        while producer.count < 2
            sleep(0.1)
        end

        # Migrate to another worker
        access_ref(x.thunk_ref) do thunk
            access_ref(thunk.f.handle) do streaming_function
                Dagger.migrate_stream!(streaming_function.stream, workers()[1])
            end
        end

        # Wait a bit for the stream to get started again on the other node
        sleep(0.5)

        # Stop it
        put!(mailbox, :exit)
        fetch(x)

        final_count = take!(mailbox)
        @info "Counts:" producer.count final_count
    end

    return

    @test test_finishes("Single task") do
        local x
        Dagger.spawn_streaming() do
            x = Dagger.spawn(Producer())
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
        producer = Producer(20)
        x = Dagger.with_options(; stream_max_evals=10) do
            Dagger.spawn_streaming() do
                # Spawn on the same node so we can access the local `producer` variable
                Dagger.spawn(producer, Dagger.Options(; scope=master_scope))
            end
        end

        wait(x)
        @test producer.count == 10
    end

    @test test_finishes("Two tasks (sequential)") do
        local x, y
        @warn "\n\n\nStart streaming\n\n\n"
        Dagger.spawn_streaming() do
            x = Dagger.spawn(Producer())
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
