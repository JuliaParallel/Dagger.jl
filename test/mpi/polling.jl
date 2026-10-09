# Run separately with an interactive coordinator:
# mpiexec -n 2 julia --project --threads=4,1 test/mpi/polling.jl
using Dagger, MPI, Test

MPI.Init(; threadlevel=:multiple)
const MPIExt = Base.get_extension(Dagger, :MPIExt)
const ni = Threads.threadpoolsize(:interactive)
const nd = Threads.threadpoolsize(:default)
@assert ni > 0 && Threads.threadpool() === :interactive

function pin_sticky!(task, tid)
    task.sticky = true
    ccall(:jl_set_task_tid, Cint, (Any, Cint), task, tid - 1)
    schedule(task)
    return task
end

function send_polling_values(raw, tagbase)
    requests = [raw ? MPI.Isend([i], MPI.COMM_SELF; dest=0, tag=tagbase+i) :
                      MPI.isend(i, MPI.COMM_SELF; dest=0, tag=tagbase+i)
                for i in 1:nd]
    # A blocking Waitall would occupy the thread that a pinned receiver needs
    # to resume on. Exercise the production cooperative request wait too.
    for (i, request) in enumerate(requests)
        MPIExt.__wait_for_request(request, MPI.COMM_SELF, 0, 0, tagbase+i,
                                  "polling test", "send")
    end
end

@testset "MPI polling permits spawned work" begin
    @test (@inferred MPIExt.mpi_poll_pause(0)) == 1

    @testset "$kind receive" for (kind, raw, tagbase) in
            ((:serialized, false, 30000), (:inplace, true, 31000),
             (:request, true, 32000))
        ready = Threads.Atomic{Int}(0)
        receivers = Task[]
        for i in 1:nd
            task = Task() do
                Threads.atomic_add!(ready, 1)
                if raw
                    array = zeros(Int, 1)
                    if kind === :request
                        request = MPI.Irecv!(array, MPI.COMM_SELF; source=0, tag=tagbase+i)
                        MPIExt.__wait_for_request(request, MPI.COMM_SELF, 0, 0,
                                                  tagbase+i, "polling test", "recv")
                    else
                        MPIExt.recv_yield!(array, MPI.COMM_SELF, 0, tagbase+i)
                    end
                    return array[1]
                end
                return MPIExt.recv_yield(MPI.COMM_SELF, 0, tagbase+i)
            end
            push!(receivers, pin_sticky!(task, ni+i))
        end
        @test timedwait(() -> ready[] == nd, 10.0) === :ok
        sleep(0.1)
        cancelled = Threads.Atomic{Bool}(false)
        sender = Threads.@spawn begin
            cancelled[] || send_polling_values(raw, tagbase)
        end
        try
            # Observe before releasing any receiver: release would itself
            # make a thread available and hide a starvation regression.
            @test timedwait(() -> istaskstarted(sender), 10.0) === :ok
        finally
            if !istaskstarted(sender)
                cancelled[] = true
                send_polling_values(raw, tagbase)
            end
            @test fetch.(receivers) == collect(1:nd)
            wait(sender)
        end
    end

    @testset "An idle sticky relay parks its task" begin
        states = MPIExt.BcastState[]
        relays = Task[]
        for i in 1:nd
            state = MPIExt.BcastState(MPI.Comm_dup(MPI.COMM_SELF),
                Dict{Tuple{Int,UInt32},MPIExt.BcastSlot}(), Threads.Condition(),
                Threads.Atomic{Bool}(true), nothing)
            task = Task(() -> MPIExt.bcast_relay_loop(state))
            state.relay = task
            push!(states, state)
            push!(relays, pin_sticky!(task, ni+i))
        end
        try
            @test timedwait(() -> all(istaskstarted, relays), 10.0) === :ok
            sleep(0.1)
            spawned = Threads.@spawn nothing
            progressed = timedwait(() -> istaskstarted(spawned), 10.0) === :ok
            @test progressed
            progressed && wait(spawned)
        finally
            for state in states
                state.running[] = false
            end
            foreach(wait, relays)
            for state in states
                MPI.free(state.bcast_comm)
            end
        end
    end
end
