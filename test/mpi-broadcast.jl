# Rank-local tests of the relay's FIFO and wait queues. No relay is started:
# delivery, heartbeat, and teardown are driven explicitly for determinism.
function test_bcast_state(comm)
    MPIExt.BcastState(comm, Dict{Tuple{Int,UInt32},MPIExt.BcastSlot}(),
                      Threads.Condition(), Threads.Atomic{Bool}(true), nothing)
end

function wait_bcast_consumers(state, n)
    status = timedwait(10.0; pollint=0.001) do
        @lock state.cond begin
            sum(slot -> slot.waiters, values(state.slots); init=0) == n &&
                all(slot -> !isempty(slot.cond), values(state.slots))
        end
    end
    status === :ok || error("Broadcast consumers did not reach their wait queues")
end

function bcast_test_consumer(state, tag, threaded)
    if threaded
        return Threads.@spawn MPIExt.bcast_slot_wait(state, 0, tag)
    else
        return @async MPIExt.bcast_slot_wait(state, 0, tag)
    end
end

@testset "Broadcast wait queues" begin
    @testset "Queued delivery preserves FIFO and root identity" begin
        state = test_bcast_state(MPI.COMM_SELF)
        tag = UInt32(1)
        MPIExt.bcast_deliver!(state, 0, tag, :first)
        MPIExt.bcast_deliver!(state, 0, tag, :second)
        MPIExt.bcast_deliver!(state, 1, tag, :other_root)
        @test MPIExt.bcast_slot_wait(state, 0, tag) === :first
        @test MPIExt.bcast_slot_wait(state, 1, tag) === :other_root
        @test MPIExt.bcast_slot_wait(state, 0, tag) === :second
        @test isempty(state.slots)
    end

    @testset "Delivery wakes only the matching tag (threaded=$threaded)" for threaded in (false, true)
        state = test_bcast_state(MPI.COMM_SELF)
        n = 32
        tasks = [bcast_test_consumer(state, UInt32(i), threaded) for i in 1:n]
        wait_bcast_consumers(state, n)
        @lock state.cond begin
            MPIExt.bcast_deliver!(state, 0, UInt32(1), 1)
            @test isempty(state.slots[(0, UInt32(1))].cond)
            @test all(i -> !isempty(state.slots[(0, UInt32(i))].cond), 2:n)
        end
        @test fetch(first(tasks)) == 1

        # Heartbeats still wake every blocked consumer for deadlock checks,
        # without deleting its empty slot or dropping a subsequent delivery.
        @lock state.cond begin
            MPIExt.bcast_heartbeat!(state)
            @test all(slot -> isempty(slot.cond), values(state.slots))
            @test length(state.slots) == n - 1
        end
        wait_bcast_consumers(state, n - 1)
        for i in 2:n
            MPIExt.bcast_deliver!(state, 0, UInt32(i), i)
        end
        @test fetch.(tasks) == collect(1:n)
        @test isempty(state.slots)
    end

    @testset "Keep a slot while another consumer is still waiting (threaded=$threaded)" for threaded in (false, true)
        state = test_bcast_state(MPI.COMM_SELF)
        tag = UInt32(2)
        tasks = [bcast_test_consumer(state, tag, threaded) for _ in 1:2]
        wait_bcast_consumers(state, 2)
        MPIExt.bcast_deliver!(state, 0, tag, 1)
        @test timedwait(() -> count(istaskdone, tasks) == 1, 10.0; pollint=0.001) === :ok
        @lock state.cond begin
            @test state.slots[(0, tag)].waiters == 1
        end
        MPIExt.bcast_deliver!(state, 0, tag, 2)
        @test sort(fetch.(tasks)) == [1, 2]
        @test isempty(state.slots)
    end

    @testset "Teardown fails every blocked tag (already woken=$woken, threaded=$threaded)" for woken in (false, true), threaded in (false, true)
        state = test_bcast_state(MPI.COMM_SELF)
        tasks = [bcast_test_consumer(state, UInt32(i), threaded) for i in 1:4]
        wait_bcast_consumers(state, 4)
        lock(MPIExt.BCAST_STATES) do states
            @assert !haskey(states, MPI.COMM_SELF)
            states[MPI.COMM_SELF] = state
        end
        @lock state.cond begin
            if woken
                MPIExt.bcast_heartbeat!(state)
                @test all(slot -> isempty(slot.cond), values(state.slots))
            end
            MPIExt.stop_bcast_relay!(MPI.COMM_SELF)
        end
        for task in tasks
            @test_throws TaskFailedException fetch(task)
            @test task.exception isa ConcurrencyViolationError
        end
        @test isempty(state.slots)
    end
end
