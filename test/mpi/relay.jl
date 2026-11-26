# The broadcast relay must keep forwarding while the driver thread is busy. A relay that shares the
# driver's thread stalls behind a driver parked in a blocking call (a long non-yielding computation,
# an `MPI` collective), and the ranks that wait for what it forwards wait with it.
relay_union_value() = rand() < 2 ? 1 : "never"      # non-concrete result type: broadcast to every rank
relay_stamp(x) = time()
relay_hog(seconds::Float64) = (t0 = time(); while time() - t0 < seconds; end; nothing)

@testset "Broadcast relay is independent of the driver thread" begin
    cpus = filter(p -> p.innerProc isa Dagger.ThreadProc, mpi_procs())
    pin(r) = Dagger.ExactScope(last(filter(p -> p.rank == r, cpus)))
    # A driver that has scheduled an `@async` task (Distributed's transport does) is thread-bound for
    # good, and a `Task` it creates starts on its thread even with `sticky = false`. Restart the relay
    # from such a driver, as a session that used Distributed first would have started it.
    wait(@async nothing)
    @test Base.current_task().sticky
    MPI.Barrier(comm)
    MPIExt.stop_bcast_relay!(accel.comm)
    MPIExt.start_bcast_relay!(accel.comm)
    MPI.Barrier(comm)
    # Whatever its placement, it forwards broadcasts
    w = Dagger.@spawn scope=pin(0) relay_union_value()
    @test fetch(Dagger.@spawn scope=pin(nranks - 1) relay_stamp(w)) isa Float64
    MPI.Barrier(comm)
    if nranks >= 2 && Threads.nthreads(:default) >= 4
        # The last rank's driver is busy for 1.5 s without yielding. Rank 0 produces a value whose type
        # is broadcast, and a task on the last rank consumes it: it can only start once that rank's relay
        # has delivered the value, so its start time measures the relay, not the driver.
        t0 = time()
        x = Dagger.@spawn scope=pin(0) relay_union_value()
        s = Dagger.@spawn scope=pin(nranks - 1) relay_stamp(x)
        if rank == nranks - 1
            sleep(0.2)           # let the tasks be planned
            relay_hog(1.5)
        end
        started = fetch(s)
        # The hog still has well over a second to run when the task starts
        rank == nranks - 1 || @test started - t0 < 1.0
        MPI.Barrier(comm)
    end
end
