# The rank gate (`Dagger.mpi_rank_gate!`): a concurrency limit and priorities for the tasks a rank runs on its threads.

# A gate that misbehaves must fail a test in bounded time, not hang the job: if the file has not finished after this
# long, report it and abort every rank.
const rg_watchdog = Timer(300) do _
    println(stderr, "Test Failed: the rank gate tests did not finish in 300 s on rank $rank (a gate hangs)")
    flush(stderr)
    MPI.Abort(MPI.COMM_WORLD, 1)
end

rg_active = Threads.Atomic{Int}(0)
rg_max = Threads.Atomic{Int}(0)
rg_done = Threads.Atomic{Int}(0)
function rg_tile(i::Int, ms::Float64)
    a = Threads.atomic_add!(rg_active, 1) + 1
    Threads.atomic_max!(rg_max, a)
    t0 = time_ns(); while (time_ns() - t0) < ms * 1e6; end      # a kernel: occupies its thread, never yields
    Threads.atomic_sub!(rg_active, 1)
    Threads.atomic_add!(rg_done, 1)
    return i
end
rg_urgent(x::Int) = rg_done[]                # tiles finished when the urgent task starts, read inside the task
rg_maxseen() = rg_max[]

function rg_run(sc, nthr, n, ms)
    Threads.atomic_xchg!(rg_done, 0)           # only meaningful on the rank that runs the tiles
    tiles = [Dagger.@spawn(scope=sc, return_type=Int, rg_tile(i, ms)) for i in 1:n]
    sleep(0.02)                                # the tiles are queued; now an urgent one
    u = Dagger.@spawn scope=sc return_type=Int priority=1 rg_urgent(1)
    return tiles, fetch(u)
end

# The gate alone, without tasks or ranks, so these run at any thread count
@testset "Rank gate: switch and defaults" begin
    @test MPIExt.MPI_RANK_GATE[] === nothing                    # off by default
    try
        @test Dagger.mpi_rank_gate!(true) == true
        g = MPIExt.MPI_RANK_GATE[]
        @test g.cap == Threads.nthreads(:default) && g.reserve == min(1, max(0, g.cap - 1))
        Dagger.mpi_rank_gate!(true; cap=4, reserve=1)
        @test (MPIExt.MPI_RANK_GATE[].cap, MPIExt.MPI_RANK_GATE[].reserve) == (4, 1)
        Dagger.mpi_rank_gate!(true; cap=1, reserve=5)          # a reserve that would leave nothing for ordinary tasks
        @test (MPIExt.MPI_RANK_GATE[].cap, MPIExt.MPI_RANK_GATE[].reserve) == (1, 0)
        @test Dagger.mpi_rank_gate!(false) == false
        @test MPIExt.MPI_RANK_GATE[] === nothing
    finally
        Dagger.mpi_rank_gate!(false)
    end
end

# wait until `n` tasks are queued at the gate
rg_waiting(g, n) = timedwait(() -> @lock(g.lock, length(g.waiters)) == n, 10.0) == :ok

@testset "Rank gate: limit and reserved slot" begin
    g = MPIExt.RankGate(4, 1)
    for _ in 1:3
        MPIExt.gate_acquire!(g, 0)                                 # cap - reserve = 3 ordinary tasks run at once
    end
    @test g.running == 3 && isempty(g.waiters)
    ordinary = Threads.@spawn (MPIExt.gate_acquire!(g, 0); :got)
    @test rg_waiting(g, 1)
    @test g.running == 3 && !istaskdone(ordinary)                  # the fourth ordinary task waits, holding no slot
    MPIExt.gate_acquire!(g, 1)                                     # an urgent task may use the reserved slot
    @test g.running == 4 && length(g.waiters) == 1
    urgent2 = Threads.@spawn (MPIExt.gate_acquire!(g, 1); :got)    # but there is no slot beyond cap
    @test rg_waiting(g, 2)
    MPIExt.gate_release!(g)                                        # running 3: only the urgent one may take it, not the ordinary one
    @test timedwait(() -> istaskdone(urgent2), 10.0) == :ok
    @test fetch(urgent2) == :got && !istaskdone(ordinary) && g.running == 4
    MPIExt.gate_release!(g)                                        # running 3: the ordinary task still has no slot
    sleep(0.05)
    @test !istaskdone(ordinary) && g.running == 3
    MPIExt.gate_release!(g)                                        # running 2: now it does
    @test timedwait(() -> istaskdone(ordinary), 10.0) == :ok
    @test fetch(ordinary) == :got && g.running == 3
    foreach(_ -> MPIExt.gate_release!(g), 1:3)
    @test g.running == 0 && isempty(g.waiters)
end

@testset "Rank gate: waiters get slots by priority, then arrival" begin
    g = MPIExt.RankGate(1, 0)
    MPIExt.gate_acquire!(g, 0)                                     # the only slot is taken
    order = Int[]
    lk = ReentrantLock()
    prios = [0, 0, 5, 0, 5, -3]
    ts = Task[]
    for (i, pr) in enumerate(prios)
        t = Threads.@spawn begin
            MPIExt.gate_acquire!(g, pr)
            @lock lk push!(order, i)
            MPIExt.gate_release!(g)
        end
        push!(ts, t)
        @test rg_waiting(g, i)                                     # arrival order is fixed: one after the other
    end
    MPIExt.gate_release!(g)                                        # start the chain: every task releases for the next
    @test timedwait(() -> all(istaskdone, ts), 10.0) == :ok
    @test order == [3, 5, 1, 2, 4, 6]                              # priority 5 first (3 before 5), then 0 in arrival order, then -3
    @test g.running == 0 && isempty(g.waiters)
end

@testset "Rank gate" begin
    nthr = Threads.nthreads(:default)
    if nranks > 1 && nthr >= 3
        Dagger.check_uniformity!(false)
        sc = Dagger.scope(mpi_rank=nranks - 1)
        n = 8 * nthr
        ms = 30.0
        try
            Dagger.mpi_rank_gate!(true; cap=nthr, reserve=1)
            # warm up with the same shape: the first call of each path compiles for a while
            wt, _ = rg_run(sc, nthr, 2 * nthr, 2.0)
            foreach(fetch, wt)
            fetch(Dagger.@spawn(scope=sc, return_type=Int, rg_maxseen()))
            Threads.atomic_xchg!(rg_max, 0)
            tiles, finished = rg_run(sc, nthr, n, ms)    # taken before the first `@test` compiles
            # the urgent task started long before the backlog was done
            @test finished < n ÷ 2
            @test fetch.(tiles) == collect(1:n)
            # at most cap - reserve ordinary tasks ran at once, on the rank that owns them
            mx = fetch(Dagger.@spawn(scope=sc, return_type=Int, rg_maxseen()))
            @test mx <= nthr - 1
            @test mx >= min(2, nthr - 1)                 # and the gate did let several run together
        finally
            Dagger.mpi_rank_gate!(false)
            Dagger.check_uniformity!(true)
        end
    else
        @test_skip false
    end
    MPI.Barrier(comm)
end

# A task cancelled while it waits for a slot must not keep the slot it was granted, nor stay in the queue
@testset "Rank gate: an interrupted waiter loses nothing" begin
    g = MPIExt.RankGate(2, 0)
    MPIExt.gate_acquire!(g, 0); MPIExt.gate_acquire!(g, 0)      # both slots taken
    waiter = Task(() -> MPIExt.gate_acquire!(g, 0))
    schedule(waiter)
    while isempty(g.waiters) && !istaskdone(waiter); sleep(0.001); end
    schedule(waiter, InterruptException(); error=true)
    @test (try wait(waiter); false catch; true end)
    @test isempty(g.waiters)
    @test g.running == 2
    # a release now finds nobody to grant the slot to, and the gate is whole again
    MPIExt.gate_release!(g); MPIExt.gate_release!(g)
    @test g.running == 0
end

# ... and one that was granted a slot at the moment it was interrupted gives that slot back (the grant happens while the
# waiter is on its way out of `wait`: the test holds the lock the waiter needs, and grants while it is blocked on it)
@testset "Rank gate: an interrupted waiter that was granted a slot returns it" begin
    g = MPIExt.RankGate(2, 0)
    MPIExt.gate_acquire!(g, 0); MPIExt.gate_acquire!(g, 0)
    waiter = Task(() -> MPIExt.gate_acquire!(g, 0))
    schedule(waiter)
    @test rg_waiting(g, 1)
    lock(g.lock)
    try
        schedule(waiter, InterruptException(); error=true)
        sleep(0.2)                                              # the waiter has left its wait and blocks on the lock we hold
        MPIExt.gate_release!(g)                                 # frees a slot and grants it to the waiter
        @test g.running == 2 && isempty(g.waiters)
    finally
        unlock(g.lock)
    end
    @test (try wait(waiter); false catch; true end)
    @test g.running == 1 && isempty(g.waiters)                 # the granted slot came back
    MPIExt.gate_release!(g)
    @test g.running == 0
end

# `execute!` has three paths (result type known and the call cannot throw / known but it may throw / not known); every
# one of them runs the task's function through the gate. With every slot of this rank held, none may start.
rg_nothrow(x::Int) = x + 1
rg_maythrow(x::Int) = x < 0 ? error("negative") : x + 1
rg_unstable(x::Int) = x % 2 == 0 ? x : Float64(x)
@testset "Rank gate: every way of executing a task goes through it" begin
    Dagger.check_uniformity!(false)
    try
        sc = Dagger.scope(mpi_rank=nranks - 1)
        proc = proc_for_rank(rank)
        kinds = ((rg_nothrow, (false, true)), (rg_maythrow, (false, false)), (rg_unstable, (true, false)))
        for (f, expected) in kinds
            plan = MPIExt.mpi_execute_bcast_plan(f, (4,), proc)
            @test (plan.need_type_bcast, plan.nothrow) == expected
            Dagger.mpi_rank_gate!(true; cap=2, reserve=0)
            g = MPIExt.MPI_RANK_GATE[]
            MPIExt.gate_acquire!(g, 0); MPIExt.gate_acquire!(g, 0)    # every slot of this rank is taken
            t = Dagger.@spawn scope=sc f(4)
            sleep(0.4)
            # On the rank that runs the task it waits at the gate. A rank that only takes part may be done at once: for a task whose
            # result type is known and that cannot throw, no message is exchanged, so only the executing rank is asserted here.
            rank == nranks - 1 && @test !isready(t)
            @test g.running == 2
            MPIExt.gate_release!(g); MPIExt.gate_release!(g)
            @test timedwait(() -> isready(t), 30.0) == :ok
            @test fetch(t) == f(4)
            @test g.running == 0 && isempty(g.waiters)
        end
    finally
        Dagger.mpi_rank_gate!(false)
        Dagger.check_uniformity!(true)
    end
    MPI.Barrier(comm)
end

# A task that waits for another task inside its function must not starve it of gate slots: the waiters below hold
# every slot, and the task that releases them can only run if they give their slots back while they wait
const RG_FLAG = Threads.Atomic{Bool}(false)
rg_waiter(i::Int) = (Dagger.Sch.thunk_yield() do
    while !RG_FLAG[]; sleep(0.005); end
end; i)
rg_release() = (RG_FLAG[] = true; 0)
@testset "Rank gate: waiting inside a task gives the slot back" begin
    Dagger.check_uniformity!(false)
    try
        Dagger.mpi_rank_gate!(true; cap=2, reserve=0)      # fewer slots than waiters
        sc = Dagger.scope(mpi_rank=nranks - 1)
        Threads.atomic_xchg!(RG_FLAG, false)
        ws = [Dagger.@spawn(scope=sc, return_type=Int, rg_waiter(i)) for i in 1:5]
        sleep(0.3)
        r = Dagger.@spawn scope=sc return_type=Int rg_release()
        done = timedwait(() -> isready(r) && all(isready, ws), 60.0)
        @test done == :ok
        done == :ok && @test fetch.(ws) == collect(1:5)
        # the slots are all back afterwards
        g = MPIExt.MPI_RANK_GATE[]
        @test g.running == 0 && isempty(g.waiters)
    finally
        Dagger.mpi_rank_gate!(false)
        Dagger.check_uniformity!(true)
    end
    MPI.Barrier(comm)
end
close(rg_watchdog)
