# Home-based task delegation (`lend=true`, `Dagger.enable_delegation!`): the owner of a ready
# task may lend its *execution* to an idle rank and gets the value (or the exception) back.
# Part 1 tests the claim protocol of the lend pool alone, with real threads. Part 2 runs
# the whole protocol across the ranks of the job.

# A protocol that goes wrong must fail a test in bounded time, not hang the job: if the file has not finished after this
# long, report it and abort every rank.
const deleg_watchdog = Timer(600) do _
    println(stderr, "Test Failed: the delegation tests did not finish in 600 s on rank $rank (the protocol hangs)")
    flush(stderr)
    MPI.Abort(MPI.COMM_WORLD, 1)
end

const LendPool = MPIExt.LendPool
const LendRecord = MPIExt.LendRecord

lend_rec(pool, i) = MPIExt.LendRecord(MPIExt.lend_pool_new_uid!(pool), identity, (i,), (;))

@testset "Delegation: lend pool" begin
    @testset "sequence: oldest runs here, newest is lent" begin
        pool = LendPool(1)
        recs = [lend_rec(pool, i) for i in 1:6]
        @test MPIExt.lend_pool_enter!(pool, recs[1])            # takes the only slot
        @test !any(r -> MPIExt.lend_pool_enter!(pool, r), recs[2:6])
        @test pool.queue == recs[2:6]
        # a thief takes from the new end, skipping what must stay
        recs[6].nolend = true
        @test MPIExt.lend_pool_steal!(pool, 3) === recs[5]
        @test MPIExt.lend_pool_steal!(pool, 3) === recs[4]
        @test recs[5].state == MPIExt.LEND_LENT && recs[5].thief == 3
        # a freed slot goes to the oldest queued task, in order
        MPIExt.lend_pool_release!(pool)
        @test recs[2].state == MPIExt.LEND_LOCAL && recs[2].event.set
        MPIExt.lend_pool_release!(pool)
        @test recs[3].state == MPIExt.LEND_LOCAL
        @test pool.free == 0 && pool.queue == [recs[6]]
        # a refused claim goes back to the old end, behind nothing
        @test MPIExt.lend_pool_requeue!(pool, recs[4].uid)
        @test pool.queue == [recs[4], recs[6]] && recs[4].state == MPIExt.LEND_QUEUED
        # an unknown or already answered uid is ignored
        @test !MPIExt.lend_pool_requeue!(pool, UInt64(10^9))
        @test MPIExt.lend_pool_complete!(pool, UInt64(10^9), false, 1) === nothing
        done = MPIExt.lend_pool_complete!(pool, recs[5].uid, false, 25)
        @test done === recs[5] && done.state == MPIExt.LEND_DONE && done.result == 25
        @test MPIExt.lend_pool_complete!(pool, recs[5].uid, false, 26) === nothing   # one answer only
        # a claim whose slot freed up meanwhile runs at once
        @test MPIExt.lend_pool_steal!(pool, 2) === recs[4]
        MPIExt.lend_pool_release!(pool); MPIExt.lend_pool_release!(pool)   # recs[6] gets one, one slot is free
        @test recs[6].state == MPIExt.LEND_LOCAL && pool.free == 1
        @test MPIExt.lend_pool_requeue!(pool, recs[4].uid)
        @test recs[4].state == MPIExt.LEND_LOCAL && pool.free == 0
    end

    @testset "shutdown fails what is still lent" begin
        pool = LendPool(1)
        a, b = lend_rec(pool, 1), lend_rec(pool, 2)
        MPIExt.lend_pool_enter!(pool, a)
        MPIExt.lend_pool_enter!(pool, b)
        @test MPIExt.lend_pool_steal!(pool, 1) === b
        waiter = Threads.@spawn (wait(b.event); (b.state, b.failed, b.result))
        yield()
        @test MPIExt.lend_pool_fail_lent!(pool, ErrorException("stopped")) == 1
        state, failed, err = fetch(waiter)
        @test state == MPIExt.LEND_DONE && failed && err isa ErrorException
    end

    # The owner of a queued task is cancelled (an InterruptException while it waits): it must neither stay in the
    # queue nor take a slot it will never give back
    @testset "an interrupted owner leaves no trace" begin
        interrupt(w) = (schedule(w, InterruptException(); error=true); try wait(w); false catch; true end)
        # queued: it leaves the queue and the next one gets the slot
        pool = LendPool(1)
        a, b, c = lend_rec(pool, 1), lend_rec(pool, 2), lend_rec(pool, 3)
        @test MPIExt.lend_pool_enter!(pool, a)
        @test !MPIExt.lend_pool_enter!(pool, b) && !MPIExt.lend_pool_enter!(pool, c)
        w = Task(() -> MPIExt.lend_wait!(pool, b)); schedule(w)
        @test timedwait(() -> istaskstarted(w), 10.0) == :ok
        sleep(0.05)                                           # it is waiting for its event now
        @test interrupt(w)
        @test pool.queue == [c]
        MPIExt.lend_pool_release!(pool)
        @test c.state == MPIExt.LEND_LOCAL && pool.free == 0
        MPIExt.lend_pool_release!(pool)
        @test pool.free == 1
        # granted a slot, then interrupted before it used it: the slot is given back
        pool = LendPool(1)
        a, b = lend_rec(pool, 1), lend_rec(pool, 2)
        MPIExt.lend_pool_enter!(pool, a); MPIExt.lend_pool_enter!(pool, b)
        MPIExt.lend_pool_release!(pool)
        @test b.state == MPIExt.LEND_LOCAL && pool.free == 0
        MPIExt.lend_pool_abandon!(pool, b)
        @test pool.free == 1 && isempty(pool.queue)
        # claimed by a thief: the thief's answer, or its refusal, is dropped and nothing is run or queued for it
        pool = LendPool(1)
        a, b = lend_rec(pool, 1), lend_rec(pool, 2)
        MPIExt.lend_pool_enter!(pool, a); MPIExt.lend_pool_enter!(pool, b)
        @test MPIExt.lend_pool_steal!(pool, 1) === b
        MPIExt.lend_pool_abandon!(pool, b)
        @test MPIExt.lend_pool_requeue!(pool, b.uid)           # refused by the thief
        @test isempty(pool.queue) && pool.free == 0 && isempty(pool.lent)
        MPIExt.lend_pool_release!(pool)
        @test pool.free == 1                                   # the slot of `a` is the only one, and it is whole
        # ... or answers it: the answer is taken and forgotten
        pool = LendPool(1)
        a, b = lend_rec(pool, 1), lend_rec(pool, 2)
        MPIExt.lend_pool_enter!(pool, a); MPIExt.lend_pool_enter!(pool, b)
        MPIExt.lend_pool_steal!(pool, 1); MPIExt.lend_pool_abandon!(pool, b)
        @test MPIExt.lend_pool_complete!(pool, b.uid, false, 4) === b
        @test isempty(pool.lent) && isempty(pool.queue) && pool.free == 0
    end

    # Owners wait, local slots are released, thieves steal and answer, all at once, with a tiny
    # capacity so the pool is always at its boundary. Every owner must get *its own* answer
    # exactly once: counts alone would not see an answer delivered to the wrong task.
    @testset "claim race with real threads (capacity $cap)" for cap in (1, 2)
        pool = LendPool(cap)
        n = 400
        ran_local = Threads.Atomic{Int}(0)
        ran_lent = Threads.Atomic{Int}(0)
        stop = Threads.Atomic{Bool}(false)
        thieves = [Threads.@spawn begin
            did = 0
            while !stop[]
                rec = MPIExt.lend_pool_steal!(pool, 1)
                if rec === nothing
                    sleep(0.0001)
                    continue
                end
                # sometimes hand it back, as a thief without room does
                if did % 5 == 4
                    MPIExt.lend_pool_requeue!(pool, rec.uid)
                else
                    MPIExt.lend_pool_complete!(pool, rec.uid, false, rec.args[1]^2)
                end
                did += 1
            end
        end for _ in 1:2]
        function run_owner(i)
            rec = lend_rec(pool, i)
            if MPIExt.lend_pool_enter!(pool, rec) || (wait(rec.event); rec.state == MPIExt.LEND_LOCAL)
                Threads.atomic_add!(ran_local, 1)
                sleep(0.0001)
                MPIExt.lend_pool_release!(pool)
                return i^2
            end
            Threads.atomic_add!(ran_lent, 1)
            return rec.result
        end
        results = fetch.([Threads.@spawn(run_owner(i)) for i in 1:n])
        stop[] = true
        foreach(wait, thieves)
        @test results == [i^2 for i in 1:n]
        @test ran_local[] + ran_lent[] == n
        @test ran_lent[] > 0 && ran_local[] > 0
        @test pool.free == cap && isempty(pool.queue) && isempty(pool.lent)
    end
end

# --- across ranks ----------------------------------------------------------------------

deleg_busy(i::Int, secs::Float64) = (sleep(secs); (i * i, MPI.Comm_rank(MPI.COMM_WORLD)))
deleg_boom(i::Int, secs::Float64) = (sleep(secs); error("boom $i from rank $(MPI.Comm_rank(MPI.COMM_WORLD))"))
deleg_where(x) = (MPI.Comm_rank(MPI.COMM_WORLD), Threads.threadid(), x)
deleg_plain(i::Int) = i + 1
deleg_chunk_busy(c, i::Int, secs::Float64) = deleg_busy(i, secs)
deleg_chunk_boom(c, i::Int, secs::Float64) = deleg_boom(i, secs)
const DELEG_SV = MPIExt.ScopedValue(0)
deleg_chunk_sv(c, i::Int, secs::Float64) = (sleep(secs); (DELEG_SV[], MPI.Comm_rank(MPI.COMM_WORLD)))
deleg_sum_busy(c, v::Vector{Float64}, i::Int, secs::Float64) = (sleep(secs); sum(v) + i)

function deleg_counters()
    st = Dagger.delegation_stats()
    return [st.lent, st.stolen, st.local_, st.returned, st.failed]
end
# sum of a counter vector over the ranks (no tasks are in flight when this is called)
deleg_total(v) = MPI.Allreduce(v, +, comm)

@testset "Delegation: across ranks" begin
    @test Dagger.delegation_stats() === nothing          # nothing is enabled by default
    # lend=true without delegation enabled is plain execution
    @test fetch(Dagger.@spawn lend=true deleg_plain(1)) == 2
    @test Dagger.delegation_stats() === nothing
    MPI.Barrier(comm)

    Dagger.enable_delegation!(slots=2)
    @test Dagger.delegation_stats() == (lent=0, failed=0, returned=0, stolen=0, local_=0)
    owner_rank = 0
    # every task prefers the owner of its chunk argument, so rank 0 is flooded while the others idle
    chunk = Dagger.tochunk(0, proc_for_rank(owner_rank), space_for_rank(owner_rank))

    @testset "which tasks may be lent" begin
        proc = proc_for_rank(rank)
        lendable(; kw...) = MPIExt.mpi_lend_state(proc, deleg_plain, Dagger.Options(; kw...)) !== nothing
        @test lendable(lend=true)
        @test lendable(lend=true, scope=Dagger.AnyScope())
        @test !lendable()                                       # not asked for
        @test !lendable(lend=false)
        @test !lendable(lend=true, scope=Dagger.ExactScope(proc))      # names a processor
        @test !lendable(lend=true, scope=Dagger.scope(mpi_rank=rank))
        @test !lendable(lend=true, compute_scope=Dagger.ExactScope(proc))
        @test !lendable(lend=true, tag=UInt32(7))               # a Datadeps task
        @test MPIExt.mpi_lend_state(proc, Dagger.move!, Dagger.Options(lend=true)) === nothing
        # arguments that are still handles are never shipped
        @test MPIExt.mpi_lend_plain_arg(1.0) && MPIExt.mpi_lend_plain_arg([1, 2]) && MPIExt.mpi_lend_plain_arg("x")
        @test !MPIExt.mpi_lend_plain_arg(chunk)
        @test !MPIExt.mpi_lend_plain_arg(Dagger.@spawn(deleg_plain(1)))
        # a second call while enabled does nothing; bad settings are refused before anything collective happens
        st1 = MPIExt.lend_state_for(comm)
        Dagger.enable_delegation!(slots=5, max_bytes=1)
        @test MPIExt.lend_state_for(comm) === st1 && st1.pool.capacity == 2 && st1.max_bytes == typemax(Int)
        @test_throws ArgumentError Dagger.enable_delegation!(slots=0)
        # the service is a migratable task that did not inherit the scope of whoever enabled delegation
        MPI.Barrier(comm)
        Dagger.disable_delegation!()
        MPIExt.with(DELEG_SV => 42) do
            Dagger.enable_delegation!(slots=2)
        end
        svc = MPIExt.lend_state_for(comm).relay
        @test !svc.sticky
        MPI.Barrier(comm)
        if nranks > 1
            # a task run for another rank sees none of the dynamic scope that was set when delegation was enabled
            seen = fetch.([Dagger.@spawn lend=true deleg_chunk_sv(chunk, i, 0.08) for i in 1:16])
            @test all(r -> r[1] == 0, seen)
            @test any(r -> r[2] != owner_rank, seen)
        end
        MPI.Barrier(comm)
        # and with delegation off nothing is lent whatever the task says
        Dagger.disable_delegation!()
        @test !lendable(lend=true)
        MPI.Barrier(comm)
        Dagger.enable_delegation!(slots=2)
    end

    @testset "results of lent tasks" begin
        n = 24
        tasks = [Dagger.@spawn lend=true deleg_busy(i, 0.05) for i in 1:n]
        res = fetch.(tasks)
        @test first.(res) == [i * i for i in 1:n]
        @test all(r -> 0 <= last(r) < nranks, res)
        MPI.Barrier(comm)
        c = deleg_counters()
        tot = deleg_total(c)
        # everything lendable ran somewhere, either at home or on a thief: nothing twice, nothing lost
        @test tot[3] + tot[1] == n       # `local_` ran at home, `lent` were answered by a thief
        @test tot[1] == tot[2]           # every answer is the run of exactly one thief
    end

    @testset "flooded rank lends to idle ranks" begin
        if nranks > 1
            before = deleg_total(deleg_counters())
            n = 24
            tasks = [Dagger.@spawn lend=true deleg_chunk_busy(chunk, i, 0.08) for i in 1:n]
            res = fetch.(tasks)
            @test first.(res) == [i * i for i in 1:n]
            MPI.Barrier(comm)
            after = deleg_total(deleg_counters())
            @test all(fetch(t; raw=true).handle.rank == owner_rank for t in tasks)   # the owner never changed
            @test after[1] - before[1] > 0                  # some were lent
            @test after[1] - before[1] == after[2] - before[2]     # lent == run by thieves
            @test after[3] - before[3] + after[1] - before[1] == n   # each task ran once
            ranks_that_ran = unique(last.(res))
            @test length(ranks_that_ran) > 1                # and really ran elsewhere
        end
    end

    @testset "exceptions come back" begin
        before = deleg_total(deleg_counters())
        n = 12
        tasks = [Dagger.@spawn lend=true deleg_boom(i, 0.05) for i in 1:n]
        msgs = String[]
        for (i, t) in enumerate(tasks)
            err = try
                fetch(t)
                nothing
            catch e
                e
            end
            @test err !== nothing
            push!(msgs, sprint(showerror, err))
            @test occursin("boom $i from rank", msgs[end])
        end
        # the failures that were lent reached the owner as failures, not as a hang
        MPI.Barrier(comm)
        after = deleg_total(deleg_counters())
        @test after[5] - before[5] <= after[1] - before[1]
        if nranks > 1
            # a flooded rank: some failures are lent, run by another rank, and come back as the same error to the owner
            before = deleg_total(deleg_counters())
            ftasks = [Dagger.@spawn lend=true deleg_chunk_boom(chunk, i, 0.08) for i in 1:16]
            errs = Any[try fetch(t); nothing catch e; e end for t in ftasks]
            @test all(e -> e !== nothing, errs)
            msgs = [sprint(showerror, e) for e in errs]
            @test all(i -> occursin("boom $i from rank", msgs[i]), 1:16)
            ran_on = [parse(Int, match(r"from rank (\d+)", m).captures[1]) for m in msgs]
            @test any(!=(owner_rank), ran_on)                   # a thief ran some and its error reached the owner
            MPI.Barrier(comm)
            after = deleg_total(deleg_counters())
            @test after[5] - before[5] == count(!=(owner_rank), ran_on)    # counted as lent and failed, once each
            @test after[5] - before[5] == after[1] - before[1]            # every lent task of this batch failed
        end
        # and the runtime is still usable
        @test fetch(Dagger.@spawn lend=true deleg_busy(3, 0.01))[1] == 9
    end

    @testset "mixed pinned and free tasks" begin
        cpus = filter(p -> p.innerProc isa Dagger.ThreadProc, mpi_procs())
        target = last(cpus)
        MPI.Barrier(comm)
        before = deleg_counters()
        pinned = [Dagger.@spawn(scope=Dagger.ExactScope(target), lend=true, deleg_where(i)) for i in 1:8]
        plain = [Dagger.@spawn deleg_plain(i) for i in 1:16]
        for (i, t) in enumerate(pinned)
            r = fetch(t)
            # a task that names its processor is never lent
            @test r == (target.rank, target.innerProc.tid, i)
        end
        @test fetch.(plain) == [i + 1 for i in 1:16]
        MPI.Barrier(comm)
        # neither pinned nor plain tasks pass through the pool: enabling delegation changes nothing for them
        @test deleg_counters() == before
        free = [Dagger.@spawn lend=true deleg_busy(i, 0.03) for i in 1:16]
        @test first.(fetch.(free)) == [i * i for i in 1:16]
    end

    @testset "size cap" begin
        # tasks whose arguments are larger than the cap are never lent; small ones still are
        MPI.Barrier(comm)
        Dagger.disable_delegation!()
        Dagger.enable_delegation!(slots=2, max_bytes=100_000)
        MPI.Barrier(comm)
        big = fill(2.0, 50_000)                                   # 400 KB
        b0 = deleg_counters()
        capped = [Dagger.@spawn lend=true deleg_sum_busy(chunk, big, i, 0.04) for i in 1:12]
        @test fetch.(capped) == [100_000.0 + i for i in 1:12]
        MPI.Barrier(comm)
        @test deleg_total(deleg_counters())[1] - deleg_total(b0)[1] == 0
        small = [Dagger.@spawn lend=true deleg_busy(i, 0.04) for i in 1:12]
        @test first.(fetch.(small)) == [i * i for i in 1:12]
        MPI.Barrier(comm)
        Dagger.disable_delegation!()
        Dagger.enable_delegation!(slots=2)
    end

    @testset "together with the rank gate" begin
        # the owner's local runs go through the gate, a lent task through the thief's lend slot: both limits hold
        # and nothing is lost or run twice
        MPI.Barrier(comm)
        try
            Dagger.mpi_rank_gate!(true; cap=max(Threads.nthreads(:default), 2), reserve=1)
            before = deleg_total(deleg_counters())
            n = 24
            res = fetch.([Dagger.@spawn lend=true deleg_chunk_busy(chunk, i, 0.05) for i in 1:n])
            @test first.(res) == [i * i for i in 1:n]
            errs = Any[try fetch(t); nothing catch e; e end for t in [Dagger.@spawn lend=true deleg_chunk_boom(chunk, i, 0.03) for i in 1:8]]
            @test all(!isnothing, errs)
            @test all(i -> occursin("boom $i from rank", sprint(showerror, errs[i])), 1:8)
            urgent = fetch(Dagger.@spawn priority=1 lend=true deleg_plain(5))
            @test urgent == 6
            MPI.Barrier(comm)
            after = deleg_total(deleg_counters())
            @test after[3] - before[3] + after[1] - before[1] == n + 9      # each lendable task ran once (n results, 8 failures, 1 urgent)
            @test after[1] - before[1] == after[2] - before[2]
            g = MPIExt.MPI_RANK_GATE[]
            @test g.running == 0 && isempty(g.waiters)                       # every slot is back
        finally
            Dagger.mpi_rank_gate!(false)
        end
        MPI.Barrier(comm)
    end

    @testset "stopping fails a task that is still lent" begin
        # a record claimed by a thief that never answers: stopping must fail it, not leave its owner waiting
        MPI.Barrier(comm)
        st = MPIExt.lend_state_for(comm)
        rec = MPIExt.LendRecord(MPIExt.lend_pool_new_uid!(st.pool), identity, (1,), (;))
        @lock st.pool.lock begin
            rec.state = MPIExt.LEND_LENT
            st.pool.lent[rec.uid] = rec
        end
        waiter = Threads.@spawn (wait(rec.event); (rec.state, rec.failed, rec.result))
        MPI.Barrier(comm)
        Dagger.disable_delegation!()
        @test timedwait(() -> istaskdone(waiter), 20.0) == :ok
        if istaskdone(waiter)
            state, failed, err = fetch(waiter)
            @test state == MPIExt.LEND_DONE && failed && err isa ErrorException
        end
        Dagger.enable_delegation!(slots=2)
        MPI.Barrier(comm)
    end

    @testset "shutdown with an open request" begin
        # every idle rank holds a request at its peers; stopping must not hang or fail anything
        sleep(0.2)
        MPI.Barrier(comm)
        st = MPIExt.lend_state_for(comm)
        @test st !== nothing
        Dagger.disable_delegation!()
        @test Dagger.delegation_stats() === nothing
        # the communicator the lending traffic used is freed, the service is gone, and nothing is left registered
        @test st.comm == MPI.COMM_NULL
        @test istaskdone(st.relay)
        @test isempty(st.pending) && !MPIExt.LEND_ENABLED[]
        @test fetch(Dagger.@spawn lend=true deleg_plain(41)) == 42
        # and it can be switched on again
        Dagger.enable_delegation!(slots=2)
        res = fetch.([Dagger.@spawn lend=true deleg_busy(i, 0.02) for i in 1:8])
        @test first.(res) == [i * i for i in 1:8]
        MPI.Barrier(comm)
        Dagger.disable_delegation!()
    end
end
close(deleg_watchdog)
