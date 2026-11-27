using Dagger, Test, Distributed, LinearAlgebra, Random
import Dagger: ThreadProc, Processor
import Dagger.Sch: COMPAT_PROCS_CACHE

# With workers, unscoped tasks may run anywhere (and take the general path), so the code the tasks
# run exists everywhere; what the fast path decides is checked where it can be seen, on one process.
@everywhere begin
    using Dagger
    struct NoThreadFn end
    (::NoThreadFn)(x) = x
    Dagger.iscompatible_func(::Dagger.ThreadProc, opts, ::Type{NoThreadFn}) = false
    FP_TINY(x) = x + 1
    struct NoThreadArg end
    Dagger.iscompatible_arg(::Dagger.ThreadProc, opts, ::Type{NoThreadArg}) = false
    fp_proc() = (sleep(0.002); Dagger.task_processor())
end

# The fast path's view of every cached compatible set that qualified
fast_entries() = count(e -> e.fast isa Dagger.Sch.LocalFast, values(Dagger.payload(COMPAT_PROCS_CACHE).cache))

# Total reserved pressure of the eager scheduler; zero once everything has finished. Every counter is
# checked on its own: a release without a matching reservation wraps a counter around to a huge value,
# which a plain sum could cancel against another counter.
function reserved_pressure()
    state = Dagger.Sch.EAGER_STATE[]
    total = UInt64(0)
    lock(state.worker_time_pressure) do wtp
        for pm in values(wtp), c in values(pm)
            c[] == 0 || (total = max(total, c[]))
        end
    end
    return total
end

@testset "Local fast path" begin
    begin
        @testset "tasks complete and release their pressure" begin
            Dagger.Sch.invalidate_compat_procs_cache!()
            ts = [Dagger.@spawn FP_TINY(i) for i in 1:500]
            @test sum(fetch.(ts)) == sum(2:501)
            Dagger.@spawn(FP_TINY(0)) |> fetch   # one more so the last completions have been handled
            # unscoped tasks on a plain process qualify (with workers they may run anywhere)
            nprocs() == 1 && @test fast_entries() >= 1
            sleep(0.2)
            @test reserved_pressure() == 0
        end

        @testset "placement spreads over the threads" begin
            nt = Threads.nthreads()
            if nt > 1 && nprocs() == 1
                ps = Set{Processor}(fetch.([Dagger.@spawn fp_proc() for _ in 1:20 * nt]))
                @test length(ps) >= min(nt, 4)
                @test all(p -> p isa ThreadProc && p.owner == myid(), ps)
            end
        end

        @testset "options the fast path does not model take the general path" begin
            @test fetch(Dagger.@spawn proclist=[ThreadProc] FP_TINY(1)) == 2
            @test fetch(Dagger.@spawn single=1 FP_TINY(1)) == 2
            @test fetch(Dagger.@spawn occupancy=Dict(ThreadProc=>0.5) FP_TINY(3)) == 4
            @test fetch(Dagger.@spawn time_util=Dict(ThreadProc=>1_000_000) FP_TINY(4)) == 5
            @test fetch(Dagger.@spawn alloc_util=Dict(ThreadProc=>UInt64(1024)) FP_TINY(5)) == 6
            # a scope is honoured
            p = fetch(Dagger.@spawn scope=Dagger.scope(worker=myid(), thread=1) fp_proc())
            @test p == ThreadProc(myid(), 1)
            # so is a scope on a chunk argument
            ch = Dagger.tochunk(5, ThreadProc(myid(), 1), Dagger.ExactScope(ThreadProc(myid(), 1)))
            @test fetch(Dagger.@spawn FP_TINY(ch)) == 6
        end

        @testset "scopes naming some threads are honoured" begin
            nt = Threads.nthreads()
            rng = MersenneTwister(7)
            for _ in 1:80
                k = rand(rng, 1:min(nt, 4))
                tids = sort!(randperm(rng, nt)[1:k])
                sc = rand(rng, Bool) ? Dagger.scope(worker=myid(), threads=tids) :
                                       Dagger.UnionScope(map(t -> Dagger.ExactScope(ThreadProc(myid(), t)), tids))
                ts = [Dagger.with_options(scope=sc) do; Dagger.@spawn fp_proc(); end for _ in 1:4]
                @test all(p -> p isa ThreadProc && p.owner == myid() && p.tid in tids, fetch.(ts))
            end
            # many small tasks pinned to a thread subset, some of them consuming earlier results
            sc = Dagger.scope(worker=myid(), threads=1:min(nt, 2))
            first_ts = [Dagger.with_options(scope=sc) do; Dagger.@spawn fp_proc(); end for _ in 1:50]
            second_ts = [Dagger.with_options(scope=sc) do; Dagger.@spawn identity(t); end for t in first_ts]
            @test all(p -> p.tid in 1:min(nt, 2), fetch.(second_ts))
            sleep(0.2)
            @test reserved_pressure() == 0
        end

        @testset "a thread that refuses an argument type is respected" begin
            t = Dagger.@spawn FP_TINY(NoThreadArg())
            @test_throws Exception fetch(t)
        end

        @testset "a thread that refuses the function is respected" begin
            t = Dagger.@spawn NoThreadFn()(1)
            @test_throws Exception fetch(t)
        end

        @testset "Datadeps" begin
            A = [zeros(8) for _ in 1:12]
            Dagger.spawn_datadeps() do
                for a in A
                    Dagger.@spawn fill!(Dagger.InOut(a), 3.0)
                end
                for a in A
                    Dagger.@spawn rmul!(Dagger.InOut(a), 2.0)
                end
            end
            @test all(a -> all(==(6.0), a), A)
        end
    end
end

@everywhere begin
    FP_PROCARG(x) = (sleep(0.002); Dagger.task_processor())
end

# Pressure counters aligned like `LocalFast.refs`
struct FPOtherAccel <: Dagger.Acceleration end
fp_refs(v) =[Threads.Atomic{UInt64}(UInt64(x)) for x in v]
fast_values() = collect(values(@atomic Dagger.Sch.FAST_INDEX.dict))

@testset "Local fast path: decisions" begin
    LocalFast = Dagger.Sch.LocalFast
    me = myid()
    accel = Dagger.DistributedAcceleration()

    @testset "only a compatible set made of this process's threads qualifies" begin
        local_set = Processor[ThreadProc(me, 1), ThreadProc(me, 2)]
        @test Dagger.Sch.build_local_fast((; compat=local_set), accel) isa LocalFast
        @test Dagger.Sch.build_local_fast((; compat=Processor[]), accel) === nothing
        # a thread of another worker
        @test Dagger.Sch.build_local_fast((; compat=Processor[ThreadProc(me, 1), ThreadProc(me + 1, 1)]), accel) === nothing
        # a processor that is not a thread
        @test Dagger.Sch.build_local_fast((; compat=Processor[ThreadProc(me, 1), FakeProc()]), accel) === nothing
        # only the distributed acceleration is handled
        @test Dagger.Sch.build_local_fast((; compat=local_set), FPOtherAccel()) === nothing
    end

    @testset "the fast path places tasks" begin
        if nprocs() == 1
            Dagger.Sch.invalidate_compat_procs_cache!()
            @test !any(e -> e.fast isa LocalFast && (@atomic e.fast.counters) !== nothing, fast_values())
            fetch.([Dagger.@spawn FP_TINY(i) for i in 1:50])
            @test any(e -> e.fast isa LocalFast && (@atomic e.fast.counters) !== nothing, fast_values())
        end
    end

    @testset "options the fast path does not model are not decided by it" begin
        if nprocs() == 1
            for (name, f) in (("proclist", () -> Dagger.@spawn proclist=[ThreadProc] FP_TINY(1)),
                              ("single", () -> Dagger.@spawn single=1 FP_TINY(1)),
                              ("occupancy", () -> Dagger.@spawn occupancy=Dict(ThreadProc=>0.5) FP_TINY(1)),
                              ("time_util", () -> Dagger.@spawn time_util=Dict(ThreadProc=>1_000_000) FP_TINY(1)),
                              ("alloc_util", () -> Dagger.@spawn alloc_util=Dict(ThreadProc=>UInt64(1024)) FP_TINY(1)))
                Dagger.Sch.invalidate_compat_procs_cache!()
                @test fetch(f()) == 2
                @test !any(e -> e.fast isa LocalFast && (@atomic e.fast.counters) !== nothing, fast_values())
            end
        end
    end

    @testset "a scope restriction on a chunk argument is honoured" begin
        nt = Threads.nthreads()
        if nt > 1
            tp = ThreadProc(me, nt)
            ch = Dagger.tochunk(5, tp, Dagger.ExactScope(tp))
            ps = fetch.([Dagger.@spawn FP_PROCARG(ch) for _ in 1:40])
            @test all(==(tp), ps)
        end
    end

    @testset "with workers, unscoped tasks also run on other workers" begin
        if nprocs() > 1
            owners = Set(p.owner for p in fetch.([Dagger.@spawn fp_proc() for _ in 1:120]))
            @test length(owners) > 1
        end
    end

    @testset "pick_fast_candidate chooses the less loaded thread" begin
        pick = Dagger.Sch.pick_fast_candidate
        @test pick(fp_refs([5])) == 1
        # up to TWO_CHOICES_MIN candidates: always the least loaded
        refs8 = fp_refs([5, 9, 0, 7, 3, 8, 6, 4])
        @test length(refs8) == Dagger.Sch.TWO_CHOICES_MIN
        @test all(_ -> pick(refs8) == 3, 1:300)
        # ties are broken at random
        @test length(Set(pick(fp_refs(zeros(Int, 8))) for _ in 1:400)) == 8
        # more candidates: two random ones are compared. The most loaded never wins, the least
        # loaded wins when it is drawn, and it is not always the one picked
        refs9 = fp_refs([5, 9, 0, 7, 3, 8, 6, 4, 2])
        picks = [pick(refs9) for _ in 1:3000]
        @test 2 ∉ picks
        @test 3 in picks
        @test any(!=(3), picks)
    end

    @testset "candidate ranking switches to two choices above TWO_CHOICES_MIN" begin
        # One heavily loaded thread among n. The full ranking selects the PLACEMENT_SELECT cheapest
        # exactly, so the loaded one is never among the first places; two choices compares only two
        # random candidates, so the loaded one is the second place whenever it is drawn
        busy_early(n) = begin
            procs = Processor[ThreadProc(me, t) for t in 1:n]
            pressure = Dict{Processor,Threads.Atomic{UInt64}}(p => Threads.Atomic{UInt64}(UInt64(p === procs[1] ? 10^9 : 0)) for p in procs)
            state = (; worker_time_pressure=Dagger.LockedObject(Dict{Int,Dict{Processor,Threads.Atomic{UInt64}}}(me => pressure)),
                       signature_time_cost=Dagger.LockedObject(Dict{Any,Any}()),
                       worker_transfer_rate=Dagger.LockedObject(Dict{Any,Any}()))
            task = (; inputs=Any[], f=identity)
            sorted = Vector{Processor}(undef, n)
            early = 0
            for _ in 1:300
                Dagger.Sch.estimate_task_costs!(sorted, nothing, state, procs, task; sig=:sig)
                early += findfirst(==(procs[1]), sorted) <= Dagger.Sch.PLACEMENT_SELECT
                @test sorted[1] !== procs[1]
            end
            early
        end
        @test busy_early(Dagger.Sch.TWO_CHOICES_MIN) == 0
        @test busy_early(Dagger.Sch.TWO_CHOICES_MIN + 1) > 0
    end

    @testset "the lock-free snapshot is bounded and follows invalidation" begin
        procs = Processor[ThreadProc(me, 1)]
        Dagger.Sch.invalidate_compat_procs_cache!()
        @test isempty(@atomic Dagger.Sch.FAST_INDEX.dict)
        fetch(Dagger.@spawn FP_TINY(1))
        nprocs() == 1 && @test !isempty(@atomic Dagger.Sch.FAST_INDEX.dict)
        Dagger.Sch.invalidate_compat_procs_cache!()
        @test isempty(@atomic Dagger.Sch.FAST_INDEX.dict)
        scopes = [Dagger.ExactScope(ThreadProc(me, t)) for t in 1:(Dagger.Sch.FAST_INDEX_MAX + 50)]
        entries = [Dagger.Sch.fast_compat_entry(accel, sc, procs) for sc in scopes]
        # beyond the bound an entry is still returned, it is just not kept in the snapshot
        @test all(i -> entries[i].scope === scopes[i], eachindex(scopes))
        @test length(@atomic Dagger.Sch.FAST_INDEX.dict) <= Dagger.Sch.FAST_INDEX_MAX
        Dagger.Sch.invalidate_compat_procs_cache!()
    end
end
