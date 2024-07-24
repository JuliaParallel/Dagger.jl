import Dagger.Sch: SchedulerOptions, ThunkOptions, SchedulerHaltedException, ComputeState, ThunkID, sch_handle

@everywhere begin
using Dagger
import Dagger: sch_handle, exec!, halt!, get_dag_ids, add_thunk!
function inc(x)
    x+1
end
function checkwid(x...)
    @assert myid() == 1
    return 1
end
function checktid(x...)
    @assert Threads.threadid() != 1 || Threads.nthreads() == 1
    return 1
end
global pressure = Ref{Int}(0)
function checkpressure(x...)
    global pressure
    pressure[] += 1
    sleep(1)
    @assert pressure[] <= 4
    pressure[] -= 1
end
function dynamic_exec(x)
    h = sch_handle()
    Dagger.Sch.exec!(h) do ctx, state, task, tid, _
        if state isa ComputeState
            return 1
        else
            return 0
        end
    end
end
function dynamic_exec_err(x)
    h = sch_handle()
    Dagger.Sch.exec!(h) do ctx, state, _, _, _
        error("An error")
    end
end
function dynamic_halt(x)
    h = sch_handle()
    Dagger.Sch.halt!(h)
    return x
end
function dynamic_get_dag(x...)
    h = sch_handle()
    ids = Dagger.Sch.get_dag_ids(h)
    return ids
end
function dynamic_add_thunk(x)
    h = sch_handle()
    id = Dagger.Sch.add_thunk!(h, nothing=>x) do y
        y+1
    end
    wait(h, id)
    return fetch(h, id)
end
function dynamic_add_thunk_self_dominated(x)
    h = sch_handle()
    id = Dagger.Sch.add_thunk!(h, nothing=>h.thunk_id, nothing=>x) do y
        y+1
    end
    return fetch(h, id)
end
function dynamic_wait_fetch_multiple(x)
    h = sch_handle()
    ids = Dagger.Sch.get_dag_ids(h)
    id = nothing
    for key in keys(ids)
        while !isempty(ids[key])
            val = pop!(ids[key])
            if val == h.thunk_id
                id = key
            end
        end
    end
    wait(h, id)
    wait(h, id)
    fetch(h, id)
    fetch(h, id)
    x
end
function dynamic_fetch_self(x)
    h = sch_handle()
    return fetch(h, h.thunk_id)
end
function dynamic_fetch_dominated(x)
    h = sch_handle()
    ids = Dagger.Sch.get_dag_ids(h)
    did = pop!(ids[h.thunk_id])
    wait(h, did)
end
end

@testset "Scheduler" begin
    @testset "Scheduler options" begin
        @testset "single worker" begin
            options = SchedulerOptions(;single=1)
            a = delayed(checkwid)(1)
            b = delayed(checkwid)(2)
            c = delayed(checkwid)(a,b)

            @test collect(Context([1,workers()...]), c; options=options) == 1
        end
        @static if VERSION >= v"1.3.0-DEV.573"
            if Threads.nthreads() == 1
                @warn "Threading tests running in serial"
            end
            @testset "proclist" begin
                options = SchedulerOptions(;proclist=[Dagger.ThreadProc])
                a = delayed(checktid)(1)
                b = delayed(checktid)(2)
                c = delayed(checktid)(a,b)

                @test collect(Context(), c; options=options) == 1
            end
        end
        @testset "allow errors" begin
            options = SchedulerOptions(;allow_errors=true)
            a = delayed(error)("Test")
            ex = try
                collect(a)
            catch err
                err
            end
            @test Dagger.Sch.unwrap_nested_exception(ex) isa ErrorException
        end
    end
    @testset "Thunk options" begin
        @testset "meta" begin
            a = Dagger.@par rand(4)
            b = Dagger.@par meta=true (a->begin
                @assert a isa Dagger.Chunk
                Dagger.tochunk(myid())
            end)(a)
            if Threads.nthreads() == 1
                @test collect(b) in workers()
            else
                @test collect(b) in procs()
            end
        end
        @testset "single worker" begin
            options = ThunkOptions(;single=1)
            a = delayed(checkwid; options=options)(1)

            @test collect(Context([1,workers()...]), a) == 1
        end
        @static if VERSION >= v"1.3.0-DEV.573"
            @testset "proclist" begin
                options = ThunkOptions(;proclist=[Dagger.ThreadProc])
                a = delayed(checktid; options=options)(1)

                @test collect(Context(), a) == 1
            end
        end
        @everywhere Dagger.add_processor_callback!(()->FakeProc(), :fakeproc)
        @testset "proclist FakeProc" begin
            @test Dagger.iscompatible_arg(FakeProc(), nothing, Int) == true
            @test Dagger.iscompatible_arg(FakeProc(), nothing, FakeVal) == true
            @test Dagger.iscompatible_arg(FakeProc(), nothing, Float64) == false
            @test Dagger.default_enabled(Dagger.ThreadProc(1,1)) == true
            @test Dagger.default_enabled(FakeProc()) == false

            opts = Dagger.Sch.ThunkOptions(;proclist=[Dagger.ThreadProc])
            as = [delayed(identity; options=opts)(i) for i in 1:5]
            opts = Dagger.Sch.ThunkOptions(;proclist=[FakeProc])
            b = delayed(fakesum; options=opts)(as...)

            @test collect(Context(), b) == FakeVal(57)
        end
        @everywhere Dagger.delete_processor_callback!(:fakeproc)
        @test_skip "procutil"
        #=
        @testset "procutil" begin
            opts = ThunkOptions(;procutil=Dict(Dagger.ThreadProc=>0.25))
            as = [delayed(checkpressure; options=opts)(i) for i in 1:30]
            b = delayed(checkpressure)(as...)
            collect(b)
        end
        =#
        @testset "allow errors" begin
            opts = ThunkOptions(;allow_errors=true)
            a = delayed(error; options=opts)("Test")
            @test_throws_unwrap Dagger.DTaskFailedException collect(a)
        end
    end

    @testset "Modify workers in running job" begin
        # Test that we can add/remove workers while scheduler is running.
        # As this requires asynchronity, a flag is used to stall the tasks to
        # ensure workers are actually modified while the scheduler is working.

        setup = quote
            using Dagger, Distributed
            function _list_workers(ctx, state, task, tid, _)
                return procs(ctx)
            end
            # blocked is to guarantee that processing is not completed before we add new workers
            # Note: blocked is used in expressions below
            blocked = true
            function testfun(i)
                i <= 4 && return myid()
                # Wait for test to do its thing before we proceed
                if blocked
                    sleep(0.1) # just so we don't end up overflowing or something while waiting for workers to be added
                    # Here we would like to just wait to be rescheduled on another worker (which is not blocked)
                    # but this functionality does not exist, so instead we do this weird thing where we reschedule
                    # until we end up on a non-blocked worker
                    h = Dagger.Sch.sch_handle()
                    wkrs = Dagger.Sch.exec!(_list_workers, h)
                    id = Dagger.Sch.add_thunk!(testfun, h, nothing=>i)
                    return fetch(h, id)
                end
                return myid()
            end
        end

        @testset "Add new workers" begin
            ps = []
            try
                ps1 = addprocs(2, exeflags="--project")
                append!(ps, ps1)

                @everywhere vcat(ps1, myid()) $setup

                ctx = Context(ps1)
                ts = delayed(vcat)((delayed(testfun)(i) for i in 1:10)...)

                job = @async collect(ctx, ts)

                while !istaskstarted(job)
                    sleep(0.001)
                end

                # Will not be added, so they should never appear in output
                ps2 = addprocs(2, exeflags="--project")
                append!(ps, ps2)

                ps3 = addprocs(2, exeflags="--project")
                append!(ps, ps3)
                @everywhere ps3 $setup
                addprocs!(ctx, ps3)
                @test length(procs(ctx)) == 4

                @everywhere ps3 blocked=false

                ps_used = fetch(job)
                @test ps_used isa Vector

                @test any(p -> p in ps_used, ps1)
                @test any(p -> p in ps_used, ps3)
                @test !any(p -> p in ps2, ps_used)
            finally
                wait(rmprocs(ps))
            end
        end

        @test_skip "Remove workers"
        #=@testset "Remove workers" begin
            ps = []
            try
                ps1 = addprocs(4, exeflags="--project")
                append!(ps, ps1)

                @everywhere vcat(ps1, myid()) $setup

                # Use single to force scheduler to make use of all workers since we assert it below
                ts = delayed(vcat)((delayed(testfun; single=ps1[mod1(i, end)])(i) for i in 1:10)...)

                # Use FilterLog as a callback function.
                nprocs_removed = Ref(0)
                first_rescheduled_thunk=Ref(false)
                rmproctrigger = Dagger.FilterLog(Dagger.NoOpLog()) do event
                    if typeof(event) == Dagger.Event{:finish} && event.category === :cleanup_proc
                        nprocs_removed[] += 1
                    end
                    if typeof(event) == Dagger.Event{:start} && event.category === :add_thunk
                        first_rescheduled_thunk[] = true
                    end
                    return false
                end

                ctx = Context(ps1; log_sink=rmproctrigger)
                job = @async collect(ctx, ts)

                # Must wait for this or else we won't get callback for rmprocs!
                # Timeout so we don't stall forever if something breaks
                starttime = time()
                while !first_rescheduled_thunk[] && (time() - starttime < 10.0)
                    sleep(0.1)
                end
                @test first_rescheduled_thunk[]

                rmprocs!(ctx, ps1[3:end])
                @test length(procs(ctx)) == 2

                # Timeout so we don't stall forever if something breaks
                starttime = time()
                while (nprocs_removed[] < 2) && (time() - starttime < 10.0)
                    sleep(0.01)
                end
                # this will fail if we timeout. Verify that we get the logevent for :cleanup_proc
                @test nprocs_removed[] >= 2

                @everywhere ps1 blocked=false

                res = fetch(job)
                @test res isa Vector

                @test res[1:4] |> unique |> sort == ps1
                @test all(pid -> pid in ps1[1:2], res[5:end])
            finally
                # Prints "From worker X:    IOError:" :/
                wait(rmprocs(ps))
            end
        end=#

        @testset "Remove all workers throws" begin
            ps = []
            try
                ps1 = addprocs(2, exeflags="--project")
                append!(ps, ps1)

                @everywhere vcat(ps1, myid()) $setup

                ts = delayed(vcat)((delayed(testfun)(i) for i in 1:16)...)

                ctx = Context(ps1)
                job = @async collect(ctx, ts)

                while !istaskstarted(job)
                    sleep(0.001)
                end

                rmprocs!(ctx, ps1)
                @test length(procs(ctx)) == 0

                @everywhere ps1 blocked=false
                if VERSION >= v"1.3.0-alpha.110"
                    @test_throws TaskFailedException fetch(job)
                else
                    @test_throws Exception fetch(job)
                end
            finally
                wait(rmprocs(ps))
            end
        end
    end
end

@testset "Scheduler algorithms" begin
    @testset "Signature Calculation" begin
        @test Dagger.Sch.signature(+, [nothing=>1, nothing=>2]) isa Vector{DataType}
        @test Dagger.Sch.signature(+, [nothing=>1, nothing=>2]) == [typeof(+), Int, Int]
        if isdefined(Core, :kwcall)
            @test Dagger.Sch.signature(+, [nothing=>1, :a=>2]) == [typeof(Core.kwcall), @NamedTuple{a::Int64}, typeof(+), Int]
        else
            kw_f = Core.kwfunc(+)
            @test Dagger.Sch.signature(+, [nothing=>1, :a=>2]) == [typeof(kw_f), @NamedTuple{a::Int64}, typeof(+), Int]
        end
        @test Dagger.Sch.signature(+, []) == [typeof(+)]
        @test Dagger.Sch.signature(+, [nothing=>1]) == [typeof(+), Int]

        c = Dagger.tochunk(1.0)
        @test Dagger.Sch.signature(*, [nothing=>c, nothing=>3]) == [typeof(*), Float64, Int]
        t = Dagger.@spawn 1+2
        @test Dagger.Sch.signature(/, [nothing=>t, nothing=>c, nothing=>3]) == [typeof(/), Int, Float64, Int]
    end

    @testset "Cost Estimation" begin
        # New function to hide from scheduler's function cost cache
        mynothing(args...) = nothing

        # New non-singleton struct to hide from `approx_size`
        struct MyStruct
            x::Int
        end

        state = Dagger.Sch.EAGER_STATE[]
        tproc1 = Dagger.ThreadProc(1, 1)
        tproc2 = Dagger.ThreadProc(first(workers()), 1)
        procs = [tproc1, tproc2]

        pres1 = state.worker_time_pressure[1][tproc1]
        pres2 = state.worker_time_pressure[first(workers())][tproc2]
        tx_rate = state.transfer_rate[]

        for (args, tx_size) in [
            ([1, 2], 0),
            ([Dagger.tochunk(1), 2], sizeof(Int)),
            ([1, Dagger.tochunk(2)], sizeof(Int)),
            ([Dagger.tochunk(1), Dagger.tochunk(2)], 2*sizeof(Int)),
            # TODO: Why does this work? Seems slow
            ([Dagger.tochunk(MyStruct(1))], sizeof(MyStruct)),
            ([Dagger.tochunk(MyStruct(1)), Dagger.tochunk(1)], sizeof(MyStruct)+sizeof(Int)),
        ]
            for arg in args
                if arg isa Chunk
                    aff = Dagger.affinity(arg)
                    @test aff[1] == OSProc(1)
                    @test aff[2] == MemPool.approx_size(MemPool.poolget(arg.handle))
                end
            end

            cargs = map(arg->MemPool.poolget(arg.handle), filter(arg->isa(arg, Chunk), args))
            est_tx_size = Dagger.Sch.impute_sum(map(MemPool.approx_size, cargs))
            @test est_tx_size == tx_size

            t = delayed(mynothing)(args...)
            inputs = Dagger.Sch.collect_task_inputs(state, t)
            sorted_procs, costs = Dagger.Sch.estimate_task_costs(state, procs, t, inputs)

            @test tproc1 in sorted_procs
            @test tproc2 in sorted_procs
            if length(cargs) > 0
                @test sorted_procs[1] == tproc1
                @test sorted_procs[2] == tproc2
            end

            @test haskey(costs, tproc1)
            @test haskey(costs, tproc2)
            @test costs[tproc1] ≈ pres1 # All chunks are local
            @test costs[tproc2] ≈ (tx_size/tx_rate) + pres2 # All chunks are remote
        end
    end
end

@testset "Dynamic Thunks" begin
    @testset "Exec" begin
        a = delayed(dynamic_exec)(2)
        @test collect(Context(), a) == 1
    end
    @testset "Exec Error" begin
        a = delayed(dynamic_exec_err)(1)
        try
            collect(Context(), a)
            @test false
        catch err
            @test err isa RemoteException
        end
    end
    @test_skip "Halt" #=begin
        a = delayed(dynamic_halt)(1)
        try
            collect(Context(), a)
            @test false
        catch err
            @test err isa SchedulerHaltedException
        end
    end=#
    @testset "DAG querying" begin
        a = delayed(identity)(1)
        b = delayed(x->x+2)(a)
        c = delayed(x->x-1)(a)
        d = delayed(dynamic_get_dag)(b, c)
        ids = collect(Context(), d)
        @test ids isa Dict
        @test length(keys(ids)) == 4

        a_id = ThunkID(a.id)
        b_id = ThunkID(b.id)
        c_id = ThunkID(c.id)
        d_id = ThunkID(d.id)

        @test haskey(ids, d_id)
        @test length(ids[d_id]) == 0 # no one waiting on our result
        @test length(ids[a_id]) == 0 # b and c finished, our result is unneeded
        @test length(ids[b_id]) == 1 # d is still executing
        @test length(ids[c_id]) == 1 # d is still executing
        @test pop!(ids[b_id]) == d_id
        @test pop!(ids[c_id]) == d_id
    end
    @testset "Add Thunk" begin
        a = delayed(dynamic_add_thunk)(1)
        res = collect(Context(), a)
        @test res == 2
        @testset "self as input" begin
            a = delayed(dynamic_add_thunk_self_dominated)(1)
            @test_throws_unwrap Dagger.Sch.DynamicThunkException reason="Cannot fetch result of dominated thunk" collect(Context(), a)
        end
    end
    @testset "Fetch/Wait" begin
        @testset "multiple" begin
            a = delayed(dynamic_wait_fetch_multiple)(delayed(+)(1,2))
            @test collect(Context(), a) == 3
        end
        @testset "self" begin
            a = delayed(dynamic_fetch_self)(1)
            @test_throws_unwrap Dagger.Sch.DynamicThunkException reason="Cannot fetch own result" collect(Context(), a)
        end
        @testset "dominated" begin
            a = delayed(identity)(delayed(dynamic_fetch_dominated)(1))
            @test_throws_unwrap Dagger.Sch.DynamicThunkException reason="Cannot fetch result of dominated thunk" collect(Context(), a)
        end
    end
end

c1 = Dagger.tochunk(1)
c2 = Dagger.tochunk(2)
@everywhere begin
function testpresent(x,y)
    @assert haskey(Dagger.Sch.CHUNK_CACHE, $c1)
    @assert haskey(Dagger.Sch.CHUNK_CACHE, $c2)
    x+y
end
function testevicted(x)
    sleep(1)
    @assert !haskey(Dagger.Sch.CHUNK_CACHE, $c1)
    @assert !haskey(Dagger.Sch.CHUNK_CACHE, $c2)
    x
end
end

@test_skip "Chunk Caching"
#=
@testset "Chunk Caching" begin
    compute(delayed(testevicted)(delayed(testpresent)(c1,c2)))
end
=#

@testset "MemPool.approx_size" begin
    for (obj, size) in [
        (rand(100), 100*sizeof(Float64)),
        (rand(Float32, 100), 100*sizeof(Float32)),
        (rand(1:10, 100), 100*sizeof(Int)),
        (fill(:a, 10), missing),
        (fill("a", 10), missing),
        (fill('a', 10), missing),
    ]
        if size !== missing
            @test MemPool.approx_size(obj) == size
        else
            @test MemPool.approx_size(obj) !== nothing
        end
    end
end
