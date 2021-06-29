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
    id = Dagger.Sch.add_thunk!(h, x) do y
        y+1
    end
    wait(h, id)
    return fetch(h, id)
end
function dynamic_add_thunk_self_dominated(x)
    h = sch_handle()
    id = Dagger.Sch.add_thunk!(h, h.thunk_id, x) do y
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
            @test collect(b) in workers()
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
        @everywhere Dagger.add_callback!(()->FakeProc())
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
        @everywhere (pop!(Dagger.PROCESSOR_CALLBACKS); empty!(Dagger.OSPROC_CACHE))
        @testset "procutil" begin
            opts = ThunkOptions(;procutil=Dict(Dagger.ThreadProc=>0.25))
            as = [delayed(checkpressure; options=opts)(i) for i in 1:30]
            b = delayed(checkpressure)(as...)
            collect(b)
        end
        @testset "allow errors" begin
            opts = ThunkOptions(;allow_errors=true)
            a = delayed(error; options=opts)("Test")
            @test_throws_unwrap Dagger.ThunkFailedException collect(a)
        end
    end

    @testset "Modify workers in running job" begin
        # Test that we can add/remove workers while scheduler is running.
        # As this requires asynchronity, a flag is used to stall the tasks to
        # ensure workers are actually modified while the scheduler is working.

        setup = quote
            using Dagger, Distributed
            # blocked is to guarantee that processing is not completed before we add new workers
            # Note: blocked is used in expressions below
            blocked = true
            function testfun(i)
                i < 4 && return myid()
                # Wait for test to do its thing before we proceed
                while blocked
                    sleep(0.001)
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

                ts = delayed(vcat)((delayed(testfun)(i) for i in 1:10)...)

                ctx = Context(ps1)
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

                @everywhere vcat(ps1, ps3) blocked=false

                @test fetch(job) isa Vector
                # TODO: Fix this unreliable test
                @test_skip fetch(job) |> unique |> sort == vcat(ps1, ps3)
            finally
                wait(rmprocs(ps))
            end
        end

        @testset "Remove workers" begin
            ps = []
            try
                ps1 = addprocs(4, exeflags="--project")
                append!(ps, ps1)

                @everywhere vcat(ps1, myid()) $setup

                ts = delayed(vcat)((delayed(testfun)(i) for i in 1:16)...)

                ctx = Context(ps1)
                job = @async collect(ctx, ts)

                while !istaskstarted(job)
                    sleep(0.001)
                end

                rmprocs!(ctx, ps1[3:end])
                @test length(procs(ctx)) == 2

                @everywhere ps1 blocked=false

                res = fetch(job)
                @test res isa Vector
                # First all four workers will report their IDs without hassle
                # Then all four will be waiting for the Condition While they
                # are waiting ps1[3:end] are removed, but when the Condition is
                # notified they will finish their tasks before being removed
                # Will probably break if workers are assigned more than one Thunk
                @test_skip res[1:8] |> unique |> sort == ps1
                @test all(pid -> pid in ps1[1:2], res[9:end])
            finally
                wait(rmprocs(ps))
            end
        end

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
@testset "Memory Retention" begin
    @testset "Chunk Caching" begin
        compute(delayed(testevicted)(delayed(testpresent)(c1,c2)))
    end
    @testset "Eager Thunk Expiration" begin
        # FIXME: Unreliable, and some thunks still get stuck
        # N.B. We need a few of these probably because of incremental WeakRef GC
        @everywhere GC.gc()
        GC.gc()
        GC.gc()
        # Ensure that all cache entries have expired
        @test_broken isempty(Dagger.Sch.EAGER_STATE[].cache)
    end
end
