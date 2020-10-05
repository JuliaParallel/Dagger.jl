import Dagger.Sch: SchedulerOptions, ThunkOptions

@everywhere begin
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
end

@testset "Scheduler" begin
    @testset "Scheduler options: single worker" begin
        options = SchedulerOptions(;single=1)
        a = delayed(checkwid)(1)
        b = delayed(checkwid)(2)
        c = delayed(checkwid)(a,b)

        @test collect(Context(), c; options=options) == 1
    end
    @testset "Thunk options: single worker" begin
        options = ThunkOptions(;single=1)
        a = delayed(checkwid; options=options)(1)

        @test collect(Context(), a) == 1
    end
    @static if VERSION >= v"1.3.0-DEV.573"
        if Threads.nthreads() == 1
            @warn "Threading tests running in serial"
        end
        @testset "Scheduler options: threads" begin
            options = SchedulerOptions(;proctypes=[Dagger.ThreadProc])
            a = delayed(checktid)(1)
            b = delayed(checktid)(2)
            c = delayed(checktid)(a,b)

            @test collect(Context(), c; options=options) == 1
        end
        @testset "Thunk options: threads" begin
            options = ThunkOptions(;proctypes=[Dagger.ThreadProc])
            a = delayed(checktid; options=options)(1)

            @test collect(Context(), a) == 1
        end
    end

    @everywhere Dagger.add_callback!(proc->FakeProc())
    @testset "Thunk options: proctypes" begin
        @test Dagger.iscompatible_arg(FakeProc(), nothing, 1) == true
        @test Dagger.iscompatible_arg(FakeProc(), nothing, FakeVal(1)) == true
        @test Dagger.iscompatible_arg(FakeProc(), nothing, 1.0) == false
        @test Dagger.default_enabled(Dagger.ThreadProc(1,1)) == true
        @test Dagger.default_enabled(FakeProc()) == false

        opts = Dagger.Sch.ThunkOptions(;proctypes=[Dagger.ThreadProc])
        as = [delayed(identity; options=opts)(i) for i in 1:5]
        opts = Dagger.Sch.ThunkOptions(;proctypes=[FakeProc])
        b = delayed(fakesum; options=opts)(as...)

        @test collect(Context(), b) == 57
    end
    @everywhere (pop!(Dagger.PROCESSOR_CALLBACKS); empty!(Dagger.OSPROC_CACHE))

    @testset "Modify workers in running job" begin
        # Test that we can add/remove workers while scheduler is running.
        # As this requires asynchronity a flag is used to stall the tasks to 
        # ensure workers are actually modified while the scheduler is working 

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
                @test fetch(job) |> unique |> sort == vcat(ps1, ps3)

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
                # Then all four will be waiting for the Condition
                # While they are waiting ps1[3:end] are removed, but when the Condition is notified they will finish their tasks before being removed
                # Will probably break if workers are assigned more than one Thunk 
                @test res[1:8] |> unique |> sort == ps1
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
                @test_throws TaskFailedException fetch(job)
            finally
                wait(rmprocs(ps))
            end
        end
    end
end
