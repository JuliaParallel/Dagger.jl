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
        @test Dagger.iscompatible(FakeProc(), nothing, 1) == true
        @test Dagger.iscompatible(FakeProc(), nothing, FakeVal(1)) == true
        @test Dagger.iscompatible(FakeProc(), nothing, 1.0) == false

        opts = Dagger.Sch.ThunkOptions(;proctypes=[Dagger.ThreadProc])
        as = [delayed(identity; options=opts)(i) for i in 1:5]
        opts = Dagger.Sch.ThunkOptions(;proctypes=[FakeProc])
        b = delayed(fakesum; options=opts)(as...)

        @test collect(Context(), b) == 57
    end
    @everywhere (pop!(Dagger.PROCESSOR_CALLBACKS); empty!(Dagger.OSPROC_CACHE))

end
