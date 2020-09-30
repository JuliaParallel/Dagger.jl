using Distributed
import Dagger: Context, Processor, OSProc, ThreadProc, get_parent, get_processors
import Dagger.Sch: ThunkOptions

@everywhere begin

struct UnknownStruct end

struct OptOutProc <: Dagger.Processor end

struct PathProc <: Dagger.Processor
    owner::Int
end
Dagger.get_parent(proc::PathProc) = OSProc(proc.owner)
Dagger.move(::PathProc, ::OSProc, x::Float64) = x+1
Dagger.move(::OSProc, ::PathProc, x::Float64) = x+2
Dagger.iscompatible(proc::PathProc, opts, f, args...) = true
Dagger.execute!(proc::PathProc, func, args...) = func(args...)

end

@testset "Processors" begin
    @testset "Parents/Children" begin
        tp = ThreadProc(1, 1)
        @test tp isa Processor
        op = get_parent(tp)
        @test op isa Processor
        @test op isa OSProc
        @test op.pid == 1
    end
    @testset "Function/argument compatability" begin
        unknown_func = () -> nothing
        tp = ThreadProc(1, 1)
        op = get_parent(tp)
        opts = ThunkOptions()
        us = UnknownStruct()
        for proc in (op, tp)
            @test Dagger.iscompatible_func(proc, opts, unknown_func)
            @test Dagger.iscompatible_arg(proc, opts, us)
            @test Dagger.iscompatible(proc, opts, unknown_func, us, 1, us, 2.0)
        end
    end
    @testset "Opt-in/Opt-out" begin
        @test Dagger.default_enabled(OSProc()) == true
        @test Dagger.default_enabled(ThreadProc(1,1)) == true
        @test Dagger.default_enabled(OptOutProc()) == false
    end
    @testset "Processor exhaustion" begin
        opts = ThunkOptions(proctypes=[OptOutProc])
        @test_throws CapturedException collect(delayed(sum; options=opts)([1,2,3]))
    end
    @testset "Roundtrip move()" begin
        ctx = Context()
        tp = ThreadProc(1, 1)
        op = get_parent(tp)
        value = rand()
        moved_value = Dagger.move(tp, op, Dagger.move(op, tp, value))
        @test value === moved_value
    end
    @testset "Generic path move()" begin
        @everywhere Dagger.add_callback!(proc->PathProc(myid()))
        ctx = Context()
        proc1 = first(filter(x->x isa PathProc, get_processors(OSProc(1))))
        proc2 = first(filter(x->x isa PathProc, get_processors(OSProc(2))))
        value = rand()
        moved_value = Dagger.move(proc1, proc2, value)
        @test moved_value == value+3
        @everywhere pop!(Dagger.PROCESSOR_CALLBACKS)
    end
    @testset "Add callback in same world" begin
        function addcb()
            @everywhere Dagger.add_callback!(cb)
            cb = eval(Dagger, :(proc->PathProc(myid())))
            opts = ThunkOptions(proctypes=[PathProc])
            collect(delayed(identity; options=opts)(1.0))
            @everywhere pop!(Dagger.PROCESSOR_CALLBACKS)
        end
    end
end
