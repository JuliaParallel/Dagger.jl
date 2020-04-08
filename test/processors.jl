using Distributed
import Dagger: Context, Processor, OSProc, ThreadProc, get_parent, get_processors
import Dagger.Sch: ThunkOptions

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
        struct UnknownStruct end
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
    @testset "Roundtrip move()" begin
        ctx = Context()
        tp = ThreadProc(1, 1)
        op = get_parent(tp)
        value = rand()
        moved_value = Dagger.move(ctx, tp, op, Dagger.move(ctx, op, tp, value))
        @test value === moved_value
    end
    @testset "Generic path move()" begin
        @everywhere begin

        struct PathProc <: Dagger.Processor
            owner::Int
        end
        Dagger.get_parent(proc::PathProc) = OSProc(proc.owner)
        Dagger.move(ctx, ::PathProc, ::OSProc, x::Float64) = x+1
        Dagger.move(ctx, ::OSProc, ::PathProc, x::Float64) = x+2
        Dagger.add_callback!(proc->PathProc(myid()))

        end

        ctx = Context()
        proc1 = first(filter(x->x isa PathProc, get_processors(OSProc(1))))
        proc2 = first(filter(x->x isa PathProc, get_processors(OSProc(2))))
        value = rand()
        moved_value = Dagger.move(ctx, proc1, proc2, value)
        @test moved_value == value+3
        @everywhere pop!(Dagger.PROCESSOR_CALLBACKS)
    end
end
