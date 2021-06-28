import Dagger: @par, @spawn, spawn

@everywhere begin
    checkwid() = myid()==1
    function dynamic_fib(n)
        n <= 1 && return n
        t = Dagger.spawn(dynamic_fib, n-1)
        y = dynamic_fib(n-2)
        return (fetch(t)::Int) + y
    end
end

@testset "@par" begin
    @testset "per-call" begin
        x = 2
        a = @par x + x
        @test a isa Dagger.Thunk
        b = @par sum([x,1,2])
        c = @par a * b
        @test collect(c) == 20
    end
    @testset "block" begin
        c = @par begin
            x = 2
            a = x + x
            b = sum([x,1,2])
            c = a * b
        end
        @test x isa Int
        @test a isa Dagger.Thunk
        @test c isa Dagger.Thunk
        @test collect(c) == 20
    end
end

@testset "@spawn" begin
    @test Dagger.Sch.eager_context() === nothing
    @testset "per-call" begin
        x = 2
        a = @spawn x + x
        @test a isa Dagger.EagerThunk
        b = @spawn sum([x,1,2])
        c = spawn(*, a, b)
        @test c isa Dagger.EagerThunk
        @test fetch(a) == 4
        @test fetch(b) == 5
        @test fetch(c) == 20
    end
    @test Dagger.Sch.eager_context() isa Context
    @testset "waiting" begin
        a = @spawn sleep(1)
        @test !isready(a)
        wait(a)
        @test isready(a)
    end
    @testset "options" begin
        s = 1
        m = true
        a = @spawn single=s checkwid()
        b = @spawn meta=m ((_a)->_a isa Dagger.Chunk)(a)
        @test fetch(a)
        @test fetch(b)
    end
    @testset "errors" begin
        @testset "independent" begin
            a = @spawn error("Test")
            wait(a)
            @test isready(a)
            @test_throws_unwrap Dagger.ThunkFailedException fetch(a)
            b = @spawn 1+2
            @test fetch(b) == 3
        end
        @testset "direct vs indirect" begin
            a = @spawn error("Test")
            b = @spawn a+1

            ex = try
                fetch(a)
            catch err
                err
            end
            ex = Dagger.Sch.unwrap_nested_exception(ex)
            ex_str = sprint(io->Base.showerror(io,ex))
            @test occursin(r"^ThunkFailedException \(Thunk.*failure\):", ex_str)
            @test occursin("Test", ex_str)
            @test !occursin("due to a failure in", ex_str)

            ex = try
                fetch(b)
            catch err
                err
            end
            ex = Dagger.Sch.unwrap_nested_exception(ex)
            ex_str = sprint(io->Base.showerror(io,ex))
            @test occursin(r"Thunk.*failure due to a failure in", ex_str)
            @test occursin("Test", ex_str)
        end
        @testset "single dependent" begin
            a = @spawn error("Test")
            b = @spawn a+2
            @test_throws_unwrap Dagger.ThunkFailedException fetch(a)
        end
        @testset "multi dependent" begin
            a = @spawn error("Test")
            b = @spawn a+2
            c = @spawn a*2
            @test_throws_unwrap Dagger.ThunkFailedException fetch(b)
            @test_throws_unwrap Dagger.ThunkFailedException fetch(c)
        end
        @testset "dependent chain" begin
            a = @spawn error("Test")
            @test_throws_unwrap Dagger.ThunkFailedException fetch(a)
            b = @spawn a+1
            @test_throws_unwrap Dagger.ThunkFailedException fetch(b)
            c = @spawn b+2
            @test_throws_unwrap Dagger.ThunkFailedException fetch(c)
        end
        @testset "single input" begin
            a = @spawn 1+1
            b = @spawn (a->error("Test"))(a)
            @test fetch(a) == 2
            @test_throws_unwrap Dagger.ThunkFailedException fetch(b)
        end
        @testset "multi input" begin
            a = @spawn 1+1
            b = @spawn 2*2
            c = @spawn ((a,b)->error("Test"))(a,b)
            @test fetch(a) == 2
            @test fetch(b) == 4
            @test_throws_unwrap Dagger.ThunkFailedException fetch(c)
        end
        @testset "diamond" begin
            a = @spawn 1+1
            b = @spawn a+1
            c = @spawn a*2
            d = @spawn ((b,c)->error("Test"))(b,c)
            @test fetch(a) == 2
            @test fetch(b) == 3
            @test fetch(c) == 4
            @test_throws_unwrap Dagger.ThunkFailedException fetch(d)
        end
    end
    @testset "remote spawn" begin
        a = fetch(Distributed.@spawnat 2 Dagger.spawn(+, 1, 2))
        @test Dagger.Sch.EAGER_INIT[]
        @test fetch(Distributed.@spawnat 2 !(Dagger.Sch.EAGER_INIT[]))
        @test a isa Dagger.EagerThunk
        @test fetch(a) == 3

        # Mild stress-test
        @test dynamic_fib(10) == 55
    end
end
