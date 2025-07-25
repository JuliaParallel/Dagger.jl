import Dagger: @par, @spawn, spawn
import Dagger: Chunk

@everywhere begin
    checkwid() = myid()==1

    function dynamic_fib(n)
        n <= 1 && return n
        t = Dagger.@spawn dynamic_fib(n-1)
        y = dynamic_fib(n-2)
        return (fetch(t)::Int) + y
    end

    struct ProcessLockedStruct
        x::Ptr{Int} # Zero'd during serialization
    end
    (pls::ProcessLockedStruct)(x) = x+UInt(pls.x)

    struct MulProc <: Dagger.Processor
        owner::Int
    end
    MulProc() = MulProc(myid())
    Dagger.get_parent(mp::MulProc) = OSProc(mp.owner)
    Dagger.move(src::MulProc, dest::Dagger.OSProc, ::Function) = Base.:*
    Dagger.move(src::MulProc, dest::Dagger.ThreadProc, ::Function) = Base.:*
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
    if nprocs() > 1
        @test_throws_unwrap ConcurrencyViolationError remotecall_fetch(last(workers())) do
            Dagger.Sch.init_eager()
        end
    end
    @test Dagger.Sch.EAGER_CONTEXT[] === nothing
    @testset "per-call" begin
        x = 2
        a = @spawn x + x
        @test a isa Dagger.DTask
        b = @spawn sum([x,1,2])
        c = @spawn a * b
        @test c isa Dagger.DTask
        @test fetch(a) == 4
        @test fetch(b) == 5
        @test fetch(c) == 20
    end
    @test Dagger.Sch.EAGER_CONTEXT[] isa Context
    @testset "keyword arguments" begin
        A = rand(4, 4)
        @test fetch(@spawn sum(A; dims=1)) ≈ sum(A; dims=1)

        @test_throws_unwrap (Dagger.DTaskFailedException, MethodError) fetch(@spawn sum(A; fakearg=2))

        @test fetch(@spawn reduce(+, A; dims=1, init=2.0)) ≈
              reduce(+, A; dims=1, init=2.0)
    end
    @testset "broadcast" begin
        A, B = rand(4), rand(4)
        @test fetch(@spawn A .+ B) ≈ A .+ B
        @test fetch(@spawn A .* B) ≈ A .* B
    end
    @testset "inner macro" begin
        A = rand(4)
        t = @spawn sum(@view A[2:3])
        @test t isa Dagger.DTask
        @test fetch(t) ≈ sum(@view A[2:3])
    end
    @testset "do block" begin
        A = rand(4)

        t = @spawn sum(A) do a
            a + 1
        end
        @test t isa Dagger.DTask
        @test fetch(t) ≈ sum(a->a+1, A)

        t = @spawn sum(A; dims=1) do a
            a + 1
        end
        @test t isa Dagger.DTask
        @test fetch(t) ≈ sum(a->a+1, A; dims=1)

        do_f = f -> f(42)
        t = @spawn do_f() do x
            x + 1
        end
        @test t isa Dagger.DTask
        @test fetch(t) == 43
    end
    @testset "anonymous direct call" begin
        A = rand(4)

        t = @spawn A->sum(A)
        @test t isa Dagger.DTask
        @test fetch(t) == sum(A)

        t = @spawn A->sum(A; dims=1)
        @test t isa Dagger.DTask
        @test fetch(t) == sum(A; dims=1)
    end
    @testset "getindex" begin
        A = rand(4, 4)

        t = @spawn A[1, 2]
        @test t isa Dagger.DTask
        @test fetch(t) == A[1, 2]

        t = @spawn A[2]
        @test t isa Dagger.DTask
        @test fetch(t) == A[2]

        B = @spawn rand(4, 4)
        t = @spawn B[1, 2]
        @test t isa Dagger.DTask
        @test fetch(t) == fetch(B)[1, 2]

        R = Ref(42)
        t = @spawn R[]
        @test t isa Dagger.DTask
        @test fetch(t) == 42
    end
    @testset "setindex!" begin
        A = Dagger.@mutable rand(4, 4)

        t = @spawn A[1, 2] = 3.0
        @test t isa Dagger.DTask
        @test fetch(t) == 3.0
        @test fetch(@spawn A[1, 2]) == 3.0

        t = @spawn A[2] = 4.0
        @test t isa Dagger.DTask
        @test fetch(t) == 4.0
        @test fetch(@spawn A[2]) == 4.0

        R = Dagger.@mutable Ref(42)
        t = @spawn R[] = 43
        @test t isa Dagger.DTask
        @test fetch(t) == 43
        @test fetch(@spawn R[]) == 43
    end
    @testset "NamedTuple" begin
        t = @spawn (;a=1, b=2)
        @test t isa Dagger.DTask
        @test fetch(t) == (;a=1, b=2)

        t = @spawn (;)
        @test t isa Dagger.DTask
        @test fetch(t) == (;)
    end
    @testset "getproperty" begin
        nt = (;a=1, b=2)

        t = @spawn nt.b
        @test t isa Dagger.DTask
        @test fetch(t) == nt.b

        nt2 = @spawn (;a=1, b=3)
        t = @spawn nt2.b
        @test t isa Dagger.DTask
        @test fetch(t) == fetch(nt2).b
    end
    @testset "broadcast" begin
        x = randn(100)

        t = @spawn abs.(x)
        @test t isa Dagger.DTask
        @test fetch(t) == abs.(x)
    end
    @testset "invalid expression" begin
        @test_throws LoadError eval(:(@spawn 1))
        @test_throws LoadError eval(:(@spawn begin 1 end))
        @test_throws LoadError eval(:(@spawn begin
            1+1
            1+1
        end))
    end
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
            @test_throws_unwrap (Dagger.DTaskFailedException, ErrorException) fetch(a)
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
            ex_str = sprint(io->Base.showerror(io, ex))
            @test occursin(r"^DTaskFailedException:", ex_str)
            @test occursin("Test", ex_str)
            @test !occursin("Root Task", ex_str)

            ex = try
                fetch(b)
            catch err
                err
            end
            ex_str = sprint(io->Base.showerror(io, ex))
            @test occursin("Test", ex_str)
            @test occursin("Root Task", ex_str)
        end
        @testset "single dependent" begin
            a = @spawn error("Test")
            b = @spawn a+2
            @test_throws_unwrap (Dagger.DTaskFailedException, ErrorException) fetch(a)
        end
        @testset "multi dependent" begin
            a = @spawn error("Test")
            b = @spawn a+2
            c = @spawn a*2
            @test_throws_unwrap (Dagger.DTaskFailedException, ErrorException) fetch(b)
            @test_throws_unwrap (Dagger.DTaskFailedException, ErrorException) fetch(c)
        end
        @testset "dependent chain" begin
            a = @spawn error("Test")
            @test_throws_unwrap (Dagger.DTaskFailedException, ErrorException) fetch(a)
            b = @spawn a+1
            @test_throws_unwrap (Dagger.DTaskFailedException, ErrorException) fetch(b)
            c = @spawn b+2
            @test_throws_unwrap (Dagger.DTaskFailedException, ErrorException) fetch(c)
        end
        @testset "single input" begin
            a = @spawn 1+1
            b = @spawn (a->error("Test"))(a)
            @test fetch(a) == 2
            @test_throws_unwrap (Dagger.DTaskFailedException, ErrorException) fetch(b)
        end
        @testset "multi input" begin
            a = @spawn 1+1
            b = @spawn 2*2
            c = @spawn ((a,b)->error("Test"))(a,b)
            @test fetch(a) == 2
            @test fetch(b) == 4
            @test_throws_unwrap (Dagger.DTaskFailedException, ErrorException) fetch(c)
        end
        @testset "diamond" begin
            a = @spawn 1+1
            b = @spawn a+1
            c = @spawn a*2
            d = @spawn ((b,c)->error("Test"))(b,c)
            @test fetch(a) == 2
            @test fetch(b) == 3
            @test fetch(c) == 4
            @test_throws_unwrap (Dagger.DTaskFailedException, ErrorException) fetch(d)
        end
    end
    if 2 in workers()
        @testset "remote spawn" begin
            a = fetch(Distributed.@spawnat 2 Dagger.@spawn 1+2)
            @test Dagger.Sch.EAGER_INIT[]
            @test fetch(Distributed.@spawnat 2 !(Dagger.Sch.EAGER_INIT[]))
            @test a isa Dagger.DTask
            @test fetch(a) == 3

            # Mild stress-test
            @test dynamic_fib(10) == 55

            # Errors on remote are correctly scrubbed (#430)
            t2 = remotecall_fetch(2) do
                t1 = Dagger.@spawn 1+"fail"
                Dagger.@spawn t1+1
            end
            @test_throws_unwrap (Dagger.DTaskFailedException, MethodError) fetch(t2)
        end
    end
    if nprocs() > 1
        @testset "undefined function" begin
            # Issues #254, #255

            # only defined on head node
            @eval evil_f(x) = x

            eager_thunks = map(1:10) do i
                single = isodd(i) ? 1 : first(workers())
                Dagger.@spawn single=single evil_f(i)
            end

            errored(t) = try
                fetch(t)
                false
            catch
                true
            end
            @test any(t->errored(t), eager_thunks)
            @test any(t->!errored(t), eager_thunks)
        end
    end
    @testset "function chunks" begin
        @testset "lazy API" begin
            a = delayed(+)(1,2)
            @test !(a.f isa Chunk)
            @test a.options.scope == nothing

            a = delayed(+; scope=NodeScope())(1,2)
            @test !(a.f isa Chunk)
            @test a.options.scope isa NodeScope

            @testset "Scope Restrictions" begin
                pls = ProcessLockedStruct(Ptr{Int}(42))
                ctx = Context([1, workers()...])

                # Negative test
                @test_skip !all(x->x==43, collect(ctx, delayed(vcat)([delayed(pls)(1) for i in 1:10]...)))
                # Positive tests (no serialization)
                @test all(x->x==43, collect(ctx, delayed(vcat)([delayed(pls; scope=ProcessScope())(1) for i in 1:10]...)))
                if nprocs() > 1
                    @test all(x->x==1, collect(ctx, delayed(vcat)([delayed(pls; scope=ProcessScope(first(workers())))(1) for i in 1:10]...)))
                end
            end
            @testset "Processor Data Movement" begin
                @everywhere Dagger.add_processor_callback!(()->MulProc(), :mulproc)
                plus_chunk = Dagger.tochunk(+, MulProc())
                @test collect(delayed(plus_chunk)(3,4)) == 12
                @everywhere Dagger.delete_processor_callback!(:mulproc)
            end
        end
        @testset "eager API" begin
            _a = Dagger.@spawn scope=NodeScope() 1+2
            a = Dagger.Sch._find_thunk(_a)
            @test !(a.f isa Chunk)
            @test a.options.scope isa NodeScope
        end
    end
    @testset "parent fetch child, one thread" begin
        # Issue #282

        s = p -> p == Dagger.ThreadProc(1, 1)
        f = (x) -> 10 + x
        g = (x) -> fetch(Dagger.@spawn proclist=s f(x))
        fetch(Dagger.@spawn proclist=s g(10))
    end
    @testset "no cross-scheduler Thunk usage" begin
        a = delayed(+)(1,2)
        @test_throws Exception Dagger.@spawn identity(a)
    end
    @testset "@sync support" begin
        result = Dagger.@spawn sleep(1)
        @test !isready(result)
        @sync begin
            result = Dagger.@spawn sleep(1)
        end
        @test isready(result)

        @test_throws Exception @sync begin
            Dagger.@spawn error()
        end
    end
    @testset "fetch_all" begin
        ts = [Dagger.@spawn(1+1) for _ in 1:4]
        @test Dagger.fetch_all(ts) == [2, 2, 2, 2]
        cs = map(t->fetch(t; raw=true), ts)
        @test Dagger.fetch_all(cs) == [2, 2, 2, 2]

        ts = Tuple(Dagger.@spawn(1+1) for _ in 1:4)
        @test Dagger.fetch_all(ts) == (2, 2, 2, 2)
        cs = fetch.(ts; raw=true)
        @test Dagger.fetch_all(cs) == (2, 2, 2, 2)

        t = Dagger.@spawn 1+1
        @test Dagger.fetch_all(t) == 2
        @test Dagger.fetch_all(fetch(t; raw=true)) == 2
    end
end
