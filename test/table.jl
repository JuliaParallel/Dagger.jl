using DataFrames
using Arrow
using CSV

@testset "dtable" begin
    @testset "constructors - Tables.jl compatibility (NamedTuple)" begin
        size = 1_000
        nt = (a = rand(Int, size), b = rand(Int, size))

        dt = DTable(nt, 100)
        @test fetch(dt) == nt
        @test fetch(dt, NamedTuple) == nt
        @test tabletype(dt) == NamedTuple

        # empty dtable case
        dt = DTable((a = [], b = []), 10)
        @test fetch(dt) == NamedTuple()
    end

    @testset "constructors - Tables.jl compatibility (DataFrames)" begin
        size = 1_000
        nt = (a = rand(Int, size), b = rand(Int, size))
        df = DataFrame(nt)

        dt = DTable(df, 100)
        @test fetch(dt) == df
        @test fetch(dt, DataFrame) == df
        @test tabletype(dt) == DataFrame

        dt = DTable(nt, 100; tabletype=DataFrame)
        @test fetch(dt) == df
        @test fetch(dt, DataFrame) == df
        @test tabletype(dt) == DataFrame

        dt = DTable(df, 100; tabletype=NamedTuple)
        @test fetch(dt) == nt
        @test fetch(dt, DataFrame) == df
        @test tabletype(dt) == NamedTuple
    end

    @testset "constructors - Tables.jl compatibility (Arrow)" begin
        size = 1_000
        nt = (a = rand(Int, size), b = rand(Int, size))
        io = IOBuffer()
        Arrow.write(io, nt)
        arr = Arrow.Table(take!(io))

        dt = DTable(arr, 100)
        @test fetch(dt) == nt
        @test fetch(dt, NamedTuple) == nt
        @test tabletype(dt) == NamedTuple # NamedTuple is the type obtained using the sink function of Arrow.Table
    end

    @testset "constructors - Tables.jl compatibility (CSV)" begin
        size = 1_000
        nt = (a = rand(Int, size), b = rand(Int, size))
        io = IOBuffer()
        CSV.write(io, nt)
        dt1 = CSV.read(take!(io), (csv -> DTable(csv, 100)))
        CSV.write(io, nt)
        # or
        dt2 = DTable(CSV.File(take!(io)), 100)
        @test fetch(dt1) == nt
        @test fetch(dt1, NamedTuple) == nt
        @test tabletype(dt1) == NamedTuple
        @test fetch(dt2) == nt
        @test fetch(dt2, NamedTuple) == nt
        @test tabletype(dt2) == NamedTuple
    end

    @testset "constructors - file input" begin
        n = 20
        size = 1000
        ios = [IOBuffer() for _ in 1:n]
        data = [(a = rand(size), b = rand(size)) for _ in 1:n]
        arr = [Arrow.write(ios[idx], data[idx]) for idx in 1:n]

        da = DTable(x -> Arrow.Table(take!(ios[tryparse(Int64, x)])), [string(i) for i in 1:n])
        db = vcat([DataFrame(d) for d in data]...)
        @test fetch(da, DataFrame) == db
        @test Dagger.resolve_tabletype(da) == NamedTuple
        @test da.tabletype === nothing
        tabletype!(da)
        @test da.tabletype === NamedTuple
    end

    @testset "map" begin
        size = 1_000
        nt = (a = rand(size), b = rand(size))

        df = DataFrame(nt)
        for src in [nt, df]
            dt = DTable(src, 100)

            # Single result 
            mt = map(x -> (r = x.a + x.b, ), dt)
            mf = map(x -> x.a + x.b, eachrow(df))
            @test fetch(mt).r == mf

            # Two results
            mt = map(x -> (r1 = x.a + x.b, r2 = x.a - x.b), dt)
            mf = combine(df, [:a, :b] => ((a, b) -> a .+ b) => :r1, [:a, :b] => ((a, b) -> a .- b) => :r2)
            @test fetch(mt).r1 == mf.r1
            @test fetch(mt).r2 == mf.r2
        end

        # Map an empty dtable
        dt = DTable((a = [], b = []), 10)
        m = map(x -> (r = x.a + x.b), dt)
        @test fetch(m) == NamedTuple()
    end

    @testset "filter" begin
        size = 1_000
        nt = (a = rand(size), b = rand(size))

        df = DataFrame(nt)

        for src in [nt, df]
            dt = DTable(src, 100)

            dfr = filter(x -> (x.a .> 0.5) .& (x.b .< 0.5), df)
            dtr = filter(x -> (x.a .> 0.5) .& (x.b .< 0.5), dt)
            @test fetch(dtr, DataFrame) == dfr
        end

        # Filter an empty DTable
        dt = DTable((a = [], b = []), 10)
        f = filter(x -> x.a .> 0.5, dt)
        @test fetch(f) == NamedTuple()
    end

    @testset "reduce" begin
        size = 1_000
        nt = (a=rand(Int, size) .% 100, b=rand(Int, size) .% 100)

        df = DataFrame(nt)
        dtdf = DTable(df, 100)
        dtnt = DTable(nt, 100)

        dtdf1 = reduce(+, dtdf, cols=[:a])
        dtnt1 = reduce(+, dtnt, cols=[:a])
        df1 = reduce((x, y) -> x + y.a, eachrow(df); init=0)

        dtdf2 = reduce(+, map(x -> (r = x.a * x.b,), dtdf), cols=[:r])
        dtnt2 = reduce(+, map(x -> (r = x.a * x.b,), dtnt), cols=[:r])
        df2 = reduce((x, y) -> x + y.a * y.b, eachrow(df);init=0)

        dtdf3 = reduce(+, map(x -> (r = x.a + x.b,), dtdf), cols=[:r])
        dtnt3 = reduce(+, map(x -> (r = x.a + x.b,), dtnt), cols=[:r])
        df3 = reduce((x, y) -> x + y.a + y.b, eachrow(df);init=0)

        dtdf4 = reduce(*, dtdf, cols=[:a])
        dtnt4 = reduce(*, dtnt, cols=[:a])
        df4 = reduce((x, y) -> x * y.a, eachrow(df); init=1)

        @test fetch(dtdf1).a == fetch(dtnt1).a == df1
        @test fetch(dtdf2).r == fetch(dtnt2).r == df2
        @test fetch(dtdf3).r == fetch(dtnt3).r == df3
        @test fetch(dtdf4).a == fetch(dtnt4).a == df4

        all_reduce = reduce(+, dtdf)
        df5 = reduce((x, y) -> x + y.b, eachrow(df); init=0)

        @test fetch(all_reduce).a == df1
        @test fetch(all_reduce).b == df5
    end

    @testset "chaining ops" begin
        nt = (a=1:100, b=(1:100))

        d = DTable(nt, 2)
        f1 = filter(x -> iseven.(x.a), d)
        m1 = map(x -> (r = x.a + x.b,), f1)
        f2 = filter(x -> x.r > 50, m1)
        r = reduce(+, f2)
        @test fetch(r).r == 4788
        r = reduce(*, f2, init=BigInt(1))
        @test fetch(r).r == reduce(*, fetch(f2).r, init=BigInt(1))
    end

    @testset "trim" begin
        nt = (a=1:100, b=(1:100))

        d = DTable(nt, 2)
        fd = filter(x -> x.a > 49, d)

        @test length(d.chunks) == length(fd.chunks)
        @test length(trim(fd).chunks) == 26
        trim!(fd)
        @test length(fd.chunks) == 26

        fd2 = filter(x -> x.a > 100, d)
        @test length(d.chunks) == length(fd2.chunks)
        @test length(trim(fd2).chunks) == 0
        trim!(fd2)
        @test length(fd2.chunks) == 0
    end

    @testset "tabletype" begin
        nt = (a=1:100, b=(1:100))
        d = DTable(nt, 2)
        @test tabletype(d) == NamedTuple
        d.tabletype = nothing
        @test Dagger.resolve_tabletype(d) == NamedTuple
        tabletype!(d)
        @test d.tabletype == NamedTuple

        # empty dtable case
        dt = DTable((a = [], b = []), 10)
        @test tabletype(dt) == NamedTuple # fallback in case it can't be found
    end

    @testset "groupby" begin
        d = DTable((a=repeat(['a','b','c','d'], 6),), 4)
        g = Dagger.groupby(d, :a) # merge=true, chunksize=0
        @test length(g.chunks) == 4
        g = Dagger.groupby(d, :a, chunksize=1)
        @test length(g.chunks) == 24
        g = Dagger.groupby(d, :a, merge=false)
        @test length(g.chunks) == 24
        g = Dagger.groupby(d, :a, chunksize=2)
        @test length(g.chunks) == 12
        g = Dagger.groupby(d, :a, chunksize=3)
        @test length(g.chunks) == 8
        g = Dagger.groupby(d, :a, chunksize=6)
        @test length(g.chunks) == 4
    end
end
