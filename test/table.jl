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
    end

    @testset "map" begin
        size = 1_000
        nt = (a = rand(size), b = rand(size))
        s = DataFrame(nt)
        d = DTable(s, 100)
        sm = map(x -> x.a + x.b, eachrow(s))
        dm = map(x -> x.a + x.b, eachrow(d))
        @test fetch(dm) == sm 
        d = DTable(nt, 100)
        dm = map(x -> x.a + x.b, eachrow(d))
        @test fetch(dm) == sm
    end

    @testset "filter" begin
        size = 1_000
        nt = (a = rand(size), b = rand(size))
        df = DataFrame(nt)
        dt = DTable(df, 100)
        dfr = filter(x -> x.a > 0.5, df)
        dtr = filter(x -> x.a > 0.5, dt)
        @test fetch(dtr) == dfr
        dt = DTable(nt, 100)
        dtr = filter(x -> x.a > 0.5, dt)
        @test fetch(dtr, DataFrame) == dfr
    end

    @testset "reduce" begin
        size = 1_000
        nt = (a=rand(Int, size) .% 1000, b=rand(Int, size) .% 1000)

        s = DataFrame(nt)
        d = DTable(s, 100)
        dn = DTable(nt, 100)

        dr1 = reduce(+, y -> y.a, eachrow(d);init=0)
        dn1 = reduce(+, y -> y.a, eachrow(dn);init=0)
        sr1 = reduce((x, y) -> x + y.a, eachrow(s);init=0)

        dr2 = reduce(+, y -> y.a * y.b, eachrow(d);init=0)
        dn2 = reduce(+, y -> y.a * y.b, eachrow(dn);init=0)
        sr2 = reduce((x, y) -> x + y.a * y.b, eachrow(s);init=0)

        dr3 = reduce(+, y -> y.a + y.b, eachrow(d);init=0)
        dn3 = reduce(+, y -> y.a + y.b, eachrow(dn);init=0)
        sr3 = reduce((x, y) -> x + y.a + y.b, eachrow(s);init=0)

        dr4 = reduce(*, y -> y.a, eachrow(d);init=0)
        dn4 = reduce(*, y -> y.a, eachrow(dn);init=0)
        sr4 = reduce((x, y) -> x * y.a, eachrow(s);init=0)

        @test fetch(dr1) == fetch(dn1) == sr1
        @test fetch(dr2) == fetch(dn2) == sr2
        @test fetch(dr3) == fetch(dn3) == sr3
        @test fetch(dr4) == fetch(dn4) == sr4
    end
end
