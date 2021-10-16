using DataFrames
using Arrow
using CSV
using Random

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

    @testset "dtable groupby basic" begin
        rng = MersenneTwister(2137)
        charset = collect('a':'d')
        cs1 = shuffle(rng, repeat(charset, inner=4, outer=4))
        cs2 = shuffle(rng, repeat(charset, inner=4, outer=4))

        kwargs_set = [
            (;)
            (chunksize=1,)
            (merge=false,)
            (chunksize=8,)
            (chunksize=16,)
            (chunksize=20,)
        ]

        ######################################################
        # single col groupby
        d = DTable((a=cs1,), 4)

        for kwargs in kwargs_set
            println("iter")
            g = Dagger.groupby(d, :a; kwargs...)
            c = Dagger._retrieve.(g.dtable.chunks)
            @test all([all(t.a[1] .== t.a) for t in c])
            @test all(getindex.(getproperty.(c, :a), 1) .∈ Ref(charset))
            @test sort(collect(fetch(d).a)) == sort(collect(fetch(g).a))
        end

        ######################################################
        # multi col groupby
        d = DTable((a=cs1, b=cs2), 4) 

        for kwargs in kwargs_set
            g = Dagger.groupby(d, [:a, :b]; kwargs...)
            c = Dagger._retrieve.(g.dtable.chunks)
            @test all([all(t.a[1] .== t.a) for t in c])
            @test all([all(t.b[1] .== t.b) for t in c])
            @test all(getindex.(getproperty.(c, :a), 1) .∈ Ref(charset))
            @test all(getindex.(getproperty.(c, :a), 1) .∈ Ref(charset))
            fd = fetch(d)
            fg = fetch(g)
            @test sort(collect(fd.a)) == sort(collect(fg.a))
            @test sort(collect(fd.b)) == sort(collect(fg.b))
        end

        ######################################################
        # function groupby
        intset = collect(10:29)
        is1 = shuffle(rng, repeat(intset, 4))
        d = DTable((a=is1,), 4)

        f1 = x -> x.a % 10
        f2 = x -> x % 10
        for kwargs in kwargs_set
            g = Dagger.groupby(d, f1)
            c = Dagger._retrieve.(g.dtable.chunks)
            @test all([all(f2(t.a[1]) .== f2.(t.a)) for t in c])
            @test all(getindex.(getproperty.(c, :a), 1) .∈ Ref(intset))
            @test sort(collect(fetch(d).a)) == sort(collect(fetch(g).a))
        end
    end

    @testset "dtable groupby index check" begin
        rng = MersenneTwister(2137)
        charset = collect('a':'d')
        cs1 = shuffle(rng, repeat(charset, inner=4, outer=4))

        d = DTable((a=cs1,), 4)
        g = Dagger.groupby(d, :a)

        for key in keys(g.index)
            chunk_indices = g.index[key]
            chunks = getindex.(Ref(g.dtable.chunks), chunk_indices)
            parts = Dagger._retrieve.(chunks)

            @test all([all(key .== p.a) for p in parts])
        end
    end

    @testset "dtable groupby ops" begin
        rng = MersenneTwister(2137)
        charset = collect('a':'d')
        cs1 = shuffle(rng, repeat(charset, inner=4, outer=4))
        is1 = [3 for _ in 1:length(cs1)]

        d = DTable((a=cs1, b=is1), 4)
        g = Dagger.groupby(d, :a, chunksize=4)

        m = map(x -> (a = x.a, result = x.a + x.b), g)

        for key in keys(m.index)
            chunk_indices = m.index[key]
            chunks = getindex.(Ref(m.dtable.chunks), chunk_indices)
            parts = Dagger._retrieve.(chunks)
            @test all([all((key + 3) .== p.result) for p in parts])
            @test all(fetch(m[key]).a .== key)
        end

        r = reduce(*, g)
        fr = fetch(r)

        for (i, key) in enumerate(fr.a)
            @test fr.result_a[i] == repeat(key, length(cs1) ÷ 4)
            @test fr.result_b[i] == 3 ^ (length(cs1) ÷ 4)
        end

        f = filter(x -> x.a ∈ ['a', 'b'], g)
        @test ['c', 'd'] ∉ fetch(f).a

        t = trim(f)
        @test ['c', 'd'] ∉ collect(keys(t.index))

        trim!(f)
        @test ['c', 'd'] ∉ collect(keys(f.index))
    end
end
