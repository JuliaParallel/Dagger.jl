using DataFrames
using Arrow
using CSV
using Random
using Tables
using TableOperations
using OnlineStats

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

    @testset "mapreduce" begin
        nt = (a=collect(1:100).%10, b=rand(100))
        d1 = DTable(nt, 25)
        d2 = DataFrame(nt)
        gg = GroupBy(Int, Mean())
        r1 = fetch(mapreduce(x -> (x.a, x.b), fit!, d1, init=gg))
        r2 = combine(groupby(d2, :a), :b => mean)
        r1t = DataFrame(((r) -> (a=r[1], b_mean=r[2].μ)).(collect(r1.value)))
        sort!(r1t)
        sort!(r2)
        @test isapprox(r1t.b_mean, r2.b_mean)

        r3 = fetch(mapreduce(sum, fit!, d1, init = Mean()))
        r4 = combine(select(d2, AsTable(:) => ByRow(sum) => :sum), :sum => mean)
        @test isapprox(r3.μ, r4.sum_mean[1])
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

    @testset "tables.jl source" begin
        nt = (a=1:100, b=1:100)

        d1 = DTable(nt, 10) # standard row based constructor

        # partition constructor, check with DTable as input
        d2 = DTable(d1)
        d3 = DTable(Tables.partitioner(identity, [nt for _ in 1:10]))

        @test length(d1.chunks) == length(d2.chunks) == length(d3.chunks)

        @test Tables.getcolumn(d1, 1) == 1:100
        @test Tables.getcolumn(d1, 2) == 1:100
        @test Tables.getcolumn(d1, :a) == 1:100
        @test Tables.getcolumn(d1, :b) == 1:100
        @test Dagger.determine_columnnames(d1) == (:a, :b)

        @test Dagger.determine_schema(d1).names == (:a, :b)
        @test Dagger.determine_schema(d1).types == (Int, Int)

        for c in Tables.columns(d1)
            @test c == 1:100
        end

        @test all([ r.a == r.b == v for (r,v) in zip(collect(Tables.rows(d1)),1:100)])

        # length tests for collect on iterators
        @test length(d1) == 100
        @test length(Tables.rows(d1)) == 100
        @test length(Tables.columns(d1)) == 2
        @test length(Tables.partitions(d1)) == 10

        # GDTable things

        g = Dagger.groupby(d1, r -> r.a % 10, chunksize=3)
        t1 = Tables.columntable(Tables.rows(g))
        @test 1:100 == sort(t1.a) == sort(t1.b)
        t2 = collect(Tables.columns(g))
        @test 1:100 == sort(t2[1]) == sort(t2[2])

        for partition in Tables.partitions(g)
            @test partition isa DTable
            v = Tables.getcolumn(partition, :a)[1]
            @test all([el%10 == v%10 for el in Tables.getcolumn(partition, :a)])
        end
    end

    @testset "join" begin
        rng = MersenneTwister(2137)

        a_len = 1000
        b_len = 100

        genkeys = (r, n, m) -> rand(r, Int32, n).%m
        geninds = (n) -> collect(1:n)

        d1_single = DataFrame(a=genkeys(rng, a_len, 100), b=geninds(a_len))
        d2_single = DataFrame(a=genkeys(rng, b_len, 100), c=geninds(b_len))

        d1_mul = DataFrame(a=genkeys(rng, a_len, 10), b=genkeys(rng, a_len, 10), c=geninds(a_len))
        d2_mul = DataFrame(a=genkeys(rng, b_len, 10), b=genkeys(rng, b_len, 10), d=geninds(b_len))


        configs = [
            (d1_single, d2_single, :a),
            (d1_single, d2_single, :a => :a),
            (d1_mul, d2_mul, [:a, :b]),
            (d1_mul, d2_mul, [:a => :a, :b => :b])
        ]

        for (d1, d2, on) in configs
            _, _, _, _, rmatch_indices = Dagger.resolve_colnames(d1, d2, on)
            r_colsymbols = [Tables.columnnames(d2)[s] for s in rmatch_indices]

            d2_lookup = nothing
            for (i, r) in enumerate(Tables.rows(d2))
                key = ([Tables.getcolumn(r, x) for x in rmatch_indices]...,)
                if d2_lookup === nothing
                    d2_lookup = Dict{typeof(key), Vector{UInt}}()
                end
                v = get!(d2_lookup, key, Vector{UInt}())
                push!(v, i)
            end

            lj1 = leftjoin(d1, d2, on=on)
            lj1u = leftjoin(d1, unique(d2, r_colsymbols), on=on)
            lj2 = fetch(leftjoin(DTable(d1, 111), d2, on=on))
            lj3 = fetch(leftjoin(DTable(d1, 111, tabletype=NamedTuple), d2, on=on), DataFrame)
            lj4 = fetch(leftjoin(DTable(d1, 111, tabletype=NamedTuple), sort(d2, r_colsymbols), on=on, r_sorted=true), DataFrame)
            lj5 = fetch(leftjoin(DTable(d1, 111, tabletype=NamedTuple), unique(d2, r_colsymbols), on=on, r_unique=true), DataFrame)
            lj6 = fetch(leftjoin(DTable(sort(d1, r_colsymbols), 111, tabletype=NamedTuple), sort(d2, r_colsymbols), on=on, r_sorted=true, l_sorted=true), DataFrame)
            lj7 = fetch(leftjoin(DTable(sort(d1, r_colsymbols), 111, tabletype=NamedTuple), sort(unique(d2, r_colsymbols), r_colsymbols), on=on, r_sorted=true, l_sorted=true, r_unique=true), DataFrame)
            lj8 = fetch(leftjoin(DTable(d1, 111, tabletype=NamedTuple), d2, on=on, lookup=d2_lookup), DataFrame)
            lj9 = fetch(leftjoin(Dagger.groupby(DTable(d1, 111, tabletype=NamedTuple), r_colsymbols), d2, on=on), DataFrame)
            lj10 = fetch(leftjoin(DTable(d1, a_len ÷ 10), DTable(d2, b_len ÷ 10), on=on), DataFrame)

            sort!.([lj1, lj1u, lj2, lj3, lj4, lj5, lj6, lj7, lj8, lj9, lj10], Ref(propertynames(lj1)))

            @test isequal(lj1, lj2)
            @test isequal(lj1, lj3)
            @test isequal(lj1, lj4)
            @test isequal(lj1u, lj5)
            @test isequal(lj1, lj6)
            @test isequal(lj1u, lj7)
            @test isequal(lj1, lj8)
            @test isequal(lj1, lj9)
            @test isequal(lj1, lj10)

            ij1 = innerjoin(d1, d2, on=on)
            ij1u = innerjoin(d1, unique(d2, r_colsymbols), on=on)
            ij2 = fetch(innerjoin(DTable(d1, 111), d2, on=on))
            ij3 = fetch(innerjoin(DTable(d1, 111, tabletype=NamedTuple), d2, on=on), DataFrame)
            ij4 = fetch(innerjoin(DTable(d1, 111, tabletype=NamedTuple), sort(d2, r_colsymbols), on=on, r_sorted=true), DataFrame)
            ij5 = fetch(innerjoin(DTable(d1, 111, tabletype=NamedTuple), unique(d2, r_colsymbols), on=on, r_unique=true), DataFrame)
            ij6 = fetch(innerjoin(DTable(sort(d1, r_colsymbols), 111, tabletype=NamedTuple), sort(d2, r_colsymbols), on=on, r_sorted=true, l_sorted=true), DataFrame)
            ij7 = fetch(innerjoin(DTable(sort(d1, r_colsymbols), 111, tabletype=NamedTuple), sort(unique(d2, r_colsymbols), r_colsymbols), on=on, r_sorted=true, l_sorted=true, r_unique=true), DataFrame)
            ij8 = fetch(innerjoin(DTable(d1, 111, tabletype=NamedTuple), d2, on=on, lookup=d2_lookup), DataFrame)
            ij9 = fetch(innerjoin(Dagger.groupby(DTable(d1, 111, tabletype=NamedTuple), r_colsymbols), d2, on=on), DataFrame)
            ij10 = fetch(innerjoin(DTable(d1, a_len ÷ 10), DTable(d2, b_len ÷ 10), on=on), DataFrame)
            ij11 = fetch(innerjoin(DTable(d1, a_len ÷ 10), DTable(d2, b_len), on=on), DataFrame)

            sort!.([ij1, ij1u, ij2, ij3, ij4, ij5, ij6, ij7, ij8, ij9, ij10, ij11], Ref(propertynames(ij1)))

            @test isequal(ij1, ij2)
            @test isequal(ij1, ij3)
            @test isequal(ij1, ij4)
            @test isequal(ij1u, ij5)
            @test isequal(ij1, ij6)
            @test isequal(ij1u, ij7)
            @test isequal(ij1, ij8)
            @test isequal(ij1, ij9)
            @test isequal(ij1, ij10)
            @test isequal(ij1, ij11)
        end
    end
end
