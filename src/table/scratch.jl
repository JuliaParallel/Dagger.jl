using Dagger, DataFrames
genchunk = (rng) -> (; [Symbol("a$i") => rand(rng, Int32(1):Int32(unique_values), n ÷ nchunks) for i in 1:ncolumns]...)
n = Int(1e7)
unique_values = Int(1e3)
using Random
rng = MersenneTwister(1111)

ncolumns = 4
max_chunksize = Int(1e6)
nchunks = (n + max_chunksize - 1) ÷ max_chunksize
d1 = DTable([Dagger.@spawn genchunk(MersenneTwister(1111 + i)) for i in 1:nchunks], NamedTuple)

d2 = fetch(d1, DataFrame)

select(d2, :a1 => sum)
precalc_column_val = sum(Tables.getcolumn(d1, :a1)) # @spawn probably
dt = map(r -> (a1_sum=precalc_column_val,), d1)
fetch(dt, DataFrame)


select(d2, AsTable([:a1, :a2]) => ByRow(sum))
dt = map(r -> (a1_a2_sum=sum(Tables.getcolumn.(Ref(r), [:a1, :a2])),), d1)
fetch(dt, DataFrame)





combine(d2, :a1 => ByRow(sin) => :c, :a2)
dt = map(r -> (c = sin(Tables.getcolumn(r, :a1)), a2 = Tables.getcolumn(r, :a2),), d1)
fetch(dt, DataFrame)


combine(d2, :a1 => sum => :c, :a2)
precalc_column_val = sum(Tables.getcolumn(d1, :a1)) # @spawn probably
# length check?
dt = map(r -> (c = precalc_column_val, a2 = Tables.getcolumn(r, :a2)), d1)
fetch(dt, DataFrame)


combine(d2, :a1 => sum => :c)
precalc_column_val = sum(Tables.getcolumn(d1, :a1)) # @spawn probably
# partitioning here is a problem, we don't know the size of the result and all columns of a row need to be in the same chunk
dt = DTable((c = [precalc_column_val...], ), 1000)
fetch(dt, DataFrame)

