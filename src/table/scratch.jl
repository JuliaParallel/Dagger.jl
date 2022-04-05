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

select(d2,
    :a1,
    :a1 => sum,
    AsTable([:a1, :a2]) => ByRow(sum)
)

precalc_column_val = Dagger.@spawn ((d) -> sum(Tables.getcolumn(d, :a1)))(d1)
val = fetch(precalc_column_val)

dt = map(r -> (
        a1=r.a1,
        a1_sum=val,
        a1_a2_sum=sum(Tables.getcolumn.(Ref(r), [:a1, :a2]))
    ), d1)
fetch(dt, DataFrame)


# for combine length checks are needed and more logic

combine(d2, :a1 => ByRow(sin) => :c,)
dt = map(r -> (c=sin(Tables.getcolumn(r, :a1)),), d1)
fetch(dt, DataFrame)

combine(d2, :a1 => sum => :c, :a2)
precalc_column_val = Dagger.@spawn ((d) -> sum(Tables.getcolumn(d, :a1)))(d1) # @spawn probably
val = fetch(precalc_column_val)
dt = map(r -> (c=val, a2=r.a2), d1)
fetch(dt, DataFrame)

combine(d2, :a1 => sum => :c)
precalc_column_val = Dagger.@spawn ((d) -> sum(Tables.getcolumn(d1, :a1)))(d1) # @spawn probably
# partitioning here is a problem, we don't know the size of the result and all columns of a row need to be in the same chunk
dt = DTable((c=[fetch(precalc_column_val)...],), 1000)
fetch(dt, DataFrame)


# We have ByRow/AsTable - maybe we could have Reduce to support reduction functions?
# combine(d, :a => Reduce(+))
# dt = DTable((a_reduce = [fetch(reduce(+, d, cols=:a))...],), 100)
# or 
# using OnlineStats; combine(d, :a => Reduce(fit!, init=Mean()))
