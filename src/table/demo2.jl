using DataFrames
using Dagger
using Statistics

r(d::Dagger.DTable, args...) = fetch(Dagger.select(d, args...), DataFrame)

r(d::DataFrames.DataFrame, args...) = (DataFrames.select(d, args...);)

t(d::Dagger.DTable, args...) = @time wait(Dagger.select(d, args...))

t(d::DataFrames.DataFrame, args...) = (@time DataFrames.select(d, args...); nothing)

partitions = 100
s = 1_000_000
nt = (a=collect(1:s) .% 3, b=rand(s))
dt = DTable(nt, s ÷ partitions)
df = fetch(dt, DataFrame)


r(dt, :a => :eee)
r(df, :a => :eee)

t(dt, :b, :a, AsTable([:a, :b]) => ByRow(sum))
t(df, :b, :a, AsTable([:a, :b]) => ByRow(sum))

## DTable Column wrapper speed
@time select(df, :b => mean)
@time mean(Dagger.DTableColumn(dt, 2))
t(dt, :b => mean)

r(dt, AsTable([:a, :b]) => ByRow(identity))
r(dt, [:a, :a] => ((x, y) -> x .+ y), :b)
r(dt, [:a, :a] => ((x, y) -> x .+ y)) # this broken

r(dt, [] => ByRow(() -> 1) => :x, :b) # make this work

r(dt, [] => ByRow(rand) => :x) # make this work

r(dt, :a => mean)

@time map(row -> (a=row.a, b=row.b), dt)


