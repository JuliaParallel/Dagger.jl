using Dagger
using Random
using BenchmarkTools
using OnlineStats

rng = MersenneTwister(1111)

n = 10_000_0000
max_chunksize = 1_000_00

# data = (;[Symbol("a$i") => abs.(rand(rng,Int64, n)).%10_00 for i in 1:10]...)
f = () -> while true sleep(0.2); println(length(Dagger.Sch.EAGER_STATE.x.running)) end
Dagger.@spawn 10+10
@async f()
# create
d = DTable((;[Symbol("a$i") => abs.(rand(rng,Int64, n)).%1_000 for i in 1:1]...), max_chunksize)


# elementwise
# m = map(row -> (r = row.a1 + 1 + 2 + 3 + 4 + 5 + 6 + 7 + 8 + 9 + 10,), d)

# @btime let
#     m = map(row -> (r = row.a1 + 1 + 2 + 3 + 4 + 5 + 6 + 7 + 8 + 9 + 10,), d)
#     wait.(m.chunks)
# end


# r = reduce(fit!, d, init=Variance()) # full dataset
# fetch(r)
# r = reduce(fit!, d, cols=[:a1], init=Variance()) # 1 col
# fetch(r)

# @btime let 
#     r = reduce(fit!, d, init=Variance())
#     fetch(r)
# end

# @btime let 
#     r = reduce(fit!, d, cols=[:a1], init=Variance())
#     fetch(r)
# end

g = @time let
    Dagger.groupby(d, :a1)
end 

g = @time let
    Dagger.groupby(d, :a1)
end 

# @btime let
#     r = reduce(fit!, g, cols=[:a1], init=Mean())
#     fetch(r)
# end samples=1

# @btime let
#     r = reduce(fit!, Dagger.groupby(d, :a1), cols=[:a1], init=Mean())
#     fetch(r)
# end samples=1


