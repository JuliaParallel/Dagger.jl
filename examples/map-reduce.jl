addprocs(2)

# Demonstration of map-reduce capabilities

using ComputeFramework

arr = [i for i in 1:9]
darr = compute(Distribute(BlockPartition(1), arr))

# Basic reduce operations
@show compute(sum(darr))
@show compute(prod(darr))
@show compute(mean(darr))

# Find squares of numbers
@show gather(map(x->x^2, darr))

# Sum of digits
@show compute(reduce((x,y)->x*10+y, darr))

# Find sum of squares
@show compute(mapreduce(x->x^2, +, darr))
