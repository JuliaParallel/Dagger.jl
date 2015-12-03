using Base.Test

addprocs(1)
using ComputeFramework

input = rand(1:10, 100)
x = Partitioned(input)

ctx = Context()
# Keep this part computed (cached). Since we will reuse it later
chunk_cumsums = compute(ctx, mappart(cumsum, x)) # mappart applies a function on the whole chunk

last_sum = gather(ctx, mappart(x -> x[end], chunk_cumsums)) # Gather will get the data to the running process
prev_last_sum = vcat(0, last_sum[1:end-1])

result = gather(ctx, mappart((x, y) -> x+y[1], chunk_cumsums, Partitioned(prev_last_sum)))

@test cumsum(input) == result
