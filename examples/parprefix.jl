using Base.Test

addprocs(2)
using Dagger

ctx = Context()

input = rand(1:10, 100)
x = distribute(input)

# Keep this part computed (cached). Since we will reuse it later
chunk_cumsums = compute(ctx, mappart(cumsum, x))  # mappart applies a function on the whole chunk
last_sum = shift(mappart(last, chunk_cumsums), 0) # Shift the last element of chunk results rightward. leftmost chunk gets 0
result = mappart(+, chunk_cumsums, last_sum)      # Add shifted last sum to each chunk

@test cumsum(input) == gather(ctx, result)
