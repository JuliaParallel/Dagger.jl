# Stencil Operations

The `@stencil` macro in Dagger.jl provides a convenient way to perform stencil computations on `DArray`s. It operates within a `Dagger.spawn_datadeps()` block and allows you to define operations that apply to each element of a `DArray`, potentially considering its neighbors.

## Basic Usage

The fundamental structure of a `@stencil` block involves iterating over an implicit index, `idx`, which represents the coordinates of an element in the processed `DArray`s.

```julia
using Dagger
import Dagger: @stencil, Wrap, Pad

# Initialize a DArray
A = zeros(Blocks(2, 2), Int, 4, 4)

Dagger.spawn_datadeps() do
    @stencil begin
        A[idx] = 1 # Assign 1 to every element of A
    end
end

@assert all(collect(A) .== 1)
```

In this example, `A[idx] = 1` is executed for each chunk of `A`. The `idx` variable corresponds to the indices within each chunk.

## Neighborhood Access with `@neighbors`

The true power of stencils comes from accessing neighboring elements. The `@neighbors` macro facilitates this.

`@neighbors(array[idx], distance, boundary_condition)`

- `array[idx]`: The array and current index from which to find neighbors.
- `distance`: An integer specifying the extent of the neighborhood (e.g., `1` for a 3x3 neighborhood in 2D).
- `boundary_condition`: Defines how to handle accesses beyond the array boundaries. Common conditions are:
    - `Wrap()`: Wraps around to the other side of the array.
    - `Pad(value)`: Pads with a specified `value`.

### Example: Averaging Neighbors with `Wrap`

```julia
# Initialize a DArray
A = ones(Blocks(1, 1), Int, 3, 3)
A[2,2] = 10 # Central element has a different value
B = zeros(Blocks(1, 1), Float64, 3, 3)

Dagger.spawn_datadeps() do
    @stencil begin
        # Calculate the average of the 3x3 neighborhood (including the center)
        B[idx] = sum(@neighbors(A[idx], 1, Wrap())) / 9.0
    end
end

# Manually calculate expected B for verification
expected_B = zeros(Float64, 3, 3)
A_collected = collect(A)
for r in 1:3, c in 1:3
    local_sum = 0.0
    for dr in -1:1, dc in -1:1
        nr, nc = mod1(r+dr, 3), mod1(c+dc, 3)
        local_sum += A_collected[nr, nc]
    end
    expected_B[r,c] = local_sum / 9.0
end

@assert collect(B) ≈ expected_B
```

### Example: Convolution with `Pad`

```julia
# Initialize a DArray
A = ones(Blocks(2, 2), Int, 4, 4)
B = zeros(Blocks(2, 2), Int, 4, 4)

Dagger.spawn_datadeps() do
    @stencil begin
        B[idx] = sum(@neighbors(A[idx], 1, Pad(0))) # Pad with 0
    end
end

# Expected result for a 3x3 sum filter with zero padding
expected_B_padded = [
    4 6 6 4;
    6 9 9 6;
    6 9 9 6;
    4 6 6 4
]
@assert collect(B) == expected_B_padded
```

## Sequential Semantics

Expressions within a `@stencil` block are executed sequentially in terms of their effect on the data. This means that the result of one statement is visible to the subsequent statements, as if they were applied "all at once" across all indices before the next statement begins.

```julia
A = zeros(Blocks(2, 2), Int, 4, 4)
B = zeros(Blocks(2, 2), Int, 4, 4)

Dagger.spawn_datadeps() do
    @stencil begin
        A[idx] = idx[1] + idx[2]  # First, A is filled based on coordinates
        B[idx] = A[idx] * 2       # Then, B is computed using the new values of A
    end
end

expected_A = [(r+c) for r in 1:4, c in 1:4]
expected_B_seq = expected_A .* 2

@assert collect(A) == expected_A
@assert collect(B) == expected_B_seq
```

## Operations on Multiple `DArray`s

You can read from and write to multiple `DArray`s within a single `@stencil` block, provided they have compatible chunk structures.

```julia
A = ones(Blocks(1, 1), Int, 2, 2)
B_multi = Dagger.fill(Blocks(1, 1), 2, Int, 2, 2) # Renamed to avoid conflict, corrected fill
C = zeros(Blocks(1, 1), Int, 2, 2)

Dagger.spawn_datadeps() do
    @stencil begin
        C[idx] = A[idx] + B_multi[idx] # Use the renamed B_multi
    end
end
@assert all(collect(C) .== 3)
```

## Example: Game of Life

The following demonstrates a more complex example: Conway's Game of Life.

```julia
# Ensure Plots and other necessary packages are available for the example
# using Plots

N = 27 # Size of one dimension of a tile
nt = 3 # Number of tiles in each dimension (results in nt x nt grid of tiles)
niters = 10 # Number of iterations for the animation

tiles = zeros(Blocks(N, N), Bool, N*nt, N*nt)
outputs = zeros(Blocks(N, N), Bool, N*nt, N*nt)

# Create a fun initial state (e.g., a glider and some random noise)
tiles[13, 14] = true
tiles[14, 14] = true
tiles[15, 14] = true
tiles[15, 15] = true # Corrected glider part
tiles[14, 16] = true
# Add some random noise in one of the tiles
# Make sure to use Dagger-compatible assignment if you were to modify chunks directly
# For simplicity, direct array indexing is used here for initial setup.
rand_tile_data = rand(Bool, N, N)
# To assign this to a specific block, you'd typically work with chunks,
# but for initial setup, direct indexing on the collected array or careful DArray construction is easier.
# For this example, we'll simplify and assume direct modification is for setup.
# A Dagger-idiomatic way for partial modification might involve map! or similar.
# Here, we just modify the underlying array before it's heavily used by Dagger tasks if possible,
# or use Dagger operations.
# For collected view for setup:
temp_tiles = collect(tiles) # This collect is fine for initial setup visualization/modification
temp_tiles[(2N+1):3N, (2N+1):3N] .= rand_tile_data
tiles = Dagger.distribute(temp_tiles, Blocks(N,N)) # Use distribute to create DArray from existing array


# The animation part requires a graphical environment.
# If running in a headless environment, you might comment out the @animate macro
# and inspect `outputs` programmatically.
# anim = @animate for _ in 1:niters
#     Dagger.spawn_datadeps() do
#         @stencil begin
#             outputs[idx] = begin
#                 nhood = @neighbors(tiles[idx], 1, Wrap())
#                 live_neighbors = sum(nhood) - tiles[idx] # Subtract self if it was counted
#                 if tiles[idx] # If current cell is alive
#                     if live_neighbors < 2 || live_neighbors > 3
#                         false # Dies by underpopulation or overpopulation
#                     else
#                         true  # Survives
#                     end
#                 else # If current cell is dead
#                     if live_neighbors == 3
#                         true  # Becomes alive by reproduction
#                     else
#                         false # Stays dead
#                     end
#                 end
#             end
#             tiles[idx] = outputs[idx] # Update tiles for the next iteration
#         end
#     end
#     # heatmap(Int.(collect(outputs))) # Visualize (requires Plots.jl)
# end
# path = mp4(anim; fps=5, show_msg=true).filename # Save animation (requires Plots.jl)

# For testing without animation:
# Execute one iteration:
Dagger.spawn_datadeps() do
    @stencil begin
        outputs[idx] = begin
            nhood = @neighbors(tiles[idx], 1, Wrap())
            live_neighbors = sum(nhood) - tiles[idx]
            if tiles[idx]
                if live_neighbors < 2 || live_neighbors > 3; false
                else; true; end
            else
                if live_neighbors == 3; true
                else; false; end
            end
        end
        tiles[idx] = outputs[idx]
    end
end
# You can inspect `collect(outputs)` or `collect(tiles)` here.
println("Game of Life example processed one iteration.")
```

This updated documentation provides a more structured explanation of `@stencil`, including its syntax, common use cases like neighborhood access with different boundary conditions, the sequential nature of its operations, and how to use it with multiple `DArray`s. The Game of Life example is also slightly corrected and clarified.
