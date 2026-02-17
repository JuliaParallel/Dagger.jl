# Stencil Operations

The `@stencil` macro in Dagger.jl provides a convenient way to perform stencil computations on `DArray`s. It automatically wraps its operations in a `Dagger.spawn_datadeps()` block and allows you to define operations that apply to each element of a `DArray`, potentially accessing values from each element's neighbors.

## Basic Usage

The fundamental structure of a `@stencil` block involves iterating over an implicit index, named `idx` in the following example, which represents the coordinates of an element in the processed `DArray`s.

### Block Syntax

Multiple stencil operations can be performed within a `begin...end` block:

```julia
using Dagger
import Dagger: @stencil, Wrap, Pad, Reflect, Clamp, LinearExtrapolate

# Initialize a DArray
A = zeros(Blocks(2, 2), Int, 4, 4)

@stencil begin
    A[idx] = 1 # Assign 1 to every element of A
end

@assert all(collect(A) .== 1)
```

### Single-Expression Syntax

If only one stencil operation is needed, the `begin...end` block can be omitted:

```julia
B = zeros(Blocks(2, 2), Int, 4, 4)
@stencil B[idx] = 2
@assert all(collect(B) .== 2)
```

In these examples, the stencil operation is executed for each chunk of the target array. The `idx` variable corresponds to the indices within each chunk.

## Allocation and Return Syntax

The `@stencil` macro also supports a functional syntax that automatically allocates an output `DArray` and returns it. This is triggered when the stencil expression (or the last expression in a block) is a "naked" expression (not an assignment).

```julia
A = ones(Blocks(2, 2), Int, 4, 4)

# Automatically allocates B as a DArray similar to A
B = @stencil sum(@neighbors(A[idx], 1, Wrap()))

@assert B isa DArray
@assert all(collect(B) .== 9)
```

This syntax can also be used as the last expression in a block:

```julia
C = @stencil begin
    A[idx] = A[idx] + 1
    sum(@neighbors(A[idx], 1, Wrap())) # Returns a new DArray
end
```

## Broadcast Integration

Because `@stencil` can return a `DArray`, it can be used directly within Julia's broadcast expressions:

```julia
A = ones(Blocks(2, 2), Int, 4, 4)
B = ones(Blocks(2, 2), Int, 4, 4)
C = zeros(Blocks(2, 2), Int, 4, 4)

# Combine broadcasted addition with a stencil operation
C .= A .+ @stencil(sum(@neighbors(B[idx], 1, Wrap())))

@assert all(collect(C) .== 10)
```

## Neighborhood Access with `@neighbors`

The true power of stencils comes from accessing neighboring elements. The `@neighbors` macro facilitates this.

`@neighbors(array[idx], distance, boundary_condition)`

- `array[idx]`: The array and current index from which to find neighbors.
- `distance`: An integer or `Tuple` of integers specifying the extent of the neighborhood (e.g., `1` for a 3x3 neighborhood in 2D).
- `boundary_condition`: Defines how to handle accesses beyond the array boundaries. Available conditions are:
    - `Wrap()`: Wraps around to the other side of the array (periodic boundaries).
    - `Pad(value)`: Pads with a specified `value`.
    - `Reflect(symmetric)`: Reflects values back into the array at boundaries. The `symmetric` boolean controls whether the edge element is included in the reflection:
        - `Reflect(true)` (symmetric): Edge element IS repeated. For array `[a,b,c,d]`, extends as `[...,c,b,a,a,b,c,d,d,c,b,...]`.
        - `Reflect(false)` (mirror): Edge element NOT repeated. For array `[a,b,c,d]`, extends as `[...,d,c,b,a,b,c,d,c,b,a,...]`.
    - `Clamp()`: Clamps to the boundary value (repeats edge elements). For array `[a,b,c,d]`, extends as `[...,a,a,a,a,b,c,d,d,d,d,...]`.
    - `LinearExtrapolate()`: Linearly extrapolates using the slope at the boundary. Only works with `Real` element types. For array `[2,4,6,8]`, the slope at the low boundary is `4-2=2`, so index 0 would be `2-2=0`.
    - **Mixed BCs (Tuple)**: You can specify different boundary conditions per dimension using a tuple. For example, `(Wrap(), Pad(0))` uses `Wrap` for dimension 1 and `Pad(0)` for dimension 2.

### Example: Averaging Neighbors with `Wrap`

```julia
import Dagger: Wrap

# Initialize a DArray
A = ones(Blocks(1, 1), Int, 3, 3)
A[2,2] = 10 # Central element has a different value
B = zeros(Blocks(1, 1), Float64, 3, 3)

@stencil begin
    # Calculate the average of the 3x3 neighborhood (including the center)
    B[idx] = sum(@neighbors(A[idx], 1, Wrap())) / 9.0
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
import Pad

# Initialize a DArray
A = ones(Blocks(2, 2), Int, 4, 4)
B = zeros(Blocks(2, 2), Int, 4, 4)

@stencil begin
    B[idx] = sum(@neighbors(A[idx], 1, Pad(0))) # Pad with 0
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

### Example: Smoothing with `Reflect`

The `Reflect` boundary condition mirrors values at the edges, which is useful for operations like smoothing or image processing where you want to avoid artificial discontinuities at boundaries.

#### Symmetric Reflection (`Reflect(true)`)

With symmetric reflection, the edge element is repeated in the reflection:

```julia
import Dagger: Reflect

# Simple 1D example to illustrate symmetric reflection
# Array [1, 2, 3, 4] extends as [..., 3, 2, 1, 1, 2, 3, 4, 4, 3, 2, ...]
#                                         ^edge^         ^edge^
A = DArray([1, 2, 3, 4], Blocks(2))
B = zeros(Blocks(2), Int, 4)

@stencil begin
    B[idx] = sum(@neighbors(A[idx], 1, Reflect(true)))
end

# B[1]: indices 0,1,2 -> 0 reflects to 1, so [1,1,2] = 4
# B[2]: indices 1,2,3 -> all in bounds, [1,2,3] = 6
# B[3]: indices 2,3,4 -> all in bounds, [2,3,4] = 9
# B[4]: indices 3,4,5 -> 5 reflects to 4, so [3,4,4] = 11
@assert collect(B) == [4, 6, 9, 11]
```

#### Mirror Reflection (`Reflect(false)`)

With mirror reflection, the edge element is NOT repeated:

```julia
# Array [1, 2, 3, 4] extends as [..., 4, 3, 2, 1, 2, 3, 4, 3, 2, 1, ...]
#                                            ^edge^   ^edge^
A = DArray([1, 2, 3, 4], Blocks(2))
B = zeros(Blocks(2), Int, 4)

@stencil begin
    B[idx] = sum(@neighbors(A[idx], 1, Reflect(false)))
end

# B[1]: indices 0,1,2 -> 0 reflects to 2, so [2,1,2] = 5
# B[2]: indices 1,2,3 -> all in bounds, [1,2,3] = 6
# B[3]: indices 2,3,4 -> all in bounds, [2,3,4] = 9
# B[4]: indices 3,4,5 -> 5 reflects to 3, so [3,4,3] = 10
@assert collect(B) == [5, 6, 9, 10]
```

### Example: Edge Detection with `Clamp`

The `Clamp` boundary condition repeats edge values, which is useful when you want boundary elements to have a neutral effect:

```julia
import Dagger: Clamp

# Array [1, 2, 3, 4] extends as [..., 1, 1, 1, 1, 2, 3, 4, 4, 4, 4, ...]
A = DArray([1, 2, 3, 4], Blocks(2))
B = zeros(Blocks(2), Int, 4)

@stencil begin
    B[idx] = sum(@neighbors(A[idx], 1, Clamp()))
end

# B[1]: indices 0,1,2 -> 0 clamps to 1, so [1,1,2] = 4
# B[2]: indices 1,2,3 -> all in bounds, [1,2,3] = 6
# B[3]: indices 2,3,4 -> all in bounds, [2,3,4] = 9
# B[4]: indices 3,4,5 -> 5 clamps to 4, so [3,4,4] = 11
@assert collect(B) == [4, 6, 9, 11]
```

### Example: Smooth Extrapolation with `LinearExtrapolate`

The `LinearExtrapolate` boundary condition extrapolates linearly based on the slope at the boundary. This is useful for maintaining trends at edges:

```julia
import Dagger: LinearExtrapolate

# Array [2.0, 4.0, 6.0, 8.0] has slope 2.0 at both boundaries
# At low boundary: index 0 -> 2.0 + 2.0*(-1) = 0.0
# At high boundary: index 5 -> 8.0 + 2.0*(1) = 10.0
A = DArray([2.0, 4.0, 6.0, 8.0], Blocks(2))
B = zeros(Blocks(2), Float64, 4)

@stencil begin
    B[idx] = sum(@neighbors(A[idx], 1, LinearExtrapolate()))
end

# B[1]: indices 0,1,2 -> [0.0, 2.0, 4.0] = 6.0
# B[2]: indices 1,2,3 -> [2.0, 4.0, 6.0] = 12.0
# B[3]: indices 2,3,4 -> [4.0, 6.0, 8.0] = 18.0
# B[4]: indices 3,4,5 -> [6.0, 8.0, 10.0] = 24.0
@assert collect(B) ≈ [6.0, 12.0, 18.0, 24.0]
```

### Example: Mixed Boundary Conditions

You can specify different boundary conditions for each dimension using a tuple. This is useful when different boundaries have different physical meanings:

```julia
import Dagger: Wrap, Pad

# 2D array with Wrap in dimension 1 (rows) and Pad(0) in dimension 2 (columns)
A = DArray(reshape(1:16, 4, 4), Blocks(2, 2))
B = zeros(Blocks(2, 2), Int, 4, 4)

@stencil begin
    B[idx] = sum(@neighbors(A[idx], 1, (Wrap(), Pad(0))))
end

# For each element:
# - Row neighbors wrap around (periodic in rows)
# - Column neighbors are padded with 0 (zero-flux at column boundaries)
```

This is particularly useful for physical simulations where, for example, you might have periodic boundaries in one direction and fixed boundaries in another.

## Sequential Semantics

Expressions within a `@stencil` block are executed sequentially in terms of their effect on the data. This means that the result of one statement is visible to the subsequent statements, as if they were applied "all at once" across all indices before the next statement begins.

```julia
A = zeros(Blocks(2, 2), Int, 4, 4)
B = zeros(Blocks(2, 2), Int, 4, 4)

@stencil begin
    A[idx] = 1  # First, A is initialized
    B[idx] = A[idx] * 2       # Then, B is computed using the new values of A
end

expected_A = [1 for r in 1:4, c in 1:4]
expected_B_seq = expected_A .* 2

@assert collect(A) == expected_A
@assert collect(B) == expected_B_seq
```

## Operations on Multiple `DArray`s

You can read from and write to multiple `DArray`s within a single `@stencil` block, provided they have compatible chunk structures.

```julia
A = ones(Blocks(1, 1), Int, 2, 2)
B = DArray(fill(3, 2, 2), Blocks(1, 1))
C = zeros(Blocks(1, 1), Int, 2, 2)

@stencil begin
    C[idx] = A[idx] + B[idx]
end
@assert all(collect(C) .== 4)
```

## Update Operators

You can use update operators like `+=`, `-=`, `*=`, and `/=` within a `@stencil` block. These are automatically transformed into the corresponding assignment and operation.

```julia
A = ones(Blocks(2, 2), Int, 4, 4)
@stencil A[idx] += 1
@assert all(collect(A) .== 2)
```

## Example: Game of Life

The following demonstrates a more complex example: Conway's Game of Life.

```julia
# Ensure Plots and other necessary packages are available for the example
using Plots

N = 27 # Size of one dimension of a tile
nt = 3 # Number of tiles in each dimension (results in nt x nt grid of tiles)
niters = 10 # Number of iterations for the animation

tiles = zeros(Blocks(N, N), Bool, N*nt, N*nt)
outputs = zeros(Blocks(N, N), Bool, N*nt, N*nt)

# Create a fun initial state (e.g., a glider and some random noise)
tiles[13, 14] = true
tiles[14, 14] = true
tiles[15, 14] = true
tiles[15, 15] = true
tiles[14, 16] = true
# Add some random noise in one of the tiles
@view(tiles[(2N+1):3N, (2N+1):3N]) .= rand(Bool, N, N)



anim = @animate for _ in 1:niters
    @stencil begin
        outputs[idx] = begin
            nhood = @neighbors(tiles[idx], 1, Wrap())
            neighs = sum(nhood) - tiles[idx] # Sum neighborhood, but subtract own value
            if tiles[idx] && neighs < 2
                0 # Dies of underpopulation
            elseif tiles[idx] && neighs > 3
                0 # Dies of overpopulation
            elseif !tiles[idx] && neighs == 3
                1 # Becomes alive by reproduction
            else
                tiles[idx] # Keeps its prior value
            end
        end
        tiles[idx] = outputs[idx] # Update tiles for the next iteration
    end
    heatmap(Int.(collect(outputs))) # Generate a heatmap visualization
end
path = mp4(anim; fps=5, show_msg=true).filename # Create an animation of the heatmaps over time
```
