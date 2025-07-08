# Datadeps (Data Dependencies)

For many programs, the restriction that tasks cannot write to their arguments
feels overly restrictive and makes certain kinds of programs (such as in-place
linear algebra) hard to express efficiently in Dagger. Thankfully, there is a
solution called "Datadeps" (short for "data dependencies"), accessible through
the `spawn_datadeps` function. This function constructs a "datadeps region",
within which tasks are allowed to write to their arguments, with parallelism
controlled via dependencies specified via argument annotations. At the end of
the "datadeps region" the `spawn_datadeps` will wait for the completion of all
the tasks launched within it. Let's look at a simple example to make things
concrete:

```julia
A = rand(1000)
B = rand(1000)
C = zeros(1000)
@everywhere add!(X, Y) = X .+= Y
Dagger.spawn_datadeps() do
    Dagger.@spawn add!(InOut(B), In(A))
    Dagger.@spawn copyto!(Out(C), In(B))
end
```

In this example, we have two Dagger tasks being launched, one adding `A` into
`B`, and the other copying `B` into `C`. The `add!` task is specifying that
`A` is being only read from (`In` for "input"), and that `B` is being read
from and written to (`Out` for "output", `InOut` for "input and output"). The
`copyto` task, similarly, is specifying that `B` is being read from, and `C`
is only being written to.

Without `spawn_datadeps` and `In`, `Out`, and `InOut`, the result of these
tasks would be undefined; the two tasks could execute in parallel, or the
`copyto!` could occur before the `add!`, resulting in all kinds of mayhem.
However, `spawn_datadeps` changes things: because we have told Dagger how our
tasks access their arguments, Dagger knows to control the parallelism and
ordering, and ensure that `add!` executes and finishes before `copyto!`
begins, ensuring that `copyto!` "sees" the changes to `B` before executing.

There is another important aspect of `spawn_datadeps` that makes the above
code work: if all of the `Dagger.@spawn` macros are removed, along with the
dependency specifiers, the program would still produce the same results,
without using Dagger. In other words, the parallel (Dagger) version of the
program produces identical results to the serial (non-Dagger) version of the
program. This is similar to using Dagger with purely functional tasks and
without `spawn_datadeps` - removing `Dagger.@spawn` will still result in a
correct (sequential and possibly slower) version of the program. Basically,
`spawn_datadeps` will ensure that Dagger respects the ordering and
dependencies of a program, while still providing parallelism, where possible.

But where is the parallelism? The above example doesn't actually have any
parallelism to exploit! Let's take a look at another example to see the
datadeps model truly shine:

```julia
# Tree reduction of multiple arrays into the first array
function tree_reduce!(op::Base.Callable, As::Vector{<:Array})
    Dagger.spawn_datadeps() do
        to_reduce = Vector[]
        push!(to_reduce, As)
        while !isempty(to_reduce)
            As = pop!(to_reduce)
            n = length(As)
            if n == 2
                Dagger.@spawn Base.mapreducedim!(identity, op, InOut(As[1]), In(As[2]))
            elseif n > 2
                push!(to_reduce, [As[1], As[div(n,2)+1]])
                push!(to_reduce, As[1:div(n,2)])
                push!(to_reduce, As[div(n,2)+1:end])
            end
        end
    end
    return As[1]
end

As = [rand(1000) for _ in 1:1000]
Bs = copy.(As)
tree_reduce!(+, As)
@assert isapprox(As[1], reduce((x,y)->x .+ y, Bs))
```

In the above implementation of `tree_reduce!` (which is designed to perform an
elementwise reduction across a vector of arrays), we have a tree reduction
operation where pairs of arrays are reduced, starting with neighboring pairs,
and then reducing pairs of reduction results, etc. until the final result is in
`As[1]`. We can see that the application of Dagger to this algorithm is simple -
only the single `Base.mapreducedim!` call is passed to Dagger - yet due to the
data dependencies and the algorithm's structure, there should be plenty of
parallelism to be exploited across each of the parallel reductions at each
"level" of the reduction tree. Specifically, any two `Dagger.@spawn` calls
which access completely different pairs of arrays can execute in parallel,
while any call which has an `In` on an array will wait for any previous call
which has an `InOut` on that same array.

Additionally, we can notice a powerful feature of this model - if the
`Dagger.@spawn` macro is removed, the code still remains correct, but simply
runs sequentially. This means that the structure of the program doesn't have to
change in order to use Dagger for parallelization, which can make applying
Dagger to existing algorithms quite effortless.

## Limitations

It's important to be aware of a key limitation when working with `Dagger.spawn_datadeps`. Operations that involve explicit synchronization or fetching results of other Dagger tasks, such as `fetch`, `wait`, or `@sync`, cannot be used directly inside a `spawn_datadeps` block.

The `spawn_datadeps` region is designed to manage data dependencies automatically based on the `In`, `Out`, and `InOut` annotations. Introducing explicit synchronization primitives can interfere with this mechanism and lead to unexpected behavior or errors.

**Example of what NOT to do:**

```julia
Dagger.spawn_datadeps() do
    # Incorrect: Using fetch inside spawn_datadeps
    task1 = Dagger.@spawn my_func1!(InOut(A))
    result1 = fetch(task1) # This will not work as expected

    # Incorrect: Using wait inside spawn_datadeps
    task2 = Dagger.@spawn my_func2!(InOut(B))
    wait(task2) # This will also lead to issues

    # Incorrect: Using @sync inside spawn_datadeps
    @sync begin
        Dagger.@spawn my_func3!(InOut(C))
        Dagger.@spawn my_func4!(InOut(D))
    end
end
```

If you need to synchronize or fetch results, these operations should be performed outside the `spawn_datadeps` block. The primary purpose of `spawn_datadeps` is to define a region where data dependencies for mutable operations are automatically managed.

## Aliasing Support

Datadeps is smart enough to detect when two arguments from different tasks
actually access the same memory (we say that these arguments "alias"). There's
the obvious case where the two arguments are exactly the same object, but
Datadeps is also aware of more subtle cases, such as when two arguments are
different views into the same array, or where two arrays point to the same
underlying memory. In these cases, Datadeps will ensure that the tasks are
executed in the correct order - if one task writes to an argument which aliases
with an argument read by another task, those two tasks will be executed in
sequence, rather than in parallel.

There are two ways to specify aliasing to Datadeps. The simplest way is the most straightforward: if the argument passed to a task is a view or another supported object (such as an `UpperTriangular`-wrapped array), Datadeps will compare it with all other task's arguments to determine if they alias. This works great when you want to pass that view or `UpperTriangular` object directly to the called function. For example:

```julia
A = rand(1000)
A_l = view(A, 1:500)
A_r = view(A, 501:1000)

# inc! supports views, so we can pass A_l and A_r directly
@everywhere inc!(X) = X .+= 1

Dagger.spawn_datadeps() do
    # These two tasks don't alias, so they can run in parallel
    Dagger.@spawn inc!(InOut(A_l))
    Dagger.@spawn inc!(InOut(A_r))

    # This task aliases with the previous two, so it will run after them
    Dagger.@spawn inc!(InOut(A))
end
```

The other way allows you to separate what argument is passed to the function,
from how that argument is accessed within the function. This is done with the
`Deps` wrapper, which is used like so:

```julia
A = rand(1000, 1000)

@everywhere inc_upper!(X) = UpperTriangular(X) .+= 1
@everywhere inc_ulower!(X) = UnitLowerTriangular(X) .+= 1
@everywhere inc_diag!(X) = X[diagind(X)] .+= 1

Dagger.spawn_datadeps() do
    # These two tasks don't alias, so they can run in parallel
    Dagger.@spawn inc_upper!(Deps(A, InOut(UpperTriangular)))
    Dagger.@spawn inc_ulower!(Deps(A, InOut(UnitLowerTriangular)))

    # This task aliases with the `inc_upper!` task (`UpperTriangular` accesses the diagonal of the array)
    Dagger.@spawn inc_diag!(Deps(A, InOut(Diagonal)))
end
```

We call `InOut(Diagonal)` an "aliasing modifier". The purpose of `Deps` is to
pass an argument (here, `A`) as-is, while specifying to Datadeps what portions
of the argument will be accessed (in this case, the diagonal elements) and how
(read/write/both). You can pass any number of aliasing modifiers to `Deps`.

`Deps` is particularly useful for declaring aliasing with `Diagonal`,
`Bidiagonal`, `Tridiagonal`, and `SymTridiagonal` access, as these "wrappers"
make a copy of their parent array and thus can't be used to "mask" access to the
parent like `UpperTriangular` and `UnitLowerTriangular` can (which is valuable
for writing memory-efficient, generic algorithms in Julia).

### Supported Aliasing Modifiers

- Any function that returns the original object or a view of the original object
- `UpperTriangular`/`LowerTriangular`/`UnitUpperTriangular`/`UnitLowerTriangular`
- `Diagonal`/`Bidiagonal`/`Tridiagonal`/`SymTridiagonal` (via `Deps`, e.g. to read from the diagonal of `X`: `Dagger.@spawn sum(Deps(X, In(Diagonal)))`)
- `Symbol` for field access (via `Deps`, e.g. to write to `X.value`: `Dagger.@spawn setindex!(Deps(X, InOut(:value)), :value, 42)`

## In-place data movement rules

Datadeps uses a specialized 5-argument function, `Dagger.move!(dep_mod, from_space::Dagger.MemorySpace, to_space::Dagger.MemorySpace, from, to)`, for managing in-place data movement. This function is an in-place variant of the more general `move` function (see [Data movement rules](@ref)) and is exclusively used within the Datadeps system. The `dep_mod` argument is usually just `identity`, but it can also be an access modifier function like `UpperTriangular`, which limits what portion of the data should be read from and written to.

The core responsibility of `move!` is to read data from the `from` argument and write it directly into the `to` argument. This is crucial for operations that modify data in place, as often encountered in numerical computing and linear algebra.

The default implementation of `move!` handles `Chunk` objects by unwrapping them and then recursively calling `move!` on the underlying values. This ensures that the in-place operation is performed on the actual data.

Users have the option to define their own `move!` implementations for custom data types. However, this is typically not necessary for types that are subtypes of `AbstractArray`, provided that these types support the standard `Base.copyto!(to, from)` function. The default `move!` will leverage `copyto!` for such array types, enabling efficient in-place updates.

Here's an example of a custom `move!` implementation:

```julia
struct MyCustomArrayWrapper{T,N}
    data::Array{T,N}
end

# Custom move! function for MyCustomArrayWrapper
function Dagger.move!(dep_mod::Any, from_space::Dagger.MemorySpace, to_space::Dagger.MemorySpace, from::MyCustomArrayWrapper, to::MyCustomArrayWrapper)
    copyto!(dep_mod(to.data), dep_mod(from.data))
    return
end
```

## Chunk and DTask slicing with `view`

The `view` function allows you to efficiently create a "view" of a `Chunk` or `DTask` that contains an array. This enables operations on specific parts of your distributed data using standard Julia array slicing, without needing to materialize the entire array.

```julia
    view(c::Chunk, slices...) -> ChunkView
    view(c::DTask, slices...) -> ChunkView
```

These methods create a `ChunkView` of a `Chunk` or `DTask`, which may be used as an argument to a `Dagger.@spawn` call in a Datadeps region. You specify the desired view using standard Julia array slicing syntax, identical to how you would slice a regular array.

#### Examples

```julia
julia> A = rand(64, 64)
64×64 Matrix{Float64}:
[...]

julia> DA = DArray(A, Blocks(8,8)) 
64x64 DMatrix{Float64} with 8x8 partitions of size 8x8:
[...]

julia> chunk = DA.chunks[1,1] 
DTask (finished)

julia> view(chunk, :, :) # View the entire 8x8 chunk
ChunkSlice{2}(Dagger.Chunk(...), (Colon(), Colon()))

julia> view(chunk, 1:4, 1:4) # View the top-left 4x4 sub-region of the chunk
ChunkSlice{2}(Dagger.Chunk(...), (1:4, 1:4))

julia> view(chunk, 1, :) # View the first row of the chunk
ChunkSlice{2}(Dagger.Chunk(...), (1, Colon()))

julia> view(chunk, :, 5) # View the fifth column of the chunk
ChunkSlice{2}(Dagger.Chunk(...), (Colon(), 5))

julia> view(chunk, 1:2:7, 2:2:8) # View with stepped ranges
ChunkSlice{2}(Dagger.Chunk(...), (1:2:7, 2:2:8))
```

#### Example Usage: Parallel Row Summation of a DArray using `view`

This example demonstrates how to sum multiple rows of a `DArray` by using `view` to process individual rows within chunks to get a vector of row sums.

```julia
julia> A = DArray(rand(10, 1000), Blocks(2, 1000))
10x1000 DMatrix{Float64} with 5x1 partitions of size 2x1000: 
[...]

# Helper function to sum a single row and store it in a provided array view
julia> @everywhere function sum_array_row!(row_sum::AbstractArray{Float64}, x::AbstractArray{Float64})
    row_sum[1] = sum(x)
end

# Number of rows
julia> nrows = size(A,1)

# Initialize a zero array in the final row sums
julia> row_sums = zeros(nrows)

# Spawn tasks to sum each row in parallel using views
julia> Dagger.spawn_datadeps() do
           sz = size(A.chunks,1) 
           nrows_per_chunk = nrows ÷ sz
           for i in 1:sz
               for j in 1:nrows_per_chunk
                   Dagger.@spawn sum_array_row!(Out(view(row_sums, (nrows_per_chunk*(i-1)+j):(nrows_per_chunk*(i-1)+j))),
                                                In(Dagger.view(A.chunks[i,1], j:j, :)))
               end
           end
       end

# Print the result
julia> println("Row sums: ", row_sums)
Row sums: [499.8765, 500.1234, ..., 499.9876]
```
