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

Datadeps also runs across distributed memory on top of MPI, using the same API
shown here; see the [MPI](mpi.md) page for how to launch and scope datadeps
regions across cluster ranks.

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

## Asynchronous Regions

By default, `spawn_datadeps` is *synchronous*: it does not return until every
task it launched has finished, every argument has been written back to where it
came from, and every temporary copy has been freed. That makes each region a
full barrier. Consecutive regions therefore cannot overlap, even when the second
region's first tasks depend on only a small part of the first region's output.

Passing `sync=false` makes the region *asynchronous*. `spawn_datadeps` returns as
soon as it has finished planning; the tasks keep running, and the write-back and
free steps are deferred. You then drain the pipeline explicitly with
`Dagger.synchronize()`:

```julia
Dagger.spawn_datadeps(; sync=false) do
    Dagger.@spawn stage1!(InOut(A))
end

Dagger.spawn_datadeps(; sync=false) do
    Dagger.@spawn stage2!(InOut(A))
end

# Nothing above is guaranteed to have finished yet. This is the barrier:
Dagger.synchronize()

# `A` is now safe to read from plain Julia code.
@show sum(A)
```

Dependencies between regions are still tracked and still enforced — `stage2!`
will not start before `stage1!` finishes, exactly as with `sync=true`. What
changes is that the *planning* of the second region overlaps with the
*execution* of the first, and that data which stays inside Dagger's tracking is
no longer copied back and forth at every region boundary.

`Dagger.synchronize()` always operates on the calling task's own region history,
so it can never accidentally wait on unrelated work elsewhere in the program. To
reach beyond the calling task, use the explicitly-named
[`Dagger.synchronize_task!`](@ref) or [`Dagger.synchronize_all!`](@ref).

If a task launched by an asynchronous region fails, the failure is not raised at
`spawn_datadeps` time (it had not happened yet) but at the next
`Dagger.synchronize()` call, wrapped in a `DataDepsRegionError` naming the region
that queued the failing task. Planning a further region on top of an unobserved
failure raises `DataDepsPoisonedError` rather than silently building on a broken
pipeline.

!!! warning "`sync=false` requires everything downstream to stay inside Dagger"
    An asynchronous region is only safe if every consumer of its data is either
    another Datadeps region or comes after an explicit `Dagger.synchronize()`.
    Plain, non-Dagger code that reads or writes the same arrays relies on the
    region boundary being a barrier, and with `sync=false` it no longer is.

    This is not a theoretical hazard. Enabling `sync=false` ambiently over
    Dagger's own FFT test suite produced **silently wrong numerical results** in
    4 of 48 cases: the 1-D `fft!` path does `copyto!(A, DA); fft!(A); copyto!(DA, A)`
    with a plain, untracked `Array` in the middle, and the asynchronous region
    raced that plain code against its own input. Wrong numbers, no error.

    So: request `sync=false` per call, for regions whose consumers you control.
    Never bind it as an ambient default (via the `DATADEPS_SYNC` scoped value)
    over code you do not own.

### Synchronizing only what you need

`Dagger.synchronize(A, B)` narrows the drain to the named values: it writes back
only `A` and `B`, waits only on the tasks that have read or written them, and
leaves everything else in the pipeline running. This is what lets you consume
one result without collapsing the pipeline behind it:

```julia
Dagger.spawn_datadeps(; sync=false) do
    Dagger.@spawn compute_fast!(InOut(A))
end
Dagger.spawn_datadeps(; sync=false) do
    Dagger.@spawn compute_slow!(InOut(B))
end

Dagger.synchronize(A)   # returns as soon as A is ready; B keeps running
report(A)

Dagger.synchronize()    # now wait for B as well
```

A `DArray` resolves to its individual chunks, so `Dagger.synchronize(DA)` works
as you would expect.

If an argument names something Dagger isn't tracking, the call falls back to a
full drain rather than guessing — narrowing can fail to be lazy, but it cannot
fail to synchronize. Two further properties worth knowing: a targeted call never
frees buffers (whether a buffer is dead depends on regions you didn't name), and
it does not clear a poisoned context, so a failure still requires a full
`Dagger.synchronize()` before you can plan again.

`Dagger.synchronize_all!` accepts arguments for signature symmetry but ignores
them; draining every context completely is its whole purpose.

!!! warning "MPI/SPMD uniformity"
    Under MPI, every rank must call `synchronize` naming the same data: which
    write-back copies get emitted is a collective decision, so a rank that
    narrows differently desynchronizes rather than merely doing less work.

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

## Custom Schedulers

The `spawn_datadeps` function accepts an optional `scheduler` keyword argument that controls how tasks are assigned to processors. By default, `spawn_datadeps` uses `RoundRobinScheduler()`, which cycles through available processors in a round-robin fashion.

### Built-in Schedulers

- **`RoundRobinScheduler()`** (default): Assigns tasks to processors in round-robin order, optionally biased toward processors that already hold the task's data (see `DATADEPS_LOCALITY_BIAS`). Simple, cheap, and rank-uniform under MPI.
- **`NaiveScheduler()`**: Costs each task with the main Dagger scheduler's `estimate_task_costs` and takes the cheapest processor. Each task is costed in isolation against the *live* scheduler's pressure, which planning does not move — so a region's own decisions are invisible to it, and it tends to place a whole region in the same spot. Not usable under MPI/SPMD (raises rather than deadlocking).
- **`UltraScheduler()`**: Places each task where it is predicted to *finish* earliest, simulating the region as it plans it: earlier decisions push a processor's predicted idle time forward and so steer later ones. Understands data movement (via `DATADEPS_LOCALITY_BIAS`) and per-task scopes, and is rank-uniform under MPI (at the cost of ignoring measured per-rank runtimes there).

!!! note "The hierarchical partitioner usually decides first"
    With `hierarchical=true` (the default), `partition_dag` assigns each task
    to the owner holding the most of its argument data *before* any
    `DataDepsScheduler` runs, and then hands the scheduler only that owner's
    processors. On a single-node, single-worker run that leaves the scheduler
    choosing among threads of one memory space, where all three schedulers
    behave near-identically. The choice of scheduler matters most on the flat
    path (`hierarchical=false`) or across multiple owners.

### Using a Different Scheduler

You can pass a scheduler to `spawn_datadeps` like so:

```julia
Dagger.spawn_datadeps(; scheduler=Dagger.RoundRobinScheduler()) do
    Dagger.@spawn my_task!(InOut(A))
    Dagger.@spawn another_task!(In(B))
end
```

### Writing Your Own Scheduler

You can implement a custom scheduler by:
1. Defining a struct that subtypes `Dagger.DataDepsScheduler`
2. Implementing the `Dagger.datadeps_schedule_task` method for your scheduler

The scheduler's job is to select which processor should execute a given task. Here's a simple example that randomly selects a processor:

```julia
# Define the scheduler type
struct RandomScheduler <: Dagger.DataDepsScheduler end

# Implement the scheduling function
function Dagger.datadeps_schedule_task(::RandomScheduler, state, all_procs, all_scope, task_scope, spec, task)
    # Reduce the available processors to the ones that are compatible with the task scope
    compatible_procs = filter(proc->proc_in_scope(proc, task_scope), all_procs)
    if isempty(compatible_procs)
        throw(SchedulingException("No processors available for task $(task.uid) with scope $(task_scope)"))
    end
    # Simply pick a random processor from the compatible ones
    return rand(compatible_procs)
end

# Use it
Dagger.spawn_datadeps(; scheduler=RandomScheduler()) do
    Dagger.@spawn my_task!(InOut(A))
end
```

The `datadeps_schedule_task` function receives:
- `state`: Internal datadeps state (typically not needed for simple schedulers)
- `all_procs`: Vector of all available processors
- `all_scope`: The combined scope of all processors
- `task_scope`: The scope constraint for this specific task
- `spec`: The task specification
- `task`: The DTask being scheduled

The function must return a processor from `all_procs` that is compatible with `task_scope`.

If your scheduler carries mutable state, also implement `Base.similar` for it. Hierarchical scheduling (below) clones the scheduler once per partition so that partitions do not share mutable state; the default `similar` calls your scheduler's zero-argument constructor.

## Hierarchical Scheduling

Planning a datadeps region is itself work: every argument's aliasing must be computed, a dependency DAG built, and each task's copies planned. For large regions this planning can dominate. Hierarchical scheduling spreads that work out, and is **enabled by default**.

It runs as a four-phase pipeline:

1. Collect per-task argument metadata and compute aliasing information.
2. Build the dependency DAG from the aliasing overlaps.
3. Partition the DAG by data affinity — by Distributed worker or MPI rank, or across local processors when there is only one owner.
4. Plan each partition's tasks, with each partition assigning processors via its own scheduler shard restricted to its own processors.

You can disable it per-region, which falls back to the flat single-threaded planner:

```julia
Dagger.spawn_datadeps(; hierarchical=false) do
    Dagger.@spawn my_task!(InOut(A))
end
```

or for a dynamic extent via the `Dagger.DATADEPS_HIERARCHICAL` scoped value.

Both paths produce the same results; they differ only in how planning is distributed. Small regions short-circuit to the flat path automatically, since partitioning does not pay for itself there.

### Current Limitations

Hierarchical scheduling does not yet parallelize everything it could. These are performance limitations only — correctness and results are unaffected.

- **Parallel planning currently applies only to single-owner (multi-threaded) regions.** When work spans multiple Distributed workers or MPI ranks, Phase 4 plans partitions sequentially, in global topological order, over one shared state. Partitions still carry worker/rank affinity — tasks are placed where their data lives — but the planning itself does not run concurrently. The per-partition parallel path is unsafe across owners because the datadeps bookkeeping is keyed by memory *space* and cannot represent two partitions holding distinct slots for the same chunk; tracking slot identity instead is the follow-up that would re-enable it.

- **Under MPI this is additionally required.** Uniform (SPMD) execution needs every rank to allocate tags and `MPIRefID`s in the same order, and aliasing may perform collectives that must be ordered identically across ranks, so Phase 1 is serial there too.

- **Planning is centralized on the calling process.** Aliasing computation (phase 1) is genuinely distributed via `remotecall`, but per-task copy planning (phase 4) runs on the process that entered the region, using threads only. Workers do not plan their own partitions.

- **Phases 2 and 3 are single-threaded.** Building the DAG and computing aliasing overlaps are incremental, order-dependent algorithms. They are cheap relative to phases 1 and 4 today, but will become the bottleneck as those scale.

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
