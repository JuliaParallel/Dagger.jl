# Dagger: A framework for out-of-core and parallel execution

Dagger.jl is a framework for parallel computing across all kinds of resources,
like CPUs and GPUs, and across multiple threads and multiple servers.


Note: when using Dagger with multiple workers, make sure that the workers are
initialized *before* importing Dagger and doing any distributed operations. This
is good:
```julia-repl
julia> using Distributed

julia> addprocs(2)

julia> using Dagger
```

But this will cause errors when using Dagger:
```julia-repl
julia> using Distributed, Dagger

julia> addprocs(2)
```

The reason is because Distributed.jl requires packages to be loaded across all
workers in order for the workers to use objects from the needed packages, and
`using Dagger` will load Dagger on all existing workers. If you're executing
custom functions then you will also need to define those on all workers with
`@everywhere`, [see the Distributed.jl
documentation](https://docs.julialang.org/en/v1/manual/distributed-computing/#code-availability)
for more information.

-----

## Quickstart: Task Spawning

For more details: [Task Spawning](@ref)

### Launch a task

If you want to call a function `myfunc` with arguments `arg1`, `arg2`, `arg3`,
and keyword argument `color=:red`:

```julia
function myfunc(arg1, arg2, arg3; color=:blue)
    arg_total = arg1 + arg2 * arg3
    printstyled(arg_total; color)
    return arg_total
end
t = Dagger.@spawn myfunc(arg1, arg2, arg3; color=:red)
```

This will run the function asynchronously; you can fetch its result with
`fetch(t)`, or just wait on it to complete with `wait(t)`. If the call to
`myfunc` throws an error, `fetch(t)` will rethrow it.

If running Dagger with multiple workers, make sure to define `myfunc` with
`@everywhere` from the `Distributed` stdlib.

### Launch a task with an anonymous function

It's more convenient to use `Dagger.spawn` for anonymous functions. Taking the
previous example, but using an anonymous function instead of `myfunc`:

```julia
Dagger.spawn((arg1, arg2, arg3; color=:blue) -> begin
    arg_total = arg1 + arg2 * arg3
    printstyled(arg_total; color)
    return arg_total
end, arg1, arg2, arg3; color=:red)
```

`spawn` is functionally identical to `@spawn`, but can be more or less
convenient to use, depending on what you're trying to do.

### Launch many tasks and wait on them all to complete

`@spawn` participates in `@sync` blocks, just like `@async` and
`Threads.@spawn`, and will cause `@sync` to wait until all the tasks have
completed:

```julia
@sync for result in simulation_results
    Dagger.@spawn send_result_to_database(result)
end
nresults = length(simulation_results)
wait(Dagger.@spawn update_database_result_count(nresults))
```

Above, `update_database_result_count` will only run once all
`send_result_to_database` calls have completed.

Note that other APIs (including `spawn`) do not participate in `@sync` blocks.

### Run a task on a specific Distributed worker

Dagger uses [Scopes](@ref) to control where tasks can execute. There's a handy
constructor, `Dagger.scope`, that makes defining scopes easy:

```julia
w2_only = Dagger.scope(worker=2)
Dagger.@spawn scope=w2_only myfunc(arg1, arg2, arg3; color=:red)
```

Now the launched task will *definitely* execute on worker 2 (or if it's not
possible to run on worker 2, Dagger will throw an error when you try to `fetch`
the result).

### Parallelize nested loops

Nested loops are a very common pattern in Julia, yet it's often difficult to
parallelize them efficiently with `@threads` or `@distributed`/`pmap`.
Thankfully, this kind of problem is quite easy for Dagger to handle; here is an
example of parallelizing a two-level nested loop, where the inner loop
computations (`g`) depend on an outer loop computation (`f`):

```julia
@everywhere begin
    using Random
    Random.seed!(0)

    # Some "expensive" functions that complete at different speeds
    const crn = abs.(randn(20, 7))
    f(i) = sleep(crn[i, 7])
    g(i, j, y) = sleep(crn[i, j])
end
function nested_dagger()
    @sync for i in 1:20
        y = Dagger.@spawn f(i)
        for j in 1:6
            z = Dagger.@spawn g(i, j, y)
        end
    end
end
```

And the equivalent (and less performant) example with `Threads.@threads`,
either parallelizing the inner or outer loop:

```julia
function nested_threads_outer()
    Threads.@threads for i in 1:20
        y = f(i)
        for j in 1:6
            z = g(i, j, y)
        end
    end
end
function nested_threads_inner()
    for i in 1:20
        y = f(i)
        Threads.@threads for j in 1:6
            z = g(i, j, y)
        end
    end
end
```

Unlike `Threads.@threads` (which is really only intended to be used for a
single loop, unnested), `Dagger.@spawn` is capable of parallelizing across both
loop levels seamlessly, using the dependencies between `f` and `g` to determine
the correct order to execute tasks.

-----

## Quickstart: Data Management

For more details: [Data Management](@ref)

### Operate on mutable data in-place

Dagger usually assumes that you won't be modifying the arguments passed to your
functions, but you can tell Dagger you plan to mutate them with `@mutable`:

```julia
A = Dagger.@mutable rand(1000, 1000)
Dagger.@spawn accumulate!(+, A, A)
```

This will lock `A` (and any tasks that use it) to the current worker. You can
also lock it to a different worker by creating the data within a task:

```julia
A = Dagger.spawn() do
    Dagger.@mutable rand(1000, 1000)
end
```

or by specifying the `worker` argument to `@mutable`:

```julia
A = Dagger.@mutable worker=2 rand(1000, 1000)
```

### Operate on distributed data

Often we want to work with more than one piece of data; the common case of
wanting one piece of data per worker is easy to do by using `@shard`:

```julia
X = Dagger.@shard myid()
```

This will execute `myid()` independently on every worker in your Julia
cluster, and place references to each within a `Shard` object called `X`. We
can then use `X` in task spawning, but we'll only get the result of
`myid()` that corresponds to the worker that the task is running on:

```julia
for w in workers()
    @show fetch(Dagger.@spawn scope=Dagger.scope(worker=w) identity(X))
end
```

The above should print the result of `myid()` for each worker in `worker()`, as
`identity(X)` receives only the value of `X` specific to that worker.

### Reducing over distributed data

Reductions are often parallelized by reducing a set of partitions on each
worker, and then reducing those intermediate reductions on a single worker.
Dagger supports this easily with `@shard`:

```julia
A = Dagger.@shard rand(1:20, 10000)
temp_bins = Dagger.@shard zeros(20)
hist! = (bins, arr) -> for elem in arr
    bins[elem] += 1
end
wait.([Dagger.@spawn scope=Dagger.scope(;worker) hist!(temp_bins, A) for worker in procs()])
final_bins = sum(map(b->fetch(Dagger.@spawn copy(b)), temp_bins); dims=1)[1]
```

Here, `A` points to unique random arrays, one on each worker, and `temp_bins`
points to a set of histogram bins on each worker. When we `@spawn hist!`,
Dagger passes in the random array and bins for only the specific worker that
the task is run on; i.e. a call to `hist!` that runs on worker 2 will get a
different `A` and `temp_bins` from a call to `hist!` on worker 3. All of the
calls to `hist!` may run in parallel.

By using `map` on `temp_bins`, we then make a copy of each worker's bins that
we can safely return back to our current worker, and sum them together to get
our total histogram.

-----

## Quickstart: File IO

Dagger has support for loading and saving files that integrates seamlessly with
its task system, in the form of `Dagger.File` and `Dagger.tofile`.

!!! warn
    These functions are not yet fully tested, so please make sure to take backups of any files that you load with them.

### Loading files from disk

In order to load one or more files from disk, Dagger provides the `File`
function, which creates a lazy reference to a file:

```julia
f = Dagger.File("myfile.jls")
```

`f` is now a lazy reference to `"myfile.jls"`, and its contents can be loaded
automatically by just passing the object to a task:

```julia
wait(Dagger.@spawn println(f))
# Prints the loaded contents of the file
```

By default, `File` assumes that the file uses Julia's Serialization format;
this can be easily changed to assume Arrow format, for example:

```julia
using Arrow
f = Dagger.File("myfile.arrow"; serialize=Arrow.write, deserialize=Arrow.Table)
```

### Writing data to disk

Saving data to disk is as easy as loading it; `tofile` provides this capability
in a similar manner to `File`:

```julia
A = rand(1000)
f = Dagger.tofile(A, "mydata.jls")
```

Like `File`, `f` can still be used to reference the file's data in tasks. It is
likely most useful to use `tofile` at the end of a task to save results:

```julia
function make_data()
    A = rand(1000)
    return Dagger.tofile(A, "mydata.jls")
end
fetch(Dagger.@spawn make_data())
# Data was also written to "mydata.jls"
```

`tofile` takes the same keyword arguments as `File`, allowing the format of
data on disk to be specified as desired.

-----

## Quickstart: Distributed Arrays

Dagger's `DArray` type represents a distributed array, where a single large
array is implemented as a set of smaller array partitions, which may be
distributed across a Julia cluster.

For more details: [Distributed Arrays](@ref)

### Distribute an existing array

Distributing any kind of array into a `DArray` is easy, just use `distribute`,
and specify the partitioning you desire with `Blocks`. For example, to
distribute a 16 x 16 matrix in 4 x 4 partitions:

```julia
A = rand(16, 16)
DA = distribute(A, Blocks(4, 4))
```

### Allocate a distributed array directly

To allocate a `DArray`, just pass your `Blocks` partitioning object into the
appropriate allocation function, such as `rand`, `ones`, or `zeros`:

```julia
rand(Blocks(20, 20), 100, 100)
ones(Blocks(20, 100), 100, 2000)
zeros(Blocks(50, 20), 300, 200)
```

### Convert a `DArray` back into an `Array`

To get back an `Array` from a `DArray`, just call `collect`:

```julia
DA = rand(Blocks(32, 32), 256, 128)
collect(DA) # returns a `Matrix{Float64}`
```

## Quickstart: Datadeps

Datadeps is a feature in Dagger.jl that facilitates parallelism control within designated regions, allowing tasks to write to their arguments while ensuring dependencies are respected.
For more details: [Datadeps (Data Dependencies)](@ref)

### Syntax

The Dagger.spawn_datadeps() function is used to create a "datadeps region" where tasks are executed with parallelism controlled by specified dependencies:

```julia
Dagger.spawn_datadeps() do
    Dagger.@spawn func!(Out(var_name_x), In(var_name_y))
end
```

### Argument Annotation

- `In(X)`: Indicates that the variable `X` is only read by the task (an "input").
- `Out(X)`: Indicates that the variable `X` is only written to by the task (an "output").
- `InOut(X)`: Indicates that the variable `X` is both read from and written to by the task (an "input" and "output" simultaneously).

### Example with Datadeps

```julia
X = [4,5,6,7,1,2,3,9,8]
C = zeros(10)

Dagger.spawn_datadeps() do
    Dagger.@spawn sort!(InOut(X))
    Dagger.@spawn copyto!(Out(C), In(X))
end

# C = [1,2,3,4,5,6,7,8,9]
```

In this example, the `sort!` function operates on array `X`, while the `copyto!` task reads from array `X` and reads from array `C`. By specifying dependencies using argument annotations, the tasks are executed in a controlled parallel manner, resulting in a sorted `C` array.

### Example without Datadeps

```julia
X = [4,5,6,7,1,2,3,9,8]
C = zeros(10)
Dagger.@spawn sort!(X)
Dagger.@spawn copyto!(C, X)


# C = [4,5,6,7,1,2,3,9,8]
```

In contrast to the previous example, here, the tasks are executed without argument annotations. As a result, there is a possibility of the `copyto!` task being executed before the `sort!` task, leading to unexpected results in the output array `C`.

## Quickstart: Streaming

Dagger.jl provides a streaming API that allows you to process data in a streaming fashion, where data is processed as it becomes available, rather than waiting for the entire dataset to be loaded into memory.

For more details: [Streaming](@ref)

### Syntax

The `Dagger.spawn_streaming()` function is used to create a streaming region,
where tasks are executed continuously, processing data as it becomes available:

```julia
# Open a file to write to on this worker
f = Dagger.@mutable open("output.txt", "w")
t = Dagger.spawn_streaming() do
    # Generate random numbers continuously
    val = Dagger.@spawn rand()
    # Write each random number to a file
    Dagger.@spawn (f, val) -> begin
        if val < 0.01
            # Finish streaming when the random number is less than 0.01
            Dagger.finish_stream()
        end
        println(f, val)
    end
end
# Wait for all values to be generated and written
wait(t)
```

The above example demonstrates a streaming region that generates random numbers
continuously and writes each random number to a file. The streaming region is
terminated when a random number less than 0.01 is generated, which is done by
calling `Dagger.finish_stream()` (this terminates the current task, and will
also terminate all streaming tasks launched by `spawn_streaming`).
