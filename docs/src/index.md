# Dagger: A framework for out-of-core and parallel execution

Dagger.jl is a framework for parallel computing across all kinds of resources,
like CPUs and GPUs, and across multiple threads and multiple servers.

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

### Parallel reduction

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
calls to `hist!` may run in parallel

By using `map` on `temp_bins`, we then make a copy of each worker's bins that
we can safely return back to our current worker, and sum them together to get
our total histogram.
