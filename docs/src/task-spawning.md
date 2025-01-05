```@meta
CurrentModule = Dagger
```

# Task Spawning

The main entrypoint to Dagger is `@spawn`:

`Dagger.@spawn [option=value]... f(args...; kwargs...)`

or `spawn` if it's more convenient:

`Dagger.spawn(f, Dagger.Options(options), args...; kwargs...)`

When called, it creates an [`DTask`](@ref) (also known as a "task" or
"thunk") object representing a call to function `f` with the arguments `args` and
keyword arguments `kwargs`. If it is called with other tasks as args/kwargs,
such as in `Dagger.@spawn f(Dagger.@spawn g())`, then, in this example, the
function `f` gets passed the results of executing `g()`, once that result is
available. If `g()` isn't yet finished executing, then the execution of `f`
waits on `g()` to complete before executing.

An important observation to make is that, for each argument to
`@spawn`/`spawn`, if the argument is the result of another `@spawn`/`spawn`
call (thus it's an [`DTask`](@ref)), the argument will be computed first, and then
its result will be passed into the function receiving the argument. If the
argument is *not* an [`DTask`](@ref) (instead, some other type of Julia object),
it'll be passed as-is to the function `f` (with some exceptions).

!!! note "Task / thread occupancy"
    By default, `Dagger` assumes that tasks saturate the thread they are running on and does not try to schedule other tasks on the thread.
    This default can be controlled by specifying [`Options`](@ref) (more details can be found under [Task and Scheduler options](@ref)).
    The section [Changing the thread occupancy](@ref) shows a runnable example of how to achieve this.

## Options

The [`Options`](@ref Dagger.Options) struct in the second argument position is
optional; if provided, it is passed to the scheduler to control its
behavior. [`Options`](@ref Dagger.Options) contains a `NamedTuple` of option
key-value pairs, which can be any field in [`Options`](@ref)
(see [Task and Scheduler options](@ref)).

## Simple example

Let's see a very simple directed acyclic graph (or DAG) constructed with Dagger:

```julia
using Dagger

add1(value) = value + 1
add2(value) = value + 2
combine(a...) = sum(a)

p = Dagger.@spawn add1(4)
q = Dagger.@spawn add2(p)
r = Dagger.@spawn add1(3)
s = Dagger.@spawn combine(p, q, r)

@assert fetch(s) == 16
```

The tasks `p`, `q`, `r`, and `s` have the following structure:

![graph](https://user-images.githubusercontent.com/25916/26920104-7b9b5fa4-4c55-11e7-97fb-fe5b9e73cae6.png)

The final result (from `fetch(s)`) is the obvious consequence of the operation:

 `add1(4) + add2(add1(4)) + add1(3)`

 `(4 + 1) + ((4 + 1) + 2) + (3 + 1) == 16`

### Eager Execution

Dagger's `@spawn` macro works similarly to `@async` and `Threads.@spawn`: when
called, it wraps the function call specified by the user in an
[`DTask`](@ref) object, and immediately places it onto a running scheduler,
to be executed once its dependencies are fulfilled.

```julia
x = rand(400,400)
y = rand(400,400)
zt = Dagger.@spawn x * y
z = fetch(zt)
@assert isapprox(z, x * y)
```

One can also `wait` on the result of `@spawn` and check completion status with
`isready`:

```julia
x = Dagger.@spawn sleep(10)
@assert !isready(x)
wait(x)
@assert isready(x)
```

Like `@async` and `Threads.@spawn`, `Dagger.@spawn` synchronizes with
locally-scoped `@sync` blocks:

```julia
function sleep_and_print(delay, str)
    sleep(delay)
    println(str)
end
@sync begin
    Dagger.@spawn sleep_and_print(3, "I print first")
end
wait(Dagger.@spawn sleep_and_print(1, "I print second"))
```

One can also safely call `@spawn` from another worker (not ID 1), and it will be executed correctly:

```
x = fetch(Distributed.@spawnat 2 Dagger.@spawn 1+2) # fetches the result of `@spawnat`
x::DTask
@assert fetch(x) == 3 # fetch the result of `@spawn`
```

This is useful for nested execution, where an `@spawn`'d task calls `@spawn`.
This is detailed further in [Dynamic Scheduler Control](@ref).

## Errors

If a task errors while running under the eager scheduler, it will be marked as
having failed, all dependent (downstream) tasks will be marked as failed, and
any future tasks that use a failed task as input will fail. Failure can be
determined with `fetch`, which will re-throw the error that the
originally-failing task threw. `wait` and `isready` will *not* check whether a
task or its upstream failed; they only check if the task has completed, error
or not.

This failure behavior is not the default for lazy scheduling ([Lazy API](@ref)),
but can be enabled by setting the scheduler/task option ([Task and Scheduler options](@ref))
`allow_error` to `true`.  However, this option isn't terribly useful for
non-dynamic usecases, since any task failure will propagate down to the output
task regardless of where it occurs.

## Cancellation

Sometimes a task runs longer than expected (maybe it's hanging due to a bug),
or the user decides that they don't want to wait on a task to run to
completion. In these cases, Dagger provides the `Dagger.cancel!` function,
which allows for stopping a task while it's running, or terminating it before
it gets the chance to start running.

```julia
t = Dagger.@spawn sleep(1000)
# We're bored, let's cancel `t`
Dagger.cancel!(t)
```

`Dagger.cancel!` is generally safe to call, as it will not actually *force* a
task to stop; instead, Dagger will simply "abandon" the task and allow it to
finish on its own in the background, and it will not block the execution of
other `DTask`s that are queued to run. It is possible to force-cancel a task by
doing `Dagger.cancel!(t; force=true)`, but this is generally discouraged, as it
can cause memory leaks, hangs, and segfaults.

If it's desired to cancel all tasks that are scheduled or running, one can call
`Dagger.cancel!()`, and all tasks will be abandoned (or force-cancelled, if
specified). Additionally, if Dagger's scheduler needs to be restarted for any
reason, one can call `Dagger.cancel!(;halt_sch=true)` to stop the scheduler and
all tasks. The scheduler will be automatically restarted on the next
`@spawn`/`spawn` call.

## Lazy API

Alongside the modern eager API, Dagger also has a legacy lazy API, accessible
via `@par` or `delayed`. The above computation can be executed with the lazy
API by substituting `@spawn` with `@par` and `fetch` with `collect`:

```julia
p = Dagger.@par add1(4)
q = Dagger.@par add2(p)
r = Dagger.@par add1(3)
s = Dagger.@par combine(p, q, r)

@assert collect(s) == 16
```

or similarly, in block form:

```julia
s = Dagger.@par begin
    p = add1(4)
    q = add2(p)
    r = add1(3)
    combine(p, q, r)
end

@assert collect(s) == 16
```

Alternatively, if you want to compute but not fetch the result of a lazy
operation, you can call `compute` on the task. This will return a `Chunk`
object which references the result (see [Chunks](@ref) for more details):

```julia
x = Dagger.@par 1+2
cx = compute(x)
cx::Chunk
@assert collect(cx) == 3
```

Note that, as a legacy API, usage of the lazy API is generally discouraged for modern usage of Dagger. The reasons for this are numerous:
- Nothing useful is happening while the DAG is being constructed, adding extra latency
- Dynamically expanding the DAG can't be done with `@par` and `delayed`, making recursive nesting annoying to write
- Each call to `compute`/`collect` starts a new scheduler, and destroys it at the end of the computation, wasting valuable time on setup and teardown
- Distinct schedulers don't share runtime metrics or learned parameters, thus causing the scheduler to act less intelligently
- Distinct schedulers can't share work or data directly

## Task and Scheduler options

While Dagger generally "just works", sometimes one needs to exert some more
fine-grained control over how the scheduler allocates work. There are two
parallel mechanisms to achieve this: Task options (from [`Options`](@ref)) and
Scheduler options (from [`Sch.SchedulerOptions`](@ref)). These two options
structs contain many shared options, with the difference being that Scheduler
options operate globally across an entire DAG, and Task options operate on a
task-by-task basis.

Scheduler options can be constructed and passed to `collect()` or `compute()`
as the keyword argument `options` for lazy API usage:

```julia
t = Dagger.@par 1+2
opts = Dagger.Sch.SchedulerOptions(;single=1) # Execute on worker 1

compute(t; options=opts)

collect(t; options=opts)
```

Task options can be passed to `@spawn/spawn`, `@par`, and `delayed` similarly:

```julia
# Execute on worker 1

Dagger.@spawn single=1 1+2
Dagger.spawn(+, Dagger.Options(;single=1), 1, 2)

delayed(+; single=1)(1, 2)
```

## Changing the thread occupancy

One of the supported [`Options`](@ref) is the `occupancy` keyword.
This keyword can be used to communicate that a task is not expected to fully
saturate a CPU core (e.g. due to being IO-bound).
The basic usage looks like this:

```julia
Dagger.@spawn occupancy=Dict(Dagger.ThreadProc=>0) fn
```

Consider the following function definitions:

```julia
using Dagger

function inner()
    sleep(0.1)
end

function outer_full_occupancy()
    @sync for _ in 1:2
        # By default, full occupancy is assumed
        Dagger.@spawn inner()
    end
end

function outer_low_occupancy()
    @sync for _ in 1:2
        # Here, we're explicitly telling the scheduler to assume low occupancy
        Dagger.@spawn occupancy=Dict(Dagger.ThreadProc => 0) inner()
    end
end
```

When running the first outer function N times in parallel, you should see parallelization until all threads are blocked:

```julia
for N in [1, 2, 4, 8, 16]
    @time fetch.([Dagger.@spawn outer_full_occupancy() for _ in 1:N])
end
```

The results from the above code snippet should look similar to this (the timings will be influenced by your specific machine):

```text
  0.124829 seconds (44.27 k allocations: 3.055 MiB, 12.61% compilation time)
  0.104652 seconds (14.80 k allocations: 1.081 MiB)
  0.110588 seconds (28.94 k allocations: 2.138 MiB, 4.91% compilation time)
  0.208937 seconds (47.53 k allocations: 2.932 MiB)
  0.527545 seconds (79.35 k allocations: 4.384 MiB, 0.64% compilation time)
```

Whereas running the outer function that communicates a low occupancy (`outer_low_occupancy`) should run fully in parallel:

```julia
for N in [1, 2, 4, 8, 16]
    @time fetch.([Dagger.@spawn outer_low_occupancy() for _ in 1:N])
end
```

In comparison, the `outer_low_occupancy` snippet should show results like this:

```text
  0.120686 seconds (44.38 k allocations: 3.070 MiB, 13.00% compilation time)
  0.105665 seconds (15.40 k allocations: 1.072 MiB)
  0.107495 seconds (28.56 k allocations: 1.940 MiB)
  0.109904 seconds (55.03 k allocations: 3.631 MiB)
  0.117239 seconds (87.95 k allocations: 5.372 MiB)
```
