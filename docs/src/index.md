# A framework for out-of-core and parallel execution

## Usage

The main entrypoint to Dagger is `@spawn`:

`Dagger.@spawn [option=value]... f(args...; kwargs...)`

or `spawn` if it's more convenient:

`Dagger.spawn(f, Dagger.Options(options), args...; kwargs...)`

When called, it creates an `EagerThunk` (also known as a "thunk" or "task")
object representing a call to function `f` with the arguments `args` and
keyword arguments `kwargs`. If it is called with other thunks as args/kwargs,
such as in `Dagger.@spawn f(Dagger.@spawn g())`, then the function `f` gets
passed the results of those input thunks, once they're available. If those
thunks aren't yet finished executing, then the execution of `f` waits on all of
its input thunks to complete before executing.

The key point is that, for each argument to a thunk, if the argument is an
`EagerThunk`, it'll be executed before this node and its result will be passed
into the function `f`. If the argument is *not* an `EagerThunk` (instead, some
other type of Julia object), it'll be passed as-is to the function `f`.

The `Options` struct in the second argument position is optional; if provided,
it is passed to the scheduler to control its behavior. `Options` contains a
`NamedTuple` of option key-value pairs, which can be any of:
- Any field in `Dagger.Sch.ThunkOptions` (see [Scheduler and Thunk options](@ref))
- `meta::Bool` -- Pass the input `Chunk` objects themselves to `f` and not the value contained in them

There are also some extra optionss that can be passed, although they're considered advanced options to be used only by developers or library authors:
- `get_result::Bool` -- return the actual result to the scheduler instead of `Chunk` objects. Used when `f` explicitly constructs a Chunk or when return value is small (e.g. in case of reduce)
- `persist::Bool` -- the result of this Thunk should not be released after it becomes unused in the DAG
- `cache::Bool` -- cache the result of this Thunk such that if the thunk is evaluated again, one can just reuse the cached value. If it’s been removed from cache, recompute the value.

### Simple example

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

The thunks `p`, `q`, `r`, and `s` have the following structure:

![graph](https://user-images.githubusercontent.com/25916/26920104-7b9b5fa4-4c55-11e7-97fb-fe5b9e73cae6.png)

The final result (from `fetch(s)`) is the obvious consequence of the operation:

 `add1(4) + add2(add1(4)) + add1(3)`

 `(4 + 1) + ((4 + 1) + 2) + (3 + 1) == 16`

### Eager Execution

Dagger's `@spawn` macro works similarly to `@async` and `Threads.@spawn`: when
called, it wraps the function call specified by the user in an `EagerThunk`
object, and immediately places it onto a running scheduler, to be executed once
its dependencies are fulfilled.

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
x::EagerThunk
@assert fetch(x) == 3 # fetch the result of `@spawn`
```

This is useful for nested execution, where an `@spawn`'d thunk calls `@spawn`. This is detailed further in [Dynamic Scheduler Control](@ref).

### Errors

If a thunk errors while running under the eager scheduler, it will be marked as
having failed, all dependent (downstream) thunks will be marked as failed, and
any future thunks that use a failed thunk as input will fail. Failure can be
determined with `fetch`, which will re-throw the error that the
originally-failing thunk threw. `wait` and `isready` will *not* check whether a
thunk or its upstream failed; they only check if the thunk has completed, error
or not.

This failure behavior is not the default for lazy scheduling ([Lazy API](@ref)),
but can be enabled by setting the scheduler/thunk option ([Scheduler and Thunk options](@ref))
`allow_error` to `true`.  However, this option isn't terribly useful for
non-dynamic usecases, since any thunk failure will propagate down to the output
thunk regardless of where it occurs.

### Lazy API

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
operation, you can call `compute` on the thunk. This will return a `Chunk`
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

## Chunks

Dagger relies heavily on communication between workers to operate. To make this
efficient when communicating potentially large units of data, Dagger uses a
remote reference, called a `Dagger.Chunk`, to refer to objects which exist on
another worker. `Chunk`s are backed by a distributed refcounting mechanism
provided by MemPool.jl, which ensures that the referenced data is not GC'd
until all `Chunk`s referencing that object are GC'd from all workers containing
them. Conveniently, if you pass in a `Chunk` object as an input to a function
using either API, then the thunk's payload function will get executed with the
value contained in the `Chunk`. The scheduler also understands `Chunk`s, and
will try to schedule work close to where their `Chunk` inputs reside, to reduce
communication overhead.

`Chunk`s also have a cached type, a "processor", and a "scope", which are
important for identifying the type of the object, where in memory (CPU RAM, GPU
VRAM, etc.) the value resides, and where the value is allowed to be transferred
and dereferenced. See [Processors](@ref) and [Scopes](@ref) for more details on
how these properties can be used to control scheduling behavior around `Chunk`s.

### Scheduler and Thunk options

While Dagger generally "just works", sometimes one needs to exert some more
fine-grained control over how the scheduler allocates work. There are two
parallel mechanisms to achieve this: Scheduler options (from
`Dagger.Sch.SchedulerOptions`) and Thunk options (from
`Dagger.Sch.ThunkOptions`). These two options structs contain many shared
options, with the difference being that Scheduler options operate
globally across an entire DAG, and Thunk options operate on a thunk-by-thunk
basis.

Scheduler options can be constructed and passed to `collect()` or `compute()`
as the keyword argument `options` for lazy API usage:

```julia
t = Dagger.@par 1+2
opts = Dagger.Sch.SchedulerOptions(;single=1) # Execute on worker 1

compute(t; options=opts)

collect(t; options=opts)
```

Thunk options can be passed to `@spawn/spawn`, `@par`, and `delayed` similarly:

```julia
# Execute on worker 1

Dagger.@spawn single=1 1+2
Dagger.spawn(+, Dagger.Options(;single=1), 1, 2)

delayed(+; single=1)(1, 2)
```

### Core vs. Worker Schedulers

Dagger's scheduler is really two kinds of entities: the "core" scheduler, and
"worker" schedulers:

The core scheduler runs on worker 1, thread 1, and is the entrypoint to tasks
which have been submitted. The core scheduler manages all task dependencies,
notifies calls to `wait` and `fetch` of task completion, and generally performs
initial task placement. The core scheduler has cached information about each
worker and their processors, and uses that information (together with metrics
about previous tasks and other aspects of the Dagger runtime) to generate a
near-optimal just-in-time task schedule.

The worker schedulers each run as a set of tasks across all workers and all
processors, and handles data movement and task execution. Once the core
scheduler has scheduled and launched a task, it arrives at the worker scheduler
for handling. The worker scheduler will pass the task to a queue for the
assigned processor, where it will wait until the processor has a sufficient
amount of "occupancy" for the task. Once the processor is ready for the task,
it will first fetch all arguments to the task from other workers, and then it
will execute the task, package the result into a `Chunk`, and pass that back to
the core scheduler.

### Workload Balancing

In general, Dagger's core scheduler tries to balance workloads as much as
possible across all the available processors, but it can fail to do so
effectively when either the cached per-processor information is outdated, or
when the estimates about the task's behavior are inaccurate. To minimize the
impact of this potential workload imbalance, the worker schedulers' processors
will attempt to steal tasks from each other when they are under-occupied. Tasks
will only be stolen if their [scope](`Scopes`) matches the processor attempting
the steal, so tasks with wider scopes have better balancing potential.

### Scheduler/Thunk Options

[`Dagger.Sch.SchedulerOptions`](@ref)
[`Dagger.Sch.ThunkOptions`](@ref)
