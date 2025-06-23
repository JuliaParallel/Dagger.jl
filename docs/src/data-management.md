# Data Management

Dagger is not just a computing platform - it also has awareness of where each
piece of data resides, and will move data between workers and perform
conversions as necessary to satisfy the needs of your tasks.

## Chunks

Dagger often needs to move data between workers to allow a task to execute. To
make this efficient when communicating potentially large units of data, Dagger
uses a remote reference, called a `Chunk`, to refer to objects which may
exist on another worker. `Chunk`s are backed by a distributed refcounting
mechanism provided by MemPool.jl, which ensures that the referenced data is not
garbage collected until all `Chunk`s referencing that object are GC'd from all
workers.

Conveniently, if you pass in a `Chunk` object as an input to a Dagger task,
then the task's payload function will get executed with the value contained in
the `Chunk`. The scheduler also understands `Chunk`s, and will try to schedule
tasks close to where their `Chunk` inputs reside, to reduce communication
overhead.

`Chunk`s also have a cached type, a "processor", and a "scope", which are
important for identifying the type of the object, where in memory (CPU RAM, GPU
VRAM, etc.) the value resides, and where the value is allowed to be transferred
and dereferenced. See [Processors](@ref) and [Scopes](@ref) for more details on
how these properties can be used to control scheduling behavior around `Chunk`s.

## Data movement rules

Dagger utilizes a 3-argument function `Dagger.move(from_proc::Dagger.Processor, to_proc::Dagger.Processor, x)` to manage data movement between processors. This function is invoked by the scheduler for every argument of a task, including the task's function itself, before the task is executed. The purpose of `move` is to transfer the argument `x` from its current processor (`from_proc`) to the target processor (`to_proc`) where the task will run.

This `move` mechanism is fundamental to how Dagger handles `Chunk` objects. When a `Chunk` is passed as an argument to a task, the `move` function is responsible for unwrapping the `Chunk` and providing its underlying value to the task.

While users can define custom `move` implementations for their specific data types if needed, the default fallback implementation of `move` is designed to handle most common use cases effectively. Therefore, custom implementations are generally unnecessary.

Here's an example of a custom `move` implementation:

```julia
struct MyCustomType
    data::Vector{Int}
end

# Custom move function for MyCustomType
function Dagger.move(from_proc::Dagger.Processor, to_proc::Dagger.Processor, x::MyCustomType)
    return MyCustomType(copy(x.data))
end

A = MyCustomType(rand(100))
s = fetch(Dagger.@spawn sum(A))
@assert s == sum(A.data)
```

## Mutation

Normally, Dagger tasks should be functional and "pure": never mutating their
inputs, always producing identical outputs for a given set of inputs, and never
producing side effects which might affect future program behavior. However, for
certain codes, this restriction ends up costing the user performance and
engineering time to work around.

Thankfully, Dagger provides the `Dagger.@mutable` macro for just this purpose.
`@mutable` allows data to be marked such that it will never be copied or
serialized by the scheduler (unless copied by the user). When used as an
argument to a task, the task will be forced to execute on the same worker that
`@mutable` was called on. For example:

```julia
x = Dagger.@mutable worker=2 Threads.Atomic{Int}(0)
x::Dagger.Chunk # The result is always a `Chunk`

# x is now considered mutable, and may only be accessed on worker 2:
wait(Dagger.@spawn Threads.atomic_add!(x, 1)) # Always executed on worker 2
wait(Dagger.@spawn scope=Dagger.scope(worker=1) Threads.atomic_add!(x, 1)) # SchedulingException
```

`@mutable`, when called as above, is constructed on worker 2, and the data
gains a scope of `ProcessScope(myid())`, which means that any processor on that
worker is allowed to execute tasks that use the object (subject to the usual
scheduling rules).

`@mutable` also allows the scope to be manually supplied, if more specific
restrictions are desirable:

```julia
x = @mutable scope=Dagger.scope(worker=1, threads=[3,4]) rand(100)
# x is now scoped to threads 3 and 4 on worker `myid()`
```

## Sharding

`@mutable` is convenient for creating a single mutable object, but often one
wants to have multiple mutable objects, with each object being scoped to their
own worker or thread in the cluster, to be used as local counters, partial
reduction containers, data caches, etc.

The `Shard` object (constructed with `Dagger.@shard`/`Dagger.shard`) is a
mechanism by which such a setup can be created with one invocation.  By
default, each worker will have their own local object which will be used when a
task that uses the shard as an argument is scheduled on that worker. Other
shard pieces that aren't scoped to the processor being executed on will not be
serialized or copied, keeping communication costs constant even with a very
large shard.

This mechanism makes it easy to construct a distributed set of mutable objects
which are treated as "mirrored shards" by the scheduler, but require no further
user input to access. For example, creating and using a local counter for each
worker is trivial:

```julia
# Create a local atomic counter on each worker that Dagger knows about:
cs = Dagger.@shard Threads.Atomic{Int}(0)

# Let's add `1` to the local counter, not caring about which worker we're on:
wait.([Dagger.@spawn Threads.atomic_add!(cs, 1) for i in 1:1000])

# And let's fetch the total sum of all counters:
@assert sum(map(ctr->fetch(ctr)[], cs)) == 1000
```

Note that `map`, when used on a shard, will execute the provided function once
per shard "piece", and each result is considered immutable. `map` is an easy
way to make a copy of each piece of the shard, to be later reduced, scanned,
etc.

Further details about what arguments can be passed to `@shard`/`shard` can be found in [Data Management Functions](@ref).
