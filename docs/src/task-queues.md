# Task Queues

By default, `@spawn`/`spawn` submit tasks immediately and directly into
Dagger's scheduler without modifications. However, sometimes you want to be
able to tweak this behavior for a region of code; for example, when working
with GPUs or other operations which operate in-place, you might want to emulate
CUDA's stream semantics by ensuring that tasks execute sequentially (to avoid
one kernel reading from an array while another kernel is actively writing to
it). Or, you might want to ensure that a set of Dagger tasks are submitted into
the scheduler all at once for benchmarking purposes or to emulate the behavior
of `delayed`. This and more is possible through a mechanism called "task
queues".

A task queue in Dagger is an object that can be configured to accept unlaunched
tasks from `@spawn`/`spawn` and either modify them or delay their launching
arbitrarily. By default, Dagger tasks are enqueued through the
`DefaultTaskQueue`, which submits tasks directly into the scheduler before
`@spawn`/`spawn` returns. However, Dagger also has an `InOrderTaskQueue`, which
ensures that tasks enqueued through it execute sequentially with respect to
each other. This queue can be allocated with `Dagger.spawn_sequential`:

```julia
A = rand(16)
B = zeros(16)
C = zeros(16)
function vcopy!(B, A)
    B .= A .+ 1.0
    return
end
function vadd!(C, A, B)
    C .+= A .+ B
    return
end
wait(Dagger.spawn_sequential() do
    Dagger.@spawn vcopy!(B, A)
    Dagger.@spawn vadd!(C, A, B)
end)
```

In the above example, `vadd!` is guaranteed to wait until `vcopy!` is
completed, even though `vadd!` isn't taking the result of `vcopy!` as an
argument (which is how tasks are normally ordered).

What if we wanted to launch multiple `vcopy!` calls within a `spawn_sequential`
region and allow them to execute in parallel, but still ensure that the `vadd!`
happens after they all finish? In this case, we want to switch to another kind
of task queue: the `LazyTaskQueue`. This task queue batches up task submissions
into groups, so that all tasks enqueued with it are placed in the scheduler all
at once. But what would happen if we used this task queue (via `spawn_bulk`)
within a region using `spawn_sequential`:

```julia
A = rand(16)
B1 = zeros(16)
B2 = zeros(16)
C = zeros(16)
wait(Dagger.spawn_sequential() do
    Dagger.spawn_bulk() do
        Dagger.@spawn vcopy!(B1, A)
        Dagger.@spawn vcopy!(B2, A)
    end
    Dagger.@spawn vadd!(C, B1, B2)
end)
```

Conveniently, Dagger's task queues can be nested to get the expected behavior;
the above example will submit the two `vcopy!` tasks as a group (and they can
execute concurrently), while still ensuring that those two tasks finish before
the `vadd!` task executes.

!!! warn
    Task queues do not propagate to nested tasks; if a Dagger task launches
    another task internally, the child task doesn't inherit the task queue that
    the parent task was enqueued in.
