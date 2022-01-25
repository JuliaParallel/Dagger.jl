# Option Propagation

Most options passed to Dagger are passed via `delayed` or `Dagger.@spawn`
directly. This works well when an option only needs to be set for a single
thunk, but is cumbersome when the same option needs to be set on multiple
thunks, or set recursively on thunks spawned within other thunks. Thankfully,
Dagger provides the `with_options` function to make this easier. This function
is very powerful, by nature of using "context variables"; let's first see some
example code to help explain it:

```julia
function f(x)
    m = Dagger.@spawn myid()
    return Dagger.@spawn x+m
end
Dagger.with_options(;scope=ProcessScope(2)) do
    @sync begin
        @async @assert fetch(Dagger.@spawn f(1)) == 3
        @async @assert fetch(Dagger.@spawn f(2)) == 4
    end
end
```

In the above example, `with_options` sets the scope for both `Dagger.@spawn
f(1)` and `Dagger.@spawn f(2)` to `ProcessScope(2)` (locking Dagger tasks to
worker 2). This is of course very useful for ensuring that a set of operations
use a certain scope. What it *also* does, however, is propagates this scope
through calls to `@async`, `Threads.@spawn`, and `Dagger.@spawn`; this means
that the task spawned by `f(x)` *also* inherits this scope! This works thanks
to the magic of context variables, which are inherited recursively through
child tasks, and thanks to Dagger intentionally propagating the scope (and
other options passed to `with_options`) across the cluster, ensuring that no
matter how deep the recursive task spawning goes, the options are maintained.

It's also possible to retrieve the options currently set by `with_options`,
using `Dagger.get_options`:

```julia
Dagger.with_options(;scope=ProcessScope(2)) do
    fetch(@async @assert Dagger.get_options().scope == ProcessScope(2))
    # Or:
    fetch(@async @assert Dagger.get_options(:scope) == ProcessScope(2))
    # Or, if `scope` might not have been propagated as an option, we can give
    # it a default value:
    fetch(@async @assert Dagger.get_options(:scope, AnyScope()) == ProcessScope(2))
end
```

This is a very powerful concept: with a single call to `with_options`, we can
apply any set of options to any nested set of operations. This is great for
isolating large workloads to different workers or processors, defining global
checkpoint/restore behavior, and more.
