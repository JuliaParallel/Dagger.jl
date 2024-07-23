# Dynamic Scheduler Control

Normally, Dagger executes static graphs defined with `delayed` and `@par`.
However, it is possible for thunks to dynamically modify the graph at runtime,
and to generally exert direct control over the scheduler's internal state. The
`Dagger.sch_handle` function provides this functionality within a thunk:

```julia
function mythunk(x)
    h = Dagger.sch_handle()
    Dagger.halt!(h)
    return x
end
```

The above example prematurely halts a running scheduler at the next
opportunity using `Dagger.halt!`:

[`Dagger.halt!`](@ref)

There are a variety of other built-in functions available for
various uses:

[`Dagger.get_dag_ids`](@ref)
[`Dagger.add_thunk!`](@ref)

When working with thunks acquired from `get_dag_ids` or `add_thunk!`,
you will have `ThunkID` objects which refer to a thunk by ID. Scheduler
control functions which work with thunks accept or return `ThunkID`s. For
example, one can create a new thunk and get its result with `Base.fetch`:

```julia
function mythunk(x)
    h = Dagger.sch_handle()
    id = Dagger.add_thunk!(h, x) do y
        y + 1
    end
    return fetch(h, id)
end
```

Alternatively, `Base.wait` can be used when one does not wish to retrieve the
returned value of the thunk.

Users with needs not covered by the built-in functions should use the
`Dagger.exec!` function to pass a user-defined function, closure, or
callable struct to the scheduler, along with a payload which will be
provided to that function:

[`Dagger.exec!`](@ref)

Note that all functions called by `Dagger.exec!` take the scheduler's internal
lock, so it's safe to manipulate the internal `ComputeState` object within the
user-provided function.
