```@meta
CurrentModule = Dagger
```

# Task Affinity


Dagger.jl's `@spawn` macro offers fine-grained control over task execution by using the `compute_scope` and `result_scope` options to precisely control where tasks run and where their results can be accessed.

## Compute Scope

`compute_scope` defines exactly where a task's computation must occur. This option overrides the standard `scope` option if both are provided.

```julia
g = Dagger.@spawn compute_scope=ExactScope(Dagger.ThreadProc(3, 1)) f(x,y)
```

In this example, task `f(x,y)` is scheduled to run specifically on thread 1 of processor 3.


## Result Scope

`result_scope` restricts the locations from which a task's result can be fetched. This is useful for managing data locality and access patterns.

```julia
g = Dagger.@spawn result_scope=ExactScope(Dagger.OSProc(2)) f(x,y)
```

Here, the result of `f(x,y)` (referenced by `g`) will be primarily accessible from worker process 2. Fetching from other locations might require data movement.

## Interaction of compute_scope and result_scope

When both `compute_scope` and `result_scope` are specified for a task, Scheduler determines the execution location based on their intersection:

- **Intersection Exists:** If there is an intersection between the compute_scope and result_scope, the task's computation will be scheduled to occur within this intersection. This is the preferred scenario.

- **No Intersection:** If there is no intersection, the task's computation will occur in the compute_scope. However, the result_scope will still be respected for accessing the result.
 
### Syntax:
```julia
g = Dagger.@spawn compute_scope=ExactScope(Dagger.ThreadProc(3, 1)) result_scope=ExactScope(Dagger.ThreadProc(2, 2)) f(x,y)
```

In this case, the task computes on `Dagger.ThreadProc(3, 1)`. Result access is restricted to `Dagger.ThreadProc(2, 2)`.

!!! note "Chunk Inputs"
    If the input to `Dagger.@spawn` is already a `Dagger.tochunk`, the `compute_scope` and `result_scope` options will have no effect on the task's execution or result accessibility.
