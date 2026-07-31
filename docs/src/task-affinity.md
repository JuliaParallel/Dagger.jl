# Task Affinity 

Dagger's allows for precise control over task placement and result availability using scopes. Tasks are assigned based on the combination of multiple scopes: `scope`/`compute_scope`, and `result_scope` (which can all be specified with `@spawn`), and additionally the scopes of any arguments to the task (in the form of a scope attached to a `Chunk` argument). Let's take a look at how to configure these scopes, and how they work together to direct task placement.

For more information on how scopes work, see [Scopes](@ref).

---

## Task Scopes

### Scope

`scope` defines the general set of locations where a Dagger task can execute. If `scope` is not specified, the task falls back to `DefaultScope()`, allowing it to run wherever execution is possible. Execution occurs on any worker within the defined scope.

**Example:**
```julia
g = Dagger.@spawn scope=Dagger.scope(worker=3) f(x,y)
```
Task `g` executes only on worker 3. Its result can be accessed by any worker.

---

### Compute Scope

Like `scope`, `compute_scope` also specifies where a Dagger task can execute. The key difference is if both `compute_scope` and `scope` are provided, `compute_scope` takes precedence over `scope` for execution placement. If neither is specified, then they default to `DefaultScope()`. 

**Example:**
```julia
g1 = Dagger.@spawn scope=Dagger.scope(worker=2,thread=3) compute_scope=Dagger.scope((worker=1, thread=2), (worker=3, thread=1))  f(x,y)
g2 = Dagger.@spawn compute_scope=Dagger.scope((worker=1, thread=2), (worker=3, thread=1)) f(x,y)
```
Tasks `g1` and `g2` execute on either thread 2 of worker 1, or thread 1 of worker 3. The `scope` argument to `g1` is ignored. Their result can be accessed by any worker.

---

### Result Scope  

The result_scope limits the processors from which a task's result can be accessed. This can be useful for managing data locality and minimizing transfers. If `result_scope` is not specified, it defaults to `AnyScope()`, meaning the result can be accessed by any processor (including those not default enabled for task execution, such as GPUs).

**Example:**
```julia
g = Dagger.@spawn result_scope=Dagger.scope(worker=3, threads=[1, 3, 4]) f(x,y)
```
The result of `g` is accessible only from threads 1, 3 and 4 of worker process 3. The task's execution may happen anywhere on threads 1, 3 and 4 of worker 3.

---

## Interaction of `compute_scope` and `result_scope`

When `scope`/`compute_scope` and `result_scope` are specified, the scheduler executes the task on the intersection of the effective compute scope (which will be `compute_scope` if provided, otherwise `scope`) and the `result_scope`. If the intersection is empty, then the scheduler throws a `Dagger.Sch.SchedulerException` error.

**Example:**
```julia
g = Dagger.@spawn scope=Dagger.scope(worker=3,thread=2) compute_scope=Dagger.scope(worker=2) result_scope=Dagger.scope((worker=2, thread=2), (worker=4, thread=2)) f(x,y)
```
The task `g` computes on thread 2 of worker 2 (as it's the intersection of compute and result scopes), but accessng its result is restricted to thread 2 of worker 2 and thread 2 of worker 4.

---

## Function as a Chunk

This section explains how `scope`/`compute_scope` and `result_scope` affect tasks when a `Chunk` is used to specify the function to be executed by `@spawn` (e.g. created via `Dagger.tochunk(...)` or by calling `fetch(task; raw=true)` on a task). This may seem strange (to use a `Chunk` to specify the function to be executed), but it can be useful with working with callable structs, such as closures or Flux.jl models.

Assume `g` is some function, e.g. `g(x, y) = x * 2 + y * 3`, and `chunk_scope` is its defined affinity.

When `Dagger.tochunk(...)` is used to pass a `Chunk` as the function to be executed by `@spawn`:
- The result is accessible only on processors in `chunk_scope`.
- Dagger validates that there is an intersection between `chunk_scope`, the effective `compute_scope` (derived from `@spawn`'s `compute_scope` or `scope`), and the `result_scope`. If no intersection exists, the scheduler throws an exception.

!!! info While `chunk_proc` is currently required when constructing a chunk, it is only used to pick the most optimal processor for accessing the chunk; it does not affect which set of processors the task may execute on.

**Usage:**
```julia
chunk_scope = Dagger.scope(worker=3)
chunk_proc = Dagger.OSProc(3) # not important, just needs to be a valid processor
g(x, y) = x * 2 + y * 3
g_chunk = Dagger.tochunk(g, chunk_proc, chunk_scope)
h1 = Dagger.@spawn scope=Dagger.scope(worker=3) g_chunk(10, 11)
h2 = Dagger.@spawn compute_scope=Dagger.scope((worker=1, thread=2), (worker=3, thread=1)) g_chunk(20, 21)
h3 = Dagger.@spawn scope=Dagger.scope(worker=2,thread=3) compute_scope=Dagger.scope((worker=1, thread=2), (worker=3, thread=1)) g_chunk(30, 31)
h4 = Dagger.@spawn result_scope=Dagger.scope(worker=3) g_chunk(40, 41)
h5 = Dagger.@spawn scope=Dagger.scope(worker=3,thread=2) compute_scope=Dagger.scope(worker=3) result_scope=Dagger.scope(worker=3,threads=[2,3]) g_chunk(50, 51)
```
In all these cases (`h1` through `h5`), the tasks get executed on any processor within `chunk_scope` and its result is accessible only within `chunk_scope`.

---

## Chunk arguments

This section details behavior when some or all of a task's arguments are `Chunk`s.

Assume `g(x, y) = x * 2 + y * 3`, and `arg = Dagger.tochunk(g(1, 2), arg_proc, arg_scope)`, where `arg_scope` is the argument's defined scope. Assume `arg_scope = Dagger.scope(worker=2)`.

### Scope
If `arg_scope` and `scope` do not intersect, the scheduler throws an exception. Execution occurs on the intersection of `scope` and `arg_scope`.

```julia
h = Dagger.@spawn scope=Dagger.scope(worker=2) g(arg, 11)
```
Task `h` executes on any worker within the intersection of `scope` and `arg_scope`. The result is accessible from any processor.

---

### Compute scope and Chunk argument scopes interaction 
If `arg_scope` and `compute_scope` do not intersect, the scheduler throws an exception. Otherwise, execution happens on the intersection of the effective compute scope (which will be `compute_scope` if provided, otherwise `scope`) and `arg_scope`.

```julia
h1 = Dagger.@spawn compute_scope=Dagger.scope((worker=1, thread=2), (worker=2, thread=1)) g(arg, 11)
h2 = Dagger.@spawn scope=Dagger.scope(worker=2,thread=3) compute_scope=Dagger.scope((worker=1, thread=2), (worker=2, thread=1)) g(arg, 21)
```
Tasks `h1` and `h2` execute on any processor within the intersection of the `compute_scope` and `arg_scope`. `scope` is ignored if `compute_scope` is specified. The result is accessible from any processor.

---

### Result scope and Chunk argument scopes interaction
If only `result_scope` is specified, computation happens on any processor within the intersection of `arg_scope` and `result_scope`, and the result is only accessible within `result_scope`.

```julia
h = Dagger.@spawn result_scope=Dagger.scope(worker=2) g(arg, 11)
```
Task `h` executes on any processor within the intersection of `arg_scope` and `result_scope`. The result is accessible from only within `result_scope`.

---

### Compute, result, and chunk argument scopes interaction  
When `scope`/`compute_scope`, `result_scope`, and `Chunk` argument scopes are all used, the scheduler executes the task on the intersection of `arg_scope`, the effective compute scope (which is `compute_scope` if provided, otherwise `scope`), and `result_scope`. If no intersection exists, the scheduler throws an exception.

```julia
h = Dagger.@spawn scope=Dagger.scope(worker=3,thread=2) compute_scope=Dagger.scope(worker=2) result_scope=Dagger.scope((worker=2, thread=2), (worker=4, thread=2)) g(arg, 31)
```
Task `h` computes on thread 2 of worker 2 (as it's the intersection of `arg_scope`, `compute_scope`, and `result_scope`), and its result access is restricted to thread 2 of worker 2 or thread 2 of worker 4.
