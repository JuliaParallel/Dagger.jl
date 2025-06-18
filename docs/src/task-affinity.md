# Task Affinity 

Dagger.jl's `@spawn` macro allows precise control over task execution and result accessibility using `scope`, `compute_scope`, and `result_scope`, which specify various chunk scopes of the task.

For more information on how these scopes work, see [Scopes](scopes.md#Scopes).

---

## Key Terms

### Scope  
`scope` defines the general set of locations where a Dagger task can execute. If `scope` is not explicitly set, the task runs within the `compute_scope`. If both `scope` and `compute_scope` both are unspecified, the task falls back to `DefaultScope()`, allowing it to run wherever execution is possible. Execution occurs on any worker within the defined scope.

**Example:**
```julia
g = Dagger.@spawn scope=Dagger.scope(worker=3) f(x,y)
```
Task `g` executes only on worker 3. Its result can be accessed by any worker.

---

### Compute Scope  
Like `scope`,`compute_scope` also specifies where a Dagger task can execute. The key difference is if both `compute_scope` and `scope` are provided, `compute_scope` takes precedence over `scope` for execution placement. If neither is specified, the they default to `DefaultScope()`. 

**Example:**
```julia
g1 = Dagger.@spawn scope=Dagger.scope(worker=2,thread=3) compute_scope=Dagger.scope((worker=1, thread=2), (worker=3, thread=1))  f(x,y)
g2 = Dagger.@spawn compute_scope=Dagger.scope((worker=1, thread=2), (worker=3, thread=1)) f(x,y)
```
Tasks `g1` and `g2` execute on either thread 2 of worker 1, or thread 1 of worker 3. The `scope` argument to `g1` is ignored. Their result can be accessed by any worker.

---

### Result Scope  

The result_scope limits the workers from which a task's result can be accessed. This is crucial for managing data locality and minimizing transfers. If `result_scope` is not specified, it defaults to `AnyScope()`, meaning the result can be accessed by any worker.

**Example:**
```julia
g = Dagger.@spawn result_scope=Dagger.scope(worker=3, threads=[1,3, 4]) f(x,y)
```
The result of `g` is accessible only from threads 1, 3 and 4 of worker process 3. The task's execution may happen anywhere on threads 1, 3 and 4 of worker 3.

---

## Interaction of `compute_scope` and `result_scope`

When `scope`, `compute_scope`, and `result_scope` are all used, the scheduler executes the task on the intersection of the effective compute scope (which will be `compute_scope` if provided, otherwise `scope`) and the `result_scope`. If the intersection is empty then the scheduler throws a `Dagger.Sch.SchedulerException` error.

**Example:**
```julia
g = Dagger.@spawn scope=Dagger.scope(worker=3,thread=2) compute_scope=Dagger.scope(worker=2) result_scope=Dagger.scope((worker=2, thread=2), (worker=4, thread=2)) f(x,y)
```
The task `g` computes on thread 2 of worker 2 (as it's the intersection of compute and result scopes), and its result access is also restricted to thread 2 of worker 2.

---

## Chunk Inputs to Tasks  

This section explains how `scope`, `compute_scope`, and `result_scope` affect tasks when a `Chunk` is the primary input to `@spawn` (e.g. created via `Dagger.tochunk(...)` or by calling `fetch(task; raw=true)` on a task).

Assume `g` is some function, e.g. `g(x, y) = x * 2 + y * 3`, `chunk_proc` is the chunk's processor, and `chunk_scope` is its defined accessibility.

When `Dagger.tochunk(...)` is directly spawned:
- The task executes on `chunk_proc`.
- The result is accessible only within `chunk_scope`.
- This behavior occurs irrespective of the `scope`, `compute_scope`, and `result_scope` values provided in the `@spawn` macro.
- Dagger validates that there is an intersection between the effective `compute_scope` (derived from `@spawn`'s `compute_scope` or `scope`) and the `result_scope`. If no intersection exists, the scheduler throws an exception.

!!! info While `chunk_proc` is currently required when constructing a chunk, it is largely unused in actual scheduling logic. It exists primarily for backward compatibility and may be deprecated in the future.

**Usage:**
```julia
h1 = Dagger.@spawn scope=Dagger.scope(worker=3) Dagger.tochunk(g(10, 11), chunk_proc, chunk_scope)
h2 = Dagger.@spawn compute_scope=Dagger.scope((worker=1, thread=2), (worker=3, thread=1)) Dagger.tochunk(g(20, 21), chunk_proc, chunk_scope)
h3 = Dagger.@spawn scope=Dagger.scope(worker=2,thread=3) compute_scope=Dagger.scope((worker=1, thread=2), (worker=3, thread=1)) Dagger.tochunk(g(30, 31), chunk_proc, chunk_scope)
h4 = Dagger.@spawn result_scope=Dagger.scope(worker=3) Dagger.tochunk(g(40, 41), chunk_proc, chunk_scope)
h5 = Dagger.@spawn scope=Dagger.scope(worker=3,thread=2) compute_scope=Dagger.ProcessScope(2) result_scope=Dagger.scope(worker=2,threads=[2,3]) Dagger.tochunk(g(50, 51), chunk_proc, chunk_scope)
```
In all these cases (`h1` through `h5`), the tasks get executed on processor `chunk_proc` of chunk, and its result is accessible only within `chunk_scope`.

---

## Function with Chunk Arguments as Tasks  

This section details behavior when `scope`, `compute_scope`, and `result_scope` are used with tasks where a function is the input, and its arguments include `Chunk`s.

Assume `g(x, y) = x * 2 + y * 3` is a function, and `arg = Dagger.tochunk(g(1, 2), arg_proc, arg_scope)` is a chunk argument, where `arg_proc` is the chunk's processor and `arg_scope` is its defined scope.

### Scope  
If `arg_scope` and `scope` do not intersect, the scheduler throws an exception. Execution occurs on the intersection of `scope` and `arg_scope`.

```julia
h = Dagger.@spawn scope=Dagger.scope(worker=3) g(arg, 11)
```
Task `h` executes on any worker within the intersection of `scope` and `arg_scope`. The result is accessible from any worker.

---

### Compute scope and Chunk argument scopes interaction 
If `arg_scope` and `compute_scope` do not intersect, the scheduler throws an exception. Otherwise, execution happens on the intersection of the effective compute scope (which will be `compute_scope` if provided, otherwise `scope`) and `arg_scope`. `result_scope` defaults to `AnyScope()`.

```julia
h1 = Dagger.@spawn compute_scope=Dagger.scope((worker=1, thread=2), (worker=3, thread=1)) g(arg, 11)
h2 = Dagger.@spawn scope=Dagger.scope(worker=2,thread=3) compute_scope=Dagger.scope((worker=1, thread=2), (worker=3, thread=1)) g(arg, 21)
```
Tasks `h1` and `h2` execute on any worker within the intersection of the `compute_scope` and `arg_scope`. `scope` is ignored if `compute_scope` is specified. The result is stored and accessible from anywhere.

---

### Result scope and Chunk argument scopes interaction
If only `result_scope` is specified, computation happens on any worker within `arg_scope`, and the result is only accessible from `result_scope`.

```julia
h = Dagger.@spawn result_scope=Dagger.scope(worker=3) g(arg, 11)
```
Task `h` executes on any worker within `arg_scope`. The result is accessible from `result_scope`.

---

### Compute, result, and chunk argument scopes interaction  
When `scope`, `compute_scope`, and `result_scope` are all used, the scheduler executes the task on the intersection of `arg_scope`, the effective compute scope (which is `compute_scope` if provided, otherwise `scope`), and `result_scope`. If no intersection exists, the scheduler throws an exception.

```julia
h = Dagger.@spawn scope=Dagger.scope(worker=3,thread=2) compute_scope=Dagger.ProcessScope(2) result_scope=Dagger.scope((worker=2, thread=2), (worker=4, thread=2)) g(arg, 31)
```
Task `h` computes on thread 2 of worker 2 (as it's the intersection of `arg`, `compute`, and `result` scopes), and its result access is also restricted to thread 2 of worker 2.
