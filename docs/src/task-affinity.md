# Task Affinity 

Dagger.jl's `@spawn` macro allows precise control over task execution and result access using `scope`, `compute_scope`, and `result_scope`.

---

## Key Terms

### Scope  
`scope` defines the general set of locations where a Dagger task can execute. If `compute_scope` and `result_scope` are not explicitly set, the task's `compute_scope` defaults to its `scope`, and its `result_scope` defaults to `AnyScope()`, meaning the result can be accessed by any processor. Execution occurs on any processor within the defined scope.

**Example:**
```julia
g = Dagger.@spawn scope=ExactScope(Dagger.OSProc(3)) f(x,y)
```
Task `g` executes only on Processor 3. Its result can be accessed by any processor.

---

### Compute Scope  
`compute_scope` also specifies where a Dagger task can execute. The key difference is if both `compute_scope` and `scope` are provided, `compute_scope` takes precedence over `scope` for execution placement. If `result_scope` isn't specified, it defaults to `AnyScope()`, allowing the result to be accessed by any processor.

**Example:**
```julia
g1 = Dagger.@spawn scope=ExactScope(Dagger.ThreadProc(2, 3)) compute_scope=Dagger.UnionScope(ExactScope(Dagger.ThreadProc(1, 2)), ExactScope(Dagger.ThreadProc(3, 1))) f(x,y)
g2 = Dagger.@spawn compute_scope=Dagger.UnionScope(ExactScope(Dagger.ThreadProc(1, 2)), ExactScope(Dagger.ThreadProc(3, 1))) f(x,y)
```
Task `g1` and `g2` execute on either thread 2 of processor 1, or thread 1 of processor 3. Their result can be accessed by any processor.

---

### Result Scope  

`result_scope` restricts where a task's result can be fetched or moved. This is crucial for managing data locality and minimizing transfers. If only `result_scope` is specified, the `compute_scope` defaults to `Dagger.DefaultScope()`, meaning computation may happen on any processor.

**Example:**
```julia
g = Dagger.@spawn result_scope=ExactScope(Dagger.OSProc(3)) f(x,y)
```
The result of `g` is accessible only from worker process 3. The task's execution may happen anywhere.

---

## Interaction of compute_scope and result_scope

When `scope`, `compute_scope`, and `result_scope` are all used, the scheduler executes the task on the intersection of the effective compute scope (which will be `compute_scope` if provided, otherwise `scope`) and the `result_scope`. If intersection does not exist then Scheduler throws Exception error.

**Example:**
```julia
g = Dagger.@spawn scope=ExactScope(Dagger.ThreadProc(3, 2)) compute_scope=Dagger.ProcessScope(2) result_scope=Dagger.UnionScope(ExactScope(Dagger.ThreadProc(2, 2)), ExactScope(Dagger.ThreadProc(4, 2))) f(x,y)
```
The task `g` computes on `Dagger.ThreadProc(2, 2)` (as it's the intersection of compute and result scopes), and its result access is also restricted to `Dagger.ThreadProc(2, 2)`.

---

## Chunk Inputs to Tasks  

This section explains how `scope`, `compute_scope`, and `result_scope` affect tasks when a `Chunk` is the primary input to `@spawn` (e.g., `Dagger.tochunk(...)`).

Assume `g` is some function, e.g., `g(x, y) = x * 2 + y * 3` and . `chunk_proc` is the chunk's processor, and `chunk_scope` is its defined accessibility.

When `Dagger.tochunk(...)` is directly spawned:
- The task executes on `chunk_proc`.
- The result is accessible only within `chunk_scope`.
- This behavior occurs irrespective of the `scope`, `compute_scope`, and `result_scope` values provided in the `@spawn` macro.
- Dagger validates that there is an intersection between the effective `compute_scope` (derived from `@spawn`'s `compute_scope` or `scope`) and the `result_scope`. If no intersection exists, the Scheduler throws an exception.

**Usage:**
```julia
h1 = Dagger.@spawn scope=ExactScope(Dagger.OSProc(3)) Dagger.tochunk(g(10, 11), chunk_proc, chunk_scope)
h2 = Dagger.@spawn compute_scope=Dagger.UnionScope(ExactScope(Dagger.ThreadProc(1, 2)), ExactScope(Dagger.ThreadProc(3, 1))) Dagger.tochunk(g(20, 21), chunk_proc, chunk_scope)
h3 = Dagger.@spawn scope=ExactScope(Dagger.ThreadProc(2, 3)) compute_scope=Dagger.UnionScope(ExactScope(Dagger.ThreadProc(1, 2)), ExactScope(Dagger.ThreadProc(3, 1))) Dagger.tochunk(g(30, 31), chunk_proc, chunk_scope)
h4 = Dagger.@spawn result_scope=ExactScope(Dagger.OSProc(3)) Dagger.tochunk(g(40, 41), chunk_proc, chunk_scope)
h5 = Dagger.@spawn scope=ExactScope(Dagger.ThreadProc(3, 2)) compute_scope=Dagger.ProcessScope(2) result_scope=Dagger.UnionScope(ExactScope(Dagger.ThreadProc(2, 2)), ExactScope(Dagger.ThreadProc(4, 2))) Dagger.tochunk(g(50, 51), chunk_proc, chunk_scope)
```
In all these cases (`h1` through `h5`), the task gets executed on `chunk_proc`, and its result is accessible only within `chunk_scope`.

---

## Function with Chunked Arguments as Tasks  

This section details behavior when `scope`, `compute_scope`, and `result_scope` are used with tasks where a function is the input, and its arguments include `Chunks`.

Assume `g(x, y) = x * 2 + y * 3` is a function, and `arg = Dagger.tochunk(g(1, 2), arg_proc, arg_scope)` is a chunked argument, where `arg_proc` is the chunk's processor and `arg_scope` is its defined scope.

### Scope  
If `arg_scope` and `scope` do not intersect, the Scheduler throws an exception. Otherwise, `compute_scope` defaults to `scope`, and `result_scope` defaults to `AnyScope()`. Execution occurs on the intersection of `scope` and `arg_scope`.

```julia
h = Dagger.@spawn scope=ExactScope(Dagger.OSProc(3)) g(arg, 11)
```
Task `h` executes on any processor within the intersection of `scope` and `arg_scope`. The result is stored and accessible from anywhere.

---

### Compute Scope  
If `arg_scope` and `compute_scope` do not intersect, the Scheduler throws an exception. Otherwise, execution happens on the intersection of the effective compute scope (which will be `compute_scope` if provided, otherwise `scope`) and `arg_scope`. `result_scope` defaults to `AnyScope()`.

```julia
h1 = Dagger.@spawn compute_scope=Dagger.UnionScope(ExactScope(Dagger.ThreadProc(1, 2)), ExactScope(Dagger.ThreadProc(3, 1))) g(arg, 11)
h2 = Dagger.@spawn scope=ExactScope(Dagger.ThreadProc(2, 3)) compute_scope=Dagger.UnionScope(ExactScope(Dagger.ThreadProc(1, 2)), ExactScope(Dagger.ThreadProc(3, 1))) g(arg, 21)
```
Task `h1` and `h2` execute on any processor within the intersection of the `compute_scope` and `arg_scope`. `scope` is ignored if `compute_scope` is specified. The result is stored and accessible from anywhere.

---

### Result Scope  
If only `result_scope` is specified, computation happens on any processor within `arg_scope`, and the result is only accessible from `result_scope`.

```julia
h = Dagger.@spawn result_scope=ExactScope(Dagger.OSProc(3)) g(arg, 11)
```
Task `h` executes on any processor within `arg_scope`. The result is accessible from `result_scope`.

---

### Compute and Result Scope  
When `scope`, `compute_scope`, and `result_scope` are all used, the scheduler executes the task on the intersection of `arg_scope`, the effective compute scope (which is `compute_scope` if provided, otherwise `scope`), and `result_scope`. If no intersection exists, the Scheduler throws an exception.

```julia
h = Dagger.@spawn scope=ExactScope(Dagger.ThreadProc(3, 2)) compute_scope=Dagger.ProcessScope(2) result_scope=Dagger.UnionScope(ExactScope(Dagger.ThreadProc(2, 2)), ExactScope(Dagger.ThreadProc(4, 2))) g(arg, 31)
```
Task `h` computes on `Dagger.ThreadProc(2, 2)` (as it's the intersection of `arg`, `compute`, and `result` scopes), and its result access is also restricted to `Dagger.ThreadProc(2, 2)`.
