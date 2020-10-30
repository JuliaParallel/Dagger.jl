# A framework for out-of-core and parallel execution

## Usage

The main function for using Dagger is `delayed`

`delayed(f; options...)`

It returns a function which when called creates a `Thunk` object representing a call to function `f` with the given arguments. If it is called with other thunks as input, then they form a graph with input nodes directed at the output. The function `f` gets the result of the input Thunks.
Thunks don't pass keyword argument to the function `f`. Options kwargs... to `delayed` are passed to the scheduler to control its behavior:
- `get_result::Bool` -- return the actual result to the scheduler instead of `Chunk` objects. Used when `f` explicitly constructs a Chunk or when return value is small (e.g. in case of reduce)
- `meta::Bool` -- pass the input “Chunk” objects themselves to `f` and not the value contained in them - this is always run on the master process
- `persist::Bool` -- the result of this Thunk should not be released after it becomes unused in the DAG
- `cache::Bool` -- cache the result of this Thunk such that if the thunk is evaluated again, one can just reuse the cached value. If it’s been removed from cache, recompute the value.

# DAG creation interface
Here is a very simple example DAG:

```julia
using Dagger

add1(value) = value + 1
add2(value) = value + 2
combine(a...) = sum(a)

p = delayed(add1)(4)
q = delayed(add2)(p)
r = delayed(add1)(3)
s = delayed(combine)(p, q, r)

@assert collect(s) == 16
```

The above computation can also be written in a more Julia-idiomatic syntax with `@par`:

```julia
p = @par add1(4)
q = @par add2(p)
r = @par add1(3)
s = @par combine(p, q, r)

@assert collect(s) == 16
```

or similarly:

```julia
s = @par begin
    p = add1(4)
    q = add2(p)
    r = add1(3)
    combine(p, q, r)
end

@assert collect(s) == 16
```

The connections between nodes `p`, `q`, `r` and `s` is represented by this dependency graph:

![graph](https://user-images.githubusercontent.com/25916/26920104-7b9b5fa4-4c55-11e7-97fb-fe5b9e73cae6.png)


The final result is the obvious consequence of the operation

 `add1(4)` + `add2(add1(4))` + `add1(3)`

 `(4 + 1)` + `((4 + 1) + 2)` + `(3 + 1)` = 16

To compute and fetch the result of a thunk (say `s`), you can call `collect(s)`. `collect` will fetch the result of the computation to the master process. Alternatively, if you want to compute but not fetch the result you can call `compute` on the thunk. This will return a `Chunk` object which references the result. If you pass in a `Chunk` objects as an input to a delayed function, then the function will get executed with the value of the `Chunk` -- this evaluation will likely happen where the input chunks are, to reduce communication.

The key point is that, for each argument to a node, if the argument is a `Thunk`, it'll be executed before this node and its result will be passed into the function `f` provided.
If the argument is *not* a `Thunk` (just some regular Julia object), it'll be passed as-is to the function `f`.

### Polytree

[Polytrees](https://en.wikipedia.org/wiki/Polytree) are easily supported by Dagger. To make this work, pass all the head nodes `Thunk`s into a call to `delayed` as arguments, which will act as the top node for the graph.
```julia
group(x...) = [x...]
top_node = delayed(group)(head_nodes...)
compute(top_node)
```

## Scheduler and Thunk options

While Dagger generally "just works", sometimes one needs to exert some more
fine-grained control over how the scheduler allocates work. There are two
parallel mechanisms to achieve this: Scheduler options (from
`Dagger.Sch.SchedulerOptions`) and Thunk options (from
`Dagger.Sch.ThunkOptions`). These two options structs generally contain the
same options, with the difference being that Scheduler options operate
globally across an entire DAG, and Thunk options operate on a thunk-by-thunk
basis. Scheduler options can be constructed and passed to `collect()` or
`compute()` as the keyword argument `options`, and Thunk options can be passed
to Dagger's `delayed` function similarly: `delayed(myfunc)(arg1, arg2, ...;
options=opts)`. Check the docstring for the two options structs to see what
options are available.

## Processors and Resource control

By default, Dagger uses the CPU to process work, typically single-threaded per
cluster node. However, Dagger allows access to a wider range of hardware and
software acceleration techniques, such as multithreading and GPUs. These more
advanced (but performant) accelerators are disabled by default, but can easily
be enabled by using Scheduler/Thunk options in the `proctypes` field. If
non-empty, only the processor types contained in `options.proctypes` will be
used to compute all or a given thunk.

### GPU Processors

The [DaggerGPU.jl](https://github.com/JuliaGPU/DaggerGPU.jl) package can be
imported to enable GPU acceleration for NVIDIA and AMD GPUs, when available.
The processors provided by that package are not enabled by default, but may be
enabled via `options.proctypes` as usual.

## Rough high level description of scheduling

- First picks the leaf Thunks and distributes them to available workers. Each worker is given at most 1 task at a time. If input to the node is a `Chunk`, then workers which already have the chunk are preferred.
- When a worker finishes a thunk it will return a `Chunk` object to the scheduler.
- Once the worker has returned a `Chunk`, the scheduler picks the next task for the worker -- this is usually the task the worker immediately made available (if possible). In the small example above, if worker 2 finished `p` it will be given `q` since it will already have the result of `p` which is input to `q`.
- The scheduler also issues "release" Commands to chunks that are no longer required by nodes in the DAG: for example, when `s` is computed all of `p`, `q`, `r` are released to free up memory. This can be prevented by passing `persist` or `cache` options to `delayed`.

## Modeling of Dagger programs

The key API for parallel and heterogeneous execution is `Dagger.delayed`.
The call signature of `Dagger.delayed` is the following:

```julia
thunk = Dagger.delayed(func)(args...)
```

This invocation serves to construct a single node in a computational graph.
`func` is a Julia function, which normally takes some number of arguments, of
length `N` and of types `Targs`. The set of arguments `args...` is specified
with ellipses to indicate that many arguments may be passed between the
parentheses. When correctly invoked, `args...` is of length `N` and of types
`Targs` (or suitable subtypes of `Targs`, for each respective argument in
`args...`).  `thunk` is an instance of a Dagger `Thunk`, which is the value
used internally by Dagger to represent a node in the graph.

A `Thunk` may be "computed":

```julia
chunk = Dagger.compute(thunk)
```

Computing a `Thunk` performs roughly the same logic as the following Julia
function invocation:

```julia
result = func(args...)
```

Such an invocation invokes `func` on `args...`, returning `result`. Computing
the above thunk would produce a value with the same type as `result`, with the
caveat that the result will be wrapped by a `Dagger.Chunk` (`chunk` in the
above example). A `Chunk` is a reference to a value stored on a compute
process within the `Distributed` cluster that Dagger is operating within. A
`Chunk` may be "collected", which will return the wrapped value to the
collecting process, which in the above example will be `result`:

```julia
result = collect(chunk)
```

In order to create a graph with more than a single node, arguments to
`delayed` may themselves be `Thunk`s or `Chunk`s. For example, the sum of the
elements of vector `[1,2,3,4]` may be represented in Dagger as follows:

```julia
thunk1 = Dagger.delayed(+)(1, 2)
thunk2 = Dagger.delayed(+)(3, 4)
thunk3 = Dagger.delayed(+)(thunk1, thunk2)
```

A graph has now been constructed, where `thunk1` and `thunk2` are dependencies
("inputs") to `thunk3`. Computing `thunk3` and then collecting its resulting
`Chunk` would result in the answer that is expected from the operation:

```julia
chunk = compute(thunk3)
result = collect(chunk)
```

```julia-repl
julia> result == 10
true
```

`result` now has the `Int64` value `10`, which is the result of summing the
elements of the vector `[1,2,3,4]`. For convenience, computation may be
performed together with collection, like so:

```julia
result = collect(thunk3)
```

The above summation example is equivalent to the following invocation in plain
Julia:

```julia
x1 = 1 + 2
x2 = 3 + 4
result = x1 + x2
result == 10
```

However, there are key differences when using Dagger to perform this operation
as compared to performing this operation without Dagger. In Dagger, the graph
is constructed separately from computing the graph ("lazily"), whereas without
Dagger the graph is executed immediately ("eagerly"). Dagger makes use of this
lazy construction approach to allow modifying the actual execution of the
overall operation in useful ways.

By default, computing a Dagger graph creates an instance of a scheduler, which
will be provided the graph to execute. The scheduler executes the individual
nodes of the graph on their arguments in the order specified by the graph
(ensuring dependencies to a node are satisfied before executing said node) on
compute processes in the cluster; the scheduler process itself typically does
not execute the nodes directly. Additionally, if a given set of nodes do not
depend on each other (the value generated by a node is not an input to another
node in the set), then those nodes may be executed in parallel, and the
scheduler attempts to schedule such nodes in parallel when possible.

The scheduler also orchestrates data movement between compute processes, such
that inputs to a given node are available on the compute process that is
scheduled to execute said node. The scheduler attempts to minimize data
movement between compute processes; it does so by trying to schedule nodes
which depend on a given input on the same compute process that computed and
retains that input.
