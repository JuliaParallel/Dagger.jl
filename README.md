# Dagger

**A framework for out-of-core and parallel computing**.

[![Build Status](https://travis-ci.org/JuliaParallel/Dagger.jl.svg?branch=master)](https://travis-ci.org/JuliaParallel/Dagger.jl) [![Coverage Status](https://coveralls.io/repos/github/JuliaParallel/Dagger.jl/badge.svg?branch=master)](https://coveralls.io/github/JuliaParallel/Dagger.jl?branch=master)

At the core of Dagger.jl is a scheduler heavily inspired by [Dask](http://dask.pydata.org/en/latest/). It can run computations represented as [directed-acyclic-graphs](https://en.wikipedia.org/wiki/Directed_acyclic_graph) (DAGs) efficiently on many Julia worker processes.

# DAG creation interface

Here is an example DAG:

```julia
using Dagger

p = delayed(f; options...)(42)
q = delayed(g)(p)
r = delayed(h)(53)
s = delayed(combine)(p, q, r)
```
The connections between nodes `p`, `q`, `r` and `s` is represented by this dependency graph:

![graph](https://user-images.githubusercontent.com/25916/26920104-7b9b5fa4-4c55-11e7-97fb-fe5b9e73cae6.png)

`delayed(f; options...)`

Returns a function which when called creates a `Thunk` object representing a call to function `f` with the given arguments. If it is called with other thunks as input, then they form a graph with input nodes directed at the output. The function `f` get the result of evaluating the input thunks.

To compute and fetch the result of a thunk (say `s`), you can call `collect(s)`. `collect` will fetch the result of the computation to the master process. Alternatively, if you want to compute but not fetch the result you can call `compute` on the thunk. This will return a `Chunk` object which references the result. If you pass in a `Chunk` objects as an input to a delayed function, then the function will get executed with the value of the `Chunk` -- this evaluation will likely happen where the input chunks are, to reduce communication.
 
Options to `delayed` are:
- `get_result::Bool` -- return the actual result to the scheduler instead of `Chunk` objects. Used when `f` explicitly constructs a Chunk or when return value is small (e.g. in case of reduce)
- `meta::Bool` -- pass the input “Chunk” objects themselves to `f` and not the value contained in them - this is always run on the master process
- `persist::Bool` -- the result of this Thunk should not be released after it becomes unused in the DAG
- `cache::Bool` -- cache the result of this Thunk such that if the thunk is evaluated again, one can just reuse the cached value. If it’s been removed from cache, recompute the value.

## Rough high level description of scheduling

- First picks the leaf Thunks and distributes them to available workers. Each worker is given at most 1 task at a time. If input to the node is a Chunk, then workers which already have the chunk are preferred.
- When a worker finishes a thunk it will return a `Chunk` object to the scheduler.
- Once the worker has returned a Chunk, scheduler picks the next task for the worker -- this is usually the task the worker immediately made available (if possible). In the small example above, if worker 2 finished `p` it will be given `q` since it will already have the result of `p` which is input to `q`.
- The scheduler also issues "release" Commands to chunks that are no longer required by nodes in the DAG: for example, when s is computed all of p, q, r are released to free up memory. This can be prevented by passing `persist` or `cache` options to `delayed`.

### Acknowledgements

We thank DARPA, Intel, and the NIH for supporting this work at MIT.
