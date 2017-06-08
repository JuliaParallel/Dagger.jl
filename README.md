# Dagger

**A framework for out-of-core and parallel computing**.

[![Build Status](https://travis-ci.org/JuliaParallel/Dagger.jl.svg?branch=master)](https://travis-ci.org/JuliaParallel/Dagger.jl) [![Coverage Status](https://coveralls.io/repos/github/JuliaParallel/Dagger.jl/badge.svg?branch=master)](https://coveralls.io/github/JuliaParallel/Dagger.jl?branch=master)

At the core of Dagger.jl is a scheduler heavily inspired by [Dask](http://dask.pydata.org/en/latest/). It can run computations represented as directed-acyclic-graphs (DAGs) efficiently on many Julia worker processes.

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

# Higher level interfaces

Building on this DAG interface, this package also provides a distributed array library. Another notable user of the DAG framework to provide a high-level distributed-table-like interface is [JuliaDB](http://juliadb.org/).

Below is a discussion of the Array interface provided by this package.

## Array interface

To begin, let us add a few worker processes and load Dagger.

```julia

addprocs() # julia will decide how many workers to add.

using Dagger
```
Note that you should run `using Dagger` *after* adding
worker processes for julia to load the package on all of them.

### Playing with random matrices

A good place to start learning to work with Dagger is to play with some distributed random data.
In this example we will create a matrix of size `10000x10000` which is cut up into pieces of `4000x4000` elements.

```julia
a = rand(Blocks(4000, 4000), 10^4, 10^4)
# => Dagger.AllocateArray(...)
```

Notice the first argument `Blocks(4000,4000)`. This tells Dagger to create the matrix with block partitioning where each partition is a `4000x4000` matrix. `4000x4000` floating point numbers takes up 128MB of RAM, this is a good chunk size to deal with - many such chunks can fit in a typical RAM, and files of size 128MB can be written to and read from disk with lesser overhead than smaller chunks. It is the onus of the user of Dagger to chose a suitable partitioning for the data.

In the above example the object `a` *represents* the random matrix . The actual data has not been created yet. If you call `compute(a)`, the data will be created.

```julia
b = compute(a)
# => Computed(10000x10000 Array{Float64,2} in 9 parts each of (max size) 4000x4000)
```

The result is an object containing metadata about the various pieces. They may be created on different workers and will stay there until a another worker needs it. However, you can request and get the whole data with the `collect` function.

```julia
collect(b)
# => 10000x10000 Array{Float64,2}:
....
```

Okay, now that we have learned how to create a matrix of random numbers, let's do some computation on them!

In this example, we will compute the sum of square of a 10000x10000 normally distributed random matrix.

```julia
x = randn(Blocks(4000,4000), 10^4, 10^4)
sum(x.^2)
# => 1.0000084097623596e8
```
the answer is close to 10^8. Note the use of `compute` on `sum`. This is because `sum` returns an object representing the computation of the sum. You need to call compute on it to actually compute it.

For the full array API supported by Dagger, see below.

### Saving and loading data

Sometimes the result of a computation might be too big to fit in memory. In such cases you will need to save the data to disk. You can do this by calling `save` on the computation along with the destination file name.

```julia
x = randn(Blocks(4000,4000), 10^5, 10^4)
compute(save(x.^2, "X_sq"))
```

Note that the metadata about the various pieces will be stored in the file `X_sq` while the data itself is stored in a directory called `X_sq_data`. Be warned that the directory will be created if it doesn't exist, and any existing files will be overwritten.

Once you have saved some data to disk, you can load it with the `load` function.

```julia
X_sq = load(Context(), "X_sq")
```

Notice the first argument `Context()` this required so that this `load` method is kept special to Dagger. There will be discussion about what `Context()` returns later on in the documentation.

Now you can do some computation on `X_sq` and the data will be read from disk when different processes need it.

```julia
sum(X_sq)
# => 1.0000065091623393e9
```

### Distributing data

Dagger also allows one to distribute an object from the master process using a certain partition type. In the following example, we are distributing an array of size 1000x1000 with a block partition of 100x100 elements per block.

```julia

# distribute a matrix and then do some computation
a = rand(1000, 1000)

b = Distribute(Blocks(100, 100), a) # Create chunks of 100x100 submatrices
c = map(x->x^2, b) # map applies a function element wise. this is equivalent to b.^2
d = sum(c)
```

Dagger starts by cutting up the 1000x1000 array into 100x100 pieces and each process takes up the task of squaring and summing one of these pieces.

#### Indexing

You can index into any Computation using the usual indexing syntax. Some examples of valid indexing syntax are:

```julia
X[20:100, 20:100]
X[[20,30,40], [30,40,60]]
X[[20,30,40], :]
X[20:40, [30,40,60]]
```

Note that indexing again results in a `Computation` object of the type `GetIndex`. You can use it as input to another computation or call `collect` on it to get the indexed sub array.

#### Sparse matrix support

Array support is made up of pretty generic code, hence SparseMatrixCSC work wherever dense arrays are applicable and wherever Julia defines the required generic functions on them.

To create a random sparse matrix or vector use the `sprand` function.

```julia
s1 = sprand(Blocks(4000,4000), 10^4, 10^4, 0.01)
# => AllocateArray(...)

compute(s1)
# => Computed(10000x10000 SparseMatrixCSC{Float64,Int64} in 3x3 parts each of (max size) 4000x4000)

s2 = sprand(Blocks(4000,), 10^4, 0.01)
# => AllocateArray(...)

julia> compute(s2)
Computed(10000 SparseVector{Float64,Int64} in 3 parts each of (max size) 4000)

julia> x = compute(s1*s2) # sparse matrix-vector multiply
Computed(10000 SparseVector{Float64,Int64} in 3 parts each of (max size) 4000)
```

You can also save a partitioned sparse matrix to disk and load it back.

```
julia> compute(save(s1, "s1"))

julia> x = load(Context(), "s1")
Computed(10000x10000 Array{Float64,2} in 3x3 parts each of (max size) 4000x4000)

julia> collect(x)
10000x10000 sparse matrix with 999793 Float64 entries:
    ...
```

#### A note on keeping memory use in check

Dagger currently does not write results of computations to disk unless you specifically ask it to.

Some operations which are computationally intensive might require you to save the input data as well as output data to disk.

As an example let us take the operation `A+A'`. Here, various chunks of `A` need to be kept in memory so that they can be added to the corresponding chunk of `A'`. But the total memory taken up by these chunks might be too large for your RAM. In such cases you will need to save the array `A` to disk first and then save the result of `A+A'` to disk as well.

```julia

A = rand(Blocks(4000, 4000), 30000, 30000) # 7.2GB of data
saved_A = compute(save(A, "A"))

result = compute(save(saved_A+saved_A', "ApAt"))
```

## API

**Creating arrays**

- `Distribute(partition_scheme, data)` - distribute `data` according to `partition_scheme`.
- `rand(partition_scheme, [type], [dimensions...])` - create a random array with the specified partition scheme
- `randn(partition_scheme, [dimensions...])` - create a normally distributed random array with the specified partition scheme
- `ones(partition_scheme, [type], [dimensions...])` - create an array of all ones
- `zeros(partition_scheme, [type], [dimensions...])` - create an array of all zeros

**Sparse arrays**
- `sprand(partition_scheme, m, [n], sparsity)` - create sparse matrix or vector with the given partition scheme

**Map-reduce**

- `map(f, c)` - apply f to each element of the distributed object
- `reduce(f, c)` - reduce `c` with a 2-arg associative function `f`.
- `reduceblock(f, c)` - reduce each block of data by applying `f` to the block. In block distributed array, the result has the same dimensionality as the input.
- `reducebykey(f, c)` - given a collection of tuples or pairs, use the first element of the tuples as the key, and reduce the values of each key. Computes a Dict of results.

*Note: all these operations result in a `Computation` object. You need to call `compute` or `collect` on them to actually do the computation.*

**Array API**
- Unary element-wise operations:

```
exp, expm1, log, log10, log1p, sqrt, cbrt, exponent,
significand, (-), sin, sinpi, cos, cospi, tan, sec,
cot, csc, sinh, cosh, tanh, coth, sech, csch, asin,
acos, atan, acot, asec, acsc, asinh, acosh, atanh,
acoth, asech, acsch, sinc, cosc
```

- Binary element-wise operations:

```
+, -, %, (.*), (.+), (.-), (.%), (./), (.^),
$, &, (.!=), (.<), (.<=), (.==), (.>),
(.>=), (.\), (.//), (.>>), (.<<)
```

- `*` on Computations can either stand for matrix-matrix or matrix-vector multiplications.
- transpose on a matrix can be done using the `x'` syntax

**Compute and collect**

- `compute(ctx, c)` - compute a computation `c`
- `collect(ctx, c)` - compute the result, and collate the result on the host process (usually pid 1).

**Context**

- `Context([pids=workers()])` - context which uses the processes specified in the pids

**Upcoming features**
- Sorting

## Debugging performance

To debug the performance of a computation you can use the `debug_compute` substitute for the `compute` function. `debug_compute` returns a vector filled with performance metadata along with the result of the computation.

For example:

```julia
x = rand(Blocks(400, 4000), 1200, 12000)
debug_compute(x*x); # first run to compile stuff (maybe run on a smaller problem)
dbg, result = debug_compute(x*x')
summarize_events(dbg)
# => 3.132245 seconds (83.25 k allocations: 467.796 MB, 2.87% gc time)
# (note that this time might be larger than the running time of the compute
#  this is because time readings from many processes are added up.)
```

The `dbg` object is just an array of `Timespan`s. A `Timespan` contains the following fields:

- `category`        : a symbol. One of `:comm` (communication), `:compute`, :scheduler`, `:scheduler_init` - denotes the type of work being done in the timespan
- `id`              : An `id` for the timespan. This is also the id of the `Thunk` object being dealt with. (category, id) pair identify a timespan uniquely.
- `timeline`        : An `OSProc` (or any `Processor`) object representing the processor where the timespan was recorded.
- `start`           : `time_ns()` when the timespan started
- `finish`          : `time_ns()` when the timespan finished
- `gc_diff`         : a Base.GC_Diff object containing the GC state change in the timespan (see `?Base.GC_Diff`)
- `profiler_samples` : if run with `profile=true`, contains profiler samples collected in the timespan.

Since `dbg` is just an array, you can filter events of particular interest to you and summarize them.

```julia
# how much time and space did the computation take on process 2?
summarize_events(filter(x->  x.timeline == OSProc(2), dbg))
# => 1.658207 seconds (16.11 k allocations: 269.361 MB, 2.89% gc time)

# how much time was spent communicating by process 3?

summarize_events(filter(x->  x.timeline == OSProc(3) && x.category==:comm, d))
  0.606891 seconds (15.69 k allocations: 149.712 MB, 6.26% gc time)

# what was the scheduler overhead?
summarize_events(filter(x->x.category==:scheduler, d))
  0.254783 seconds (47.67 k allocations: 3.314 MB)
```

If you run `debug_compute` with profile keyword argument set to true, profile samples during each timespan are collected. `summarize_events` will also pretty print the profiler data for you.

For example,
```julia
summarize_events(filter(x->x.category==:scheduler, dbg))
# =>  0.146829 seconds (44.60 k allocations: 3.103 MB)
2   ./stream.jl; process_events; line: 731
154 REPL.jl; anonymous; line: 92
 126 REPL.jl; eval_user_input; line: 62
  59 util.jl; debug_compute; line: 155
   59 ...e/shashi/.julia/v0.4/Dagger/src/compute.jl; compute; line: 203
    16 ...e/shashi/.julia/v0.4/Dagger/src/compute.jl; finish_task!; line: 144
    43 ...e/shashi/.julia/v0.4/Dagger/src/compute.jl; finish_task!; line: 152
     43 .../shashi/.julia/v0.4/Dagger/src/compute.jl; release!; line: 218
      43 ...shi/.julia/v0.4/Dagger/src/lib/dumbref.jl; release_token; line: 24
      ...
```
## Design

The goal of Dagger is to create sufficient scope for multiple-dispatch to be employed at various stages of a parallel computation. New capabilities, distributions, device types can be added by defining new methods on a very small set of generic functions. The DAG also allows for other optimizations (fusing maps and reduces), fault-tolerance and visualization.


### Acknowledgements

We thank DARPA, Intel, and the NIH for supporting this work at MIT.
