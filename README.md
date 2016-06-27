# Dagger

**A framework for out-of-core and parallel computation**.

[![Build Status](https://travis-ci.org/JuliaParallel/Dagger.jl.svg?branch=master)](https://travis-ci.org/shashi/Dagger.jl) [![Coverage Status](https://coveralls.io/repos/github/JuliaParallel/Dagger.jl/badge.svg?branch=master)](https://coveralls.io/github/JuliaParallel/Dagger.jl?branch=master)

Dagger allows you to represent huge amounts of data as smaller pieces and compute on the pieces in parallel. The API mimicks Julia's standard library, with a few simple differences, so that it is easy to use. Computation with Dagger uses a scheduler similar to that in [Dask](http://dask.pydata.org/en/latest/). This scheduler tries to minimize the amount of memory allocated at any given time while maximizing CPU utilization. Dagger provides distributed arrays and sparse matrices and operations on them out-of-the-box. Distributed variations of various collection types such as arrays, sparse matrices, NDSparse datastructures or dictionaries can be expressed using the framework.

## Tutorial introduction

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
a = rand(BlockPartition(4000, 4000), 10^4, 10^4)
# => Dagger.AllocateArray(...)
```

Notice the first argument `BlockPartition(4000,4000)`. This tells Dagger to create the matrix with block partitioning where each partition is a `4000x4000` matrix. `4000x4000` floating point numbers takes up 128MB of RAM, this is a good chunk size to deal with - many such chunks can fit in a typical RAM, and files of size 128MB can be written to and read from disk with lesser overhead than smaller chunks. It is the onus of the user of Dagger to chose a suitable partitioning for the data.

In the above example the object `a` *represents* the random matrix . The actual data has not been created yet. If you call `compute(a)`, the data will be created.

```julia
b = compute(a)
# => Computed(10000x10000 Array{Float64,2} in 9 parts each of (max size) 4000x4000)
```

The result is an object containing metadata about the various pieces. They may be created on different workers and will stay there until a another worker needs it. However, you can request and get the whole data with the `gather` function.

```julia
gather(b)
# => 10000x10000 Array{Float64,2}:
....
```

Okay, now that we have learned how to create a matrix of random numbers, let's do some computation on them!

In this example, we will compute the sum of square of a 10000x10000 normally distributed random matrix.

```julia
x = randn(BlockPartition(4000,4000), 10^4, 10^4)
sum(x.^2)
# => 1.0000084097623596e8
```
the answer is close to 10^8. Note the use of `compute` on `sum`. This is because `sum` returns an object representing the computation of the sum. You need to call compute on it to actually compute it.

For the full array API supported by Dagger, see below.

### Saving and loading data

Sometimes the result of a computation might be too big to fit in memory. In such cases you will need to save the data to disk. You can do this by calling `save` on the computation along with the destination file name.

```julia
x = randn(BlockPartition(4000,4000), 10^5, 10^4)
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

## Distributing data

Dagger also allows one to distribute an object from the master process using a certain partition type. In the following example, we are distributing an array of size 1000x1000 with a block partition of 100x100 elements per block.

```julia

# distribute a matrix and then do some computation
a = rand(1000, 1000)

b = Distribute(BlockPartition(100, 100), a) # Create chunks of 100x100 submatrices
c = map(x->x^2, b) # map applies a function element wise. this is equivalent to b.^2
d = sum(c)
```

Dagger starts by cutting up the 1000x1000 array into 100x100 pieces and each process takes up the task of squaring and summing one of these pieces.

### Loading CSV Data

You can load data from a big CSV file in parallel and write it in to disk in two simple calls.

```julia
x = dist_readdlm("bigdata.csv", ',', 100, cleanup)
processed_x = compute(save(x, "bigbinarydata"))
```

100 approximately equal chunks from `bigdata.csv` will be read by the available processors in parallel, passed to Julia's `readdlm` function, and then to the `cleanup` function you provided. Note that `cleanup` must return a matrix. The distribution of the data will be block partitioned, the width of the block will span all columns. For example, if you have 10^6 rows in the CSV files and 20 columns, the each block will contain approximately `10^4x20` elements. We say approximately because `dist_readdlm` divides the input CSV by its size rather than number of lines, each chunk is delimited at the nearest newline.

Once you've loaded the data in this manner, it's a perfect time to slice and dice it and run some statistics on it. Try `processed_x'processed_x` for example, since the number of columns is less, the result of this computation is also a small matrix. E.g. the aforementioned example dataset would yeild a 20x20 matrix as a result.

### Array support

We have seen simple operations like broadcast (namely `.^2`), `reduce` and `sum` on distributed arrays so far. Dagger also supports other essential array operations such as transpose, matrix-matrix multiplication and matrix-vector multiplication.

See the [Array API](#array-api) below for details. Some special features are discussed below.

#### Indexing

You can index into any Computation using the usual indexing syntax. Some examples of valid indexing syntax are:

```julia
X[20:100, 20:100]
X[[20,30,40], [30,40,60]]
X[[20,30,40], :]
X[20:40, [30,40,60]]
```

Note that indexing again results in a `Computation` object of the type `GetIndex`. You can use it as input to another computation or call `gather` on it to get the indexed sub array.

#### Sparse matrix support

Array support is made up of pretty generic code, hence SparseMatrixCSC work wherever dense arrays are applicable and wherever Julia defines the required generic functions on them.

To create a random sparse matrix or vector use the `sprand` function.

```julia
s1 = sprand(BlockPartition(4000,4000), 10^4, 10^4, 0.01)
# => AllocateArray(...)

compute(s1)
# => Computed(10000x10000 SparseMatrixCSC{Float64,Int64} in 3x3 parts each of (max size) 4000x4000)

s2 = sprand(BlockPartition(4000,), 10^4, 0.01)
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

julia> gather(x)
10000x10000 sparse matrix with 999793 Float64 entries:
    ...
```

#### A note on keeping memory use in check

Dagger currently does not write results of computations to disk unless you specifically ask it to.

Some operations which are computationally intensive might require you to save the input data as well as output data to disk.

As an example let us take the operation `A+A'`. Here, various chunks of `A` need to be kept in memory so that they can be added to the corresponding chunk of `A'`. But the total memory taken up by these chunks might be too large for your RAM. In such cases you will need to save the array `A` to disk first and then save the result of `A+A'` to disk as well.

```julia

A = rand(BlockPartition(4000, 4000), 30000, 30000) # 7.2GB of data
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

*Note: all these operations result in a `Computation` object. You need to call `compute` or `gather` on them to actually do the computation.*

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

**Compute and gather**

- `compute(ctx, c)` - compute a computation `c`
- `gather(ctx, c)` - compute the result, and collate the result on the host process (usually pid 1).

**Context**

- `Context([pids=workers()])` - context which uses the processes specified in the pids

**Upcoming features**
- Sorting

## Debugging performance

To debug the performance of a computation you can use the `debug_compute` substitute for the `compute` function. `debug_compute` returns a vector filled with performance metadata along with the result of the computation.

For example:

```julia
x = rand(BlockPartition(400, 4000), 1200, 12000)
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
