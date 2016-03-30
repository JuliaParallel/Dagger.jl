# ComputeFramework

**A framework for out-of-core and distributed computation**.

ComputeFramework allows you to represent huge amounts of data as smaller pieces and compute on the pieces in parallel. The API mimicks Julia's standard library, with a few simple differences, so that it is easy to use. Computation with ComputeFramework uses a scheduler similar to that in [Dask](http://dask.pydata.org/en/latest/). This scheduler tries to minimize the amount of memory allocated at any given time while maximizing CPU utilization. ComputeFramework provides distributed arrays and sparse matrices and operations on them out-of-the-box. Distributed variations of various collection types such as arrays, sparse matrices, NDSparse datastructures or dictionaries can be expressed using the framework.

## Tutorial introduction

To begin, let us add a few worker processes and load ComputeFramework.

```julia

addprocs() # julia will decide how many workers to add.

using ComputeFramework
```
Note that you should run `using ComputeFramework` *after* adding
worker processes for julia to load the package on all of them.

### Playing with random matrices

A good place to start learning to work with ComputeFramework is to play with some distributed random data.
In this example we will create a matrix of size `10000x10000` which is cut up into pieces of `4000x4000` elements. 

```julia
a = rand(BlockPartition(4000, 4000), 10^4, 10^4)
# => ComputeFramework.AllocateArray(...)
```

Notice the first argument `BlockPartition(4000,4000)`. This tells ComputeFramework to create the matrix with block partitioning where each partition is a `4000x4000` matrix. `4000x4000` floating point numbers takes up 128MB of RAM, this is a good chunk size to deal with - many such chunks can fit in a typical RAM, and files of size 128MB can be written to and read from disk with lesser overhead than smaller chunks. It is the onus of the user of ComputeFramework to chose a suitable partitioning for the data.

In the above example the object `a` *represents* the random matrix . The actual data has not been created yet. If you call `compute(a)`, the data will be created.

```julia
b = compute(a)
#=> Computed(10000x10000 Array{Float64,2} in 9 parts each of (max size) 4000x4000)
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
compute(sum(x.^2))
#=> 1.0000084097623596e8
```
the answer is close to 10^8. Note the use of `compute` on `sum`. This is because `sum` returns an object representing the computation of the sum. You need to call compute on it to actually compute it.

For the full array API supported by ComputeFramework, see below.

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

Notice the first argument `Context()` this required so that this `load` method is kept special to ComputeFramework. There will be discussion about what `Context()` returns later on in the documentation.

Now you can do some computation on `X_sq` and the data will be read from disk when different processes need it.

```julia
compute(sum(X_sq))
#=> 1.0000065091623393e9
```

## Distributing data

ComputeFramework also allows one to distribute an object from the master process using a certain partition type. In the following example, we are distributing an array of size 1000x1000 with a block partition of 100x100 elements per block.

```julia

# distribute a matrix and then do some computation
a = rand(1000, 1000)

b = Distribute(BlockPartition(100, 100), a) # Create chunks of 100x100 submatrices
c = map(x->x^2, b) # map applies a function element wise. this is equivalent to b.^2
d = sum(c)

compute(d)
```

When `compute` runs the computation, it starts by cutting up the 1000x1000 array into 100x100 pieces and each process takes up the task of squaring and summing one of these pieces.

### Loading CSV Data

You can load data from a big CSV file in parallel and write it in to disk in two simple calls.

```julia
x = dist_readdlm("bigdata.csv", ',', 100, cleanup)
processed_x = compute(save(x, "bigbinarydata"))
```

100 approximately equal chunks from `bigdata.csv` will be read by the available processors in parallel, passed to Julia's `readdlm` function, and then to the `cleanup` function you provided. Note that `cleanup` must return a matrix. The distribution of the data will be block partitioned, the width of the block will span all columns. For example, if you have 10^6 rows in the CSV files and 20 columns, the each block will contain approximately `10^4x20` elements. We say approximately because `dist_readdlm` divides the input CSV by its size rather than number of lines, each chunk is delimited at the nearest newline.

Once you've loaded the data in this manner, it's a perfect time to slice and dice it and run some statistics on it. Try `processed_x'processed_x` for example, since the number of columns is less, the result of this computation is also a small matrix. E.g. the aforementioned example dataset would yeild a 20x20 matrix as a result.

### Array support

We have seen simple operations like broadcast (namely `.^2`), `reduce` and `sum` on distributed arrays so far. ComputeFramework also supports other essential array operations.

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
#=> AllocateArray(...)

compute(s1)
#=> Computed(10000x10000 SparseMatrixCSC{Float64,Int64} in 3x3 parts each of (max size) 4000x4000)

s2 = sprand(BlockPartition(4000,), 10^4, 0.01)
#=> AllocateArray(...)

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

## API

**Creating distributed arrays**

- `Distribute(partition_scheme, data)` - distribute `data` according to `partition_scheme`.
- `rand(partition_scheme, [type], [dimensions...])` - create a random array with the specified partition scheme
- `randn(partition_scheme, [dimensions...])` - create a normally distributed random array with the specified partition scheme
- `ones(partition_scheme, [type], [dimensions...])` - create an array of all ones
- `zeros(partition_scheme, [type], [dimensions...])` - create an array of all zeros

**Map-reduce**

- `map(f, c)` - apply f to each element of the distributed object
- `reduce(f, c)` - reduce `c` with a 2-arg associative function `f`.
- `reduceblock(f, c)` - reduce each block of data by applying `f` to the block. In block distributed array, the result has the same dimensionality as the input.
- `reducebykey(f, c)` - given a collection of tuples or pairs, use the first element of the tuples as the key, and reduce the values of each key. Computes a Dict of results.

*Note: all these operations result in a `Computation` object. You need to call `compute` or `gather` on them to actually do the computation.*

**Compute and gather**

- `compute(ctx, c)` - compute a computation `c`
- `gather(ctx, c)` - compute the result, and collate the result on the host process (usually pid 1).

**Context**

- `Context([pids=workers()])` - context which uses the processes specified in the pids

**Reading from a file**

- `TextFile(f, [mode="r", chunksize=128M])` - creates a recipe for a text file to be read. The file is read in `chunksize` units in one go.
- `split(f, char)` - split the `TextFile` node `f` wherever `char` appears to create an array of strings.
- `readlines(f)` - same as `split(f, '\n')`

The file is read at different offsets by different processes each reading it at `chunksize` bytes at a time - that's how parallelizm is achieved.

**Upcoming features**
- Sorting

## Design

The goal of ComputeFramework is to create sufficient scope for multiple-dispatch to be employed at various stages of a parallel computation. New capabilities, distributions, device types can be added by defining new methods on a very small set of generic functions. The DAG also allows for other optimizations (fusing maps and reduces), fault-tolerance and visualization.

### Acknowledgements

We thank DARPA, Intel, and the NIH for supporting this work at MIT.
