# ComputeFramework

**A framework for DAG-based computations**

## Usage

```julia
addprocs(4)

using ComputeFramework

ctx = Context()

a = rand(100, 100)

b = distribute(a) # Distribute the array column-wise. distribute(a, cutdim(1)) distributes by rows
c = map(x->x^2, b)
d = reduce(+, 0.0, c)

compute(Context(), d) # compute the sum of squares
```

## API

The first step in using ComputeFramework is to distribute some data.

**Distribution**

- `distribute(c, [layout=cutdim(last_dimension)])` - distribute an object `c` using a specified *layout*.

**Map-reduce**

- `mappart(f, c)` - apply f to each partition of the distributed object
- `map(f, c)` - apply f to each element of the distributed object
- `filter(f, c)` - filter based on a predicate `f`
- `reduce(f, v0, c)` - reduce `c` with a 2-arg associative function `f` and an initial / zero value v0.
- `reducebykey(f, v0, c)` - given a collection of tuples or pairs, use the first element of the tuples as the key, and reduce the values of each key. Computes a Dict of results.

*Note: all these operations result in an `AbstractNode` object. You need to call `compute` or `gather` on them to actually do the computation.*

**Compute and gather**

- `compute(ctx, c)` - compute a computation represented by a node c
- `gather(ctx, c)` - compute the result, and collate the result on the host process (usually pid 1).

**Context**

- `Context([pids=workers()])` - context which uses the processes specified in the pids

**Redistribution**
- `redistribute(c, layout)` - redistribute an object from the current layout to a specified layout.
- `shift(c, v0, N)` - move chunks from one worker to the Nth next worker in the worker pool. N can be negative. The first N workers (last N if N < 0) get v0.
- `rotate(c, N)` - rotate chunks from one worker to the Nth next worker in the worker pool, wrapping around when end of list of workers is reached.

**Reading from a file**

- `TextFile(f, [mode="r", chunksize=128M])` - creates a recipe for a text file to be read. The file is read in `chunksize` units in one go.
- `split(f, char)` - split the `TextFile` node `f` wherever `char` appears to create an array of strings.
- `readlines(f)` - same as `split(f, '\n')`

The file is read at different offsets by different processes each reading it at `chunksize` bytes at a time - that's how parallelizm is achieved.

**Upcoming features**
- Sorting
- Getindex, permutations
- Array operations like MatMul and transpose
- Partitioning for irregular data

## Design

The goal of ComputeFramework is to create sufficient scope for multiple-dispatch to be employed at various stages of a parallel computation. New capabilities, distributions, device types can be added by defining new methods on a very small set of generic functions. The DAG also allows for other optimizations (fusing maps and reduces), fault-tolerance and visualization.

### Nodes

**AbstractNode**

An abstract type for a node in the computation DAG

**DataNode <: AbstractNode**

A node that does not require further computation. For example, a DistMemory and a FileNode.

**ComputeNodes <: AbstractNode**

Nodes that represent computation. When `compute` is called on a `ComputeNode` it results in a `DataNode`.

![compute-node](https://cloud.githubusercontent.com/assets/25916/11872894/cee06854-a4fd-11e5-94d8-bb22d5d7bad4.png)

### Distribution, gathering, and redistribution

Data Layout types are defined in `layout.jl`, subtypes of `AbstractLayout` represent a certain slicing of an object.

A layout represents a way of splitting an object in preparation for its parts to be scattered to worker processes, and also a way of combining pieces of an object back together to form the original object. These two operations are described by means of methods to `partition` and `gather` generic functions. As an example `ColumnLayout` is a Layout type which divides a matrix as blocks of columns, and can piece such blocks of columns together to form the original matrix. Once a layout type and the corresponding `partition` and `gather` methods are implemented, the machinary of changing an object's layout redistribute will start to work.

![layouts](https://cloud.githubusercontent.com/assets/25916/11873353/05c01520-a500-11e5-898b-0bf5b838fcb6.png)

Specifically, a layout type `MyLayoutType` should define `partition(::Context, object, p::MyLayoutType, targets)` and `gather(::Context, p::MyLayoutType, pieces::Vector)` methods. Here `targets` is a vector of processes (more generally devices) where the partitions need to go to, `pieces` is the vector of parts received from processes, in the same order they were returned by `partition`.

### Fault-tolerence and UI

Since the DAG has enough information to recompute nodes or chunks from any failed point, fault-tolerance can be built into the system. `compute` and `gather` take a first argument which is the `Context` type and pass it throughout the computation. There can be a centralized way of signalling what is currently going on in the cluster which can then be visualized with more UI to read error messages, restart computation and so on.

###

We thank the NIH for supporting this work at MIT.
