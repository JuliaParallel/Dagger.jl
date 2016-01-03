# ComputeFramework

**A framework for DAG-based computations**

The goal of ComputeFramework is to create sufficient scope for multiple-dispatch to be employed at various stages of a parallel computation. New capabilities, distributions, device types can be added by defining new methods on a very small set of generic functions. The DAG also allows for other optimizations (fusing maps and reduces), fault-tolerance and visualization.

## Nodes

**AbstractNode**

An abstract type for a node in the computation DAG

**DataNode <: AbstractNode**

A node that does not require further computation. For example, a DistMemory and a FileNode.

**ComputeNodes <: AbstractNode**

Nodes that represent computation. When `compute` is called on a `ComputeNode` it results in a `DataNode`.

![compute-node](https://cloud.githubusercontent.com/assets/25916/11872894/cee06854-a4fd-11e5-94d8-bb22d5d7bad4.png)

## Distribution gathering, and redistribution

Data Layout types are defined in `layout.jl`, subtypes of `AbstractLayout` represent a certain slicing of an object.

A layout is represents a way of splitting an object before its parts are scattered to worker processes, and a way of combining pieces of an object back together to form the original object. These two operations are described by means of methods to `slice` and `gather` functions. As an example `ColumnLayout` is a Layout type which divides a matrix as blocks of columns, and similarly can piece such blocks of columns together to form the original matrix. Once a layout type and the corresponding `slice` and `gather` methods are implemented, the machinary of changing an object's partition will start to work. For example, to change an object's layout from `RowLayout` to `ColumnLayout`, one can create a `Redistribute` compute node, e.g. `column_layout_data = redistribute(row_layout_data, ColumnLayout())`.

![layouts](https://cloud.githubusercontent.com/assets/25916/11873353/05c01520-a500-11e5-898b-0bf5b838fcb6.png)

Specifically, a layout type `MyLayoutType` should come with `slice(::Context, object, p::MyLayoutType, targets)` and `gather(::Context, p::MyLayoutType, pieces::Vector)` methods. Here `targets` is a vector of processes (more generally devices) where the slices need to go to, `pieces` is the vector of parts received from processes, in the same order they were returned by `slice` in.

## Fault-tolerence and UI

Since the DAG has enough information to recompute nodes or chunks from any failed point, fault-tolerance can be built into DataNodes. `compute` and `gather` take a first argument which is the `Context` type and pass it throughout the computation. There can be a centralized way of signalling what is currently going on in the cluster which can then be visualized with more UI to read error messages, restart computation and so on.
