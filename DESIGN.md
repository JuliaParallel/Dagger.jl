# ComputeFramework

**A framework for DAG-based computations**

The goal of ComputeFramework is to create sufficient scope for multiple-dispatch to be employed at various stages of a parallel computation. New capabilities, distributions, device types can be added by defining new methods on a very small set of generic functions. The DAG also allows for other optimizations (fusing maps and reduces), fault-tolerance and visualization.

## Nodes

**AbstractNode**

An abstract type for a node in the computation DAG

**DataNode <: AbstractNode**

A node that does not require further computation. For example, a DistMemory (a distributed object), BlocksNode or HDFSNode

**ComputeNodes <: AbstractNode**

Nodes that represent computation. When `compute` is called on a `ComputeNode` it results in a `DataNode`.

![compute-node](https://cloud.githubusercontent.com/assets/25916/11553313/8e81ed32-99b3-11e5-96fc-adc37a5c11c1.png)

## Partitioning and gathering

Partition types are defined in `partition.jl`, subtypes of `AbstractPartition` represent a certain slicing of an object. A partition must define `slice(::Context, object, p::MyPartitionType, targets)` where `targets` is a vector of processes (more generally devices) where the slices need to go to, and similarly `gather(::Context, p::Partition, pieces::Vector)` which collates a vector of pieces back into an object according to the partition. This method is the fallback for `gather(ctx::Context, node::DistMemory)` - other DataNodes can define their own methods to collate data according to different partitioning by adding `gather` methods.

## Conversion and promotion of data nodes

Two data nodes of the same type have a data layout such that `map` operation on the both of them is efficient (e.g. corresponding data chunks are on the same process). Hence `DataNode` types should be parameterized by the partition type (which itself needs to be sufficiently parameterized) for this. For example the type `DistMemory{AbstractArray{T}, CutDim{1}}` means a distribution where an Array is split along dimension 1, whereas `DistMemory{AbstractArray{T}, CutDim{2}}` is split along node 2. Or a HDFSNode and a DistMemory node should either both be made an HDFSNode or a DistMemory node.

`promote_dnode(::Context, a::DataNode, b::DataNode)` should return `a'` and `b'` which can now work with `map` without data movement. `promote_dnode` may make use of `convert_dnode(::Type{T<:DataNode}, x)` function to do this. Similarly there can be a `promote_dnode_rule` function which decides which DataNode type wins. Since deciding which node gets promoted can be worth the time, unlike `Base.promote_rule` these convert and promote functions might look at the actual data before deciding.

Since partitions can be defined however you please and on whatever object is chosen, they can serve many purposes - like splitting a matrix for stencil operations, and then joining them back by removing the extranious boundary rows and columns required during the stencil.

## Dispatch matrices

This design gives us two dispatch tables to define how node computation and node data movement should be done.

* `compute` defines how each compute operation (such as Map, Reduce, Filter) will work when applied on each different data node. Ideally operations on a given DataNode will compute to a DataNode of the same type.

A part of this table might look like.

|                | Map     | Reduce | Filter |
| :------------- |:------- |:-------|--------|
| **DistMemory** |         |        |        |
| **HDFSNode**   |         |        |        |
| **MyDataNode** |         |        |        |

* `promote_dnode` defines how each data node type converts to another data node type

A part of this table might look like.

|                    | DistMemory{P2} | HDFSNode{P2} | MyDataNode{P2} |
| :------------------|:---------------|--------------|----------------|
| **DistMemory{P1}** |                |              |                |
| **HDFSNode{P1}**   |                |              |                |
| **MyDataNode{P1}** |                |              |                |

**Note about heterogeneous devices** The design does not preclude the possibility of having DataNodes that on GPUs or... mechanical turk?

## Fault-tolerence and UI

Since the DAG has enough information to recompute nodes or chunks from any failed point, fault-tolerance can be built into DataNodes. `compute` and `gather` take a first argument which is the `Context` type and pass it throughout the computation. There can be a centralized way of signalling what is currently going on in the cluster which can then be visualized with more UI to read error messages, restart computation and so on.
