
# When a node is computed, it becomes a Chunks node.

"""
Most basic and default DataNode
"""
immutable DistMemory{T, P <: AbstractPartition} <: DataNode
    refs::Vector
    partition::P
end
DistMemory{P<:AbstractPartition}(T::Type, chunks, partition::P) =
    DistMemory{T, P}(chunks, partition)

DistMemory(chunks, partition) = DistMemory(Any, chunks, partition)

function gather(ctx, n::DistMemory)
    # Fall back to generic gather on the partition
    # TODO: Fault tolerance
    gather(ctx, n.partition, [take!(dev, ref) for (dev, ref) in refs(n)])
end

refs(c::DistMemory) = c.refs
partition(c::DistMemory) = c.partition
