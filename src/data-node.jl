
# When a node is computed, it becomes a Chunks node.

"""
Most basic and default DataNode
"""
immutable DistMemory{T, P <: AbstractPartition} <: DataNode
    chunks::Vector
    partition::P
end
DistMemory(T::Type, chunks, partition) = DistMemory{T}(chunks, partition)
DistMemory(chunks, partition) = DistMemory(Any, chunks, partition)

chunks(c::DistMemory) = c.chunks
partition(c::DistMemory) = c.partition
