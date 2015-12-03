
import Base: map, reduce, filter

export Broadcast, Partitioned

### Distributing data ###

immutable Partitioned{T, P<:AbstractPartition} <: ComputeNode
    obj::T
    partition::P
end
Partitioned(x::AbstractArray) = Partitioned(x, CutDim{ndims(x)}())
Broadcast(x) = Partitioned(x, Bcast())

function compute(ctx, x::Partitioned)
    targets = chunk_targets(ctx, x)
    chunks = slice(ctx, x.obj, x.partition, targets)

    refs = Pair[(targets[i] => remotecall(targets[i], () -> chunks[i]))
                for i in 1:length(targets)]

    DistMemory(eltype(chunks), refs, x.partition)
end

immutable MapNode{F, T<:Tuple} <: ComputeNode
    f::F
    input::T
end

map(f, ns::AbstractNode...) = MapNode(f, ns)

function compute(ctx, x::MapNode)
   compute(ctx, MapNode(x.f, map(inp -> compute(ctx, inp), x.input)))
end

"""
All inputs are DistMemory with the same partitioning
"""
function compute{F, N, T<:DistMemory}(ctx, node::MapNode{F, NTuple{N, T}})
    inp = node.input[1]
    futures = Pair[dev => remotecall(dev, (x) -> map(node.f, fetch(x)), ref)
                    for (dev, ref) in refs(inp)]
    DistMemory(futures, inp.partition)
end

function compute{F, N}(ctx, x::MapNode{F, NTuple{N, DataNode}})
    # promote_dnode them all
end

immutable GroupByNode{F, N<:AbstractNode} <: ComputeNode
    group::Function
    input::N
end

