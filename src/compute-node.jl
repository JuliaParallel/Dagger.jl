
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

### Map ###

immutable MapNode{F, T<:Tuple} <: ComputeNode
    f::F
    input::T
end

map(f, ns::AbstractNode...) = MapNode(f, ns)

"""
All inputs are DistMemory with the same partitioning
"""
function compute(ctx, node::MapNode)
    compute(ctx, mappart(part -> map(node.f, part), node.input))
end

### MapParts ###

immutable MapPartNode{F, T<:Tuple} <: ComputeNode
    f::F
    input::T
end

mappart(f, ns::Tuple) = MapPartNode(f, ns)
mappart(f, ns::AbstractNode...) = MapPartNode(f, ns)

function compute{F, N, T<:DistMemory}(ctx, node::MapPartNode{F, NTuple{N, T}})
    inp = node.input[1]
    futures = Pair[dev => remotecall(dev, (x) -> node.f(fetch(x)), ref)
                    for (dev, ref) in refs(inp)]
    DistMemory(futures, inp.partition)
end

function compute{F, N}(ctx, x::MapPartNode{F, NTuple{N, DataNode}})
    # promote_dnode them all
end

function compute(ctx, x::MapPartNode)
   compute(ctx, MapPartNode(x.f, map(inp -> compute(ctx, inp), x.input)))
end

### Reduce ###

immutable ReduceNode{F, X, T<:AbstractNode} <: ComputeNode
    f::F
    v0::X
    input::T
end

reduce(f, v0, node::AbstractNode) = ReduceNode(f, v0, node)

function compute(ctx, node::ReduceNode)
   reduce(node.f, node.v0, gather(ctx, mappart(part -> reduce(node.f, node.v0, part), node.input)))
end

### GroupBy ###

immutable GroupByNode{F, N<:AbstractNode} <: ComputeNode
    group::Function
    input::N
end

