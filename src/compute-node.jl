
import Base: map, reduce, filter

export Broadcast, Partitioned

immutable Partitioned{T, P<:AbstractPartition} <: ComputeNode
    obj::T
    partition::P
end
Partitioned(x::AbstractArray) = Partitioned(x, CutDim{ndims(x)}())
Broadcast(x) = Partitioned(x, Bcast())

function compute(ctx, x::Partitioned)
    targets = chunk_targets(ctx, x)
    chunks = slice(x.arr, x.partition, devs)

    refs = [remotecall(targets[i], () -> chunks[i])
                for i in 1:length(targets)]

    DistMemory(refs, x.partition)
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
function compute{F, N, T<:DistMemory}(ctx, x::MapNode{F, NTuple{N, T}})
    futures = [doafter(fs..., (xs...) -> map(x.f, xs...)) for fs in map(chunks, x.input)]
    DistMemory(futures, dist.partition)
end

function compute{F, N}(ctx, x::MapNode{F, NTuple{N, DataNode}})
    # promote_dnode them all
end

immutable GroupByNode{F, N<:AbstractNode} <: ComputeNode
    group::Function
    input::N
end
