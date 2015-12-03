
import Base: map, reduce, filter

export Broadcast, Partitioned, reducebykey

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

immutable MapNode{T<:Tuple, F} <: ComputeNode
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

immutable MapPartNode{T<:Tuple, F} <: ComputeNode
    f::F
    input::T
end

mappart(f, ns::Tuple) = MapPartNode(f, ns)
mappart(f, ns::AbstractNode...) = MapPartNode(f, ns)

function compute{N, T<:DistMemory}(ctx, node::MapPartNode{NTuple{N, T}})
    inp = node.input[1]
    futures = Pair[dev => remotecall(dev, (x) -> node.f(fetch(x)), ref)
                    for (dev, ref) in refs(inp)]
    DistMemory(futures, inp.partition)
end

function compute{N}(ctx, x::MapPartNode{NTuple{N, DataNode}})
    # promote_dnode them all
end

function compute(ctx, x::MapPartNode)
   compute(ctx, MapPartNode(x.f, map(inp -> compute(ctx, inp), x.input)))
end

### Reduce ###

immutable ReduceNode{T<:AbstractNode, F, X} <: ComputeNode
    f::F
    v0::X
    input::T
end

reduce(f, v0, node::AbstractNode) = ReduceNode(f, v0, node)

function compute(ctx, node::ReduceNode)
   reduce(node.f, node.v0, gather(ctx, mappart(part -> reduce(node.f, node.v0, part), node.input)))
end

### Filter ###

immutable FilterNode{N<:AbstractNode, F} <: ComputeNode
    f::F
    input::N
end

filter(f, x::AbstractNode) = FilterNode(f, x)

function compute(ctx, node::FilterNode)
    compute(ctx, mappart(part -> filter(node.f, part), node))
end

### GroupBy ###

immutable ReduceByKey{N<:AbstractNode, F, T} <: ComputeNode
    f::F
    v0::T
    input::N
end

reducebykey(f, v0, input) = ReduceByKey(f, v0, input)

function reducebykey_seq(f, v0, itr, dict=Dict())
    for (k, v) in itr
        dict[k] = f(get(dict, k, v0), v)
    end
    dict
end

function compute(ctx, node::ReduceByKey)
    parts = mappart((part) -> reducebykey_seq(node.f, node.v0, part), node.input)
    reduce((acc, chunk) -> reducebykey_seq(node.f, node.v0, chunk, acc), Dict(), gather(ctx, parts))
end
