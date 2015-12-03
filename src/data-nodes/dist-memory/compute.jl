
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
    gather(ctx, n.partition, [take!(ref) for (pid, ref) in refs(n)])
end

refs(c::DistMemory) = c.refs
partition(c::DistMemory) = c.partition

##### Compute #####

function compute(ctx, x::Partitioned)
    targets = chunk_targets(ctx, x)
    chunks = slice(ctx, x.obj, x.partition, targets)

    refs = Pair[(targets[i] => remotecall(targets[i], () -> chunks[i]))
                for i in 1:length(targets)]

    DistMemory(eltype(chunks), refs, x.partition)
end

function compute{N, T<:DistMemory}(ctx, node::MapPartNode{NTuple{N, T}})
    refsets = zip(map(x -> map(y->y[2], refs(x)), node.input)...) |> collect
    pids = map(x->x[1], refs(node.input[1]))
    pid_chunks = zip(pids, map(tuplize, refsets)) |> collect

    futures = Pair[pid => remotecall(pid, (xs) -> node.f(map(fetch, xs)...), rs)
                    for (pid, rs) in pid_chunks]

    DistMemory(futures, node.input[1].partition)
end

