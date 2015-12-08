
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

immutable ResultData{T}
    accumulators::Vector
    value::T
end

function make_result(x)
    # Get values of accumulators
    ResultData(Pair[acc.id => val for (acc, val) in values(ComputeFramework._proc_accumulators)], x)
end

function gather(ctx, n::DistMemory)
    # Fall back to generic gather on the partition
    results = Any[]
    for (pid, ref) in refs(n)
        result = remotecall_fetch(pid, (r) -> make_result(fetch(r)), ref)
        # TODO: retry etc

        if isa(result.value, RemoteException)
            Base.showerror(STDERR, result.value)
            rethrow(result.value)
        end

        push!(results, result.value)

        result.accumulators == nothing && continue
        # Update accumulators
        for (id, x) in result.accumulators
            if !haskey(_accumulators, id)
                warn("Unknown accumulator updated")
            else
                acc, val = _accumulators[id]
                _accumulators[id] = (acc, acc.operation(val, x))
            end
        end
    end

    # Fallback to default gather method on the partition
    gather(ctx, n.partition, results)
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

tuplize(t::Tuple) = t
tuplize(t) = (t,)

function compute{N, T<:DistMemory}(ctx, node::MapPartNode{NTuple{N, T}})
    refsets = zip(map(x -> map(y->y[2], refs(x)), node.input)...) |> collect
    pids = map(x->x[1], refs(node.input[1]))
    pid_chunks = zip(pids, map(tuplize, refsets)) |> collect

    let f = node.f
        futures = Pair[pid => @spawnat pid f(map(fetch, rs)...)
                        for (pid, rs) in pid_chunks]
        DistMemory(futures, node.input[1].partition)
    end
end
