
"""
Most basic and default DataNode
"""
immutable DistMemory{T, P<:AbstractLayout} <: DataNode
    refs::Vector     # Array of PID => RemoteRef pairs
    layout::P
end

function DistMemory{P<:AbstractLayout}(T::Type, chunks, layout::P)
    DistMemory{T, P}(chunks, layout)
end

function DistMemory(chunks, layout)
    DistMemory(Any, chunks, layout)
end

immutable ResultData{T}
    accumulators::Vector
    value::T
end

function make_result(x)
    # Get values of accumulators
    ResultData(Pair[acc.id => val for (acc, val) in values(ComputeFramework._proc_accumulators)], x)
end

function gather(ctx, n::DistMemory)
    # Fall back to generic gather on the layout
    results = Any[]
    for (pid, ref) in refs(n)
        result = remotecall_fetch((r) -> make_result(fetch(r)), pid, ref)
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

    # Fallback to default gather method on the layout
    gather(ctx, n.layout, results)
end

refs(c::DistMemory) = c.refs
layout(c::DistMemory) = c.layout

##### Compute #####

#typealias SharedArrays Union{SharedSparseMatrixCSC, SharedArray}

# function compute{A<:SharedArrays}(ctx, x::Distribute{A})
#     targets = chunk_targets(ctx, x)
#     chunk_idxs = slice_indices(ctx, size(x.obj), x.layout, targets)
#
#     refs = Pair[(targets[i] => remotecall(() -> getindex(x.obj, targets[i], chunk_idxs[i])))
#                 for i in 1:length(targets)]
#
#     DistMemory(eltype(chunks), refs, x.layout)
# end

function compute(ctx, x::Distribute)
    targets = chunk_targets(ctx, x)
    chunks = slice(ctx, x.obj, x.layout, targets)

    refs = Pair[(targets[i] => remotecall(() -> chunks[i], targets[i]))
                for i in 1:length(targets)]

    DistMemory(eltype(chunks), refs, x.layout)
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
        DistMemory(futures, node.input[1].layout)
    end
end
