
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
    res=ResultData(Pair[acc.id => val for (acc, val) in values(ComputeFramework._proc_accumulators)], x)
    io=IOBuffer()
    tic()
    serialize(io, res)
    seekstart(io)
    t=toq()
    println("$t seconds, size : ", Base.nb_available(io))
    res
end

function gather(ctx, n::DistMemory)
    # Fall back to generic gather on the layout
    results = Array(Any, length(refs(n)))
    t0=time()
    println(t0)
    @time @sync begin
        for (idx, v) in enumerate(refs(n))
            (pid, ref) = v

            @async begin
                results[idx] = remotecall_fetch(pid, (r, idx) -> (t0=time();println(t0); res=make_result(fetch(r));println("@ rcfetch : ", idx, ", ", time()-t0); res) , ref, idx)
                println("$idx returned after : ", time()-t0)
            end
        end
    end
    println("@sync after : ",  time()-t0)

        # TODO: retry etc
   for result in results
        if isa(result.value, RemoteException)
            Base.showerror(STDERR, result.value)
            rethrow(result.value)
        end

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
    gather(ctx, n.layout, map(x -> x.value, results))
end

refs(c::DistMemory) = c.refs
layout(c::DistMemory) = c.layout

##### Compute #####

#typealias SharedArrays Union{SharedSparseMatrixCSC, SharedArray}

# function compute{A<:SharedArrays}(ctx, x::Distribute{A})
#     targets = chunk_targets(ctx, x)
#     chunk_idxs = slice_indices(ctx, size(x.obj), x.layout, targets)
#
#     refs = Pair[(targets[i] => remotecall(targets[i], () -> getindex(x.obj, chunk_idxs[i])))
#                 for i in 1:length(targets)]
#
#     DistMemory(eltype(chunks), refs, x.layout)
# end

function compute(ctx, x::Distribute)
    targets = chunk_targets(ctx, x)
    chunks = slice(ctx, x.obj, x.layout, targets)

    refs = Pair[(targets[i] => remotecall(targets[i], () -> chunks[i]))
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
        println("@1 : ", time())
        futures = Pair[pid => @spawnat pid f(map(fetch, rs)...)
                        for (pid, rs) in pid_chunks]
        println("@2 : ", time())
        DistMemory(futures, node.input[1].layout)
    end
end
