
"""
A distributed object

Fields:
    - `chunk_type`: A type for the chunks
    - `refs`: Vector of `PID => RemoteRef` pairs
    - `layout`: A layout object that defines how data is partitioned
    - `metadata`: arbitrary metadata stored internally during compute
"""

type DistData <: DataNode
    chunk_type::Type
    refs::Vector     # Array of PID => RemoteRef pairs
    layout::AbstractLayout
    metadata
end

DistData(chunks, layout, metadata=nothing) =
    DistData(Any, chunks, layout, metadata)

refs(c::DistData) = c.refs
layout(c::DistData) = c.layout

function metadata(c::DistData)
    if c.metadata == nothing
        m = metadata(refs(c), layout(c))
        c.metadata = m
    else
        c.metadata
    end
end

"""
    gather(ctx, node, layout=layout(node))

`gather` a DistData into a sequential type on the parent process.

Args:
- `ctx`: a `Context` object
- `node`: The `DistData` node
- `layout`: The layout to use for collating (defaults to `layout(node)`)
"""
function gather(ctx, node::DistData, layout=layout(node))
    # Fall back to generic gather on the layout
    results = Any[]

    for (pid, ref) in refs(node)

        result = remotecall_fetch((r) -> make_result(fetch(r)), pid, ref)

        if isa(result.value, RemoteException)
            Base.showerror(STDERR, result.value)
            rethrow(result.value)
        end

        push!(results, result.value)

        # Update accumulators
        result.accumulators == nothing && continue

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
    gather(ctx, layout, results)
end


##### Utilities #####

"""
Result from a worker along with accumulator values.

Fileds:
- accumulators: Vector of accumulator ID => Value pairs
- value: value of computation of chunks
"""
immutable ResultData{T}
    accumulators::Vector
    value::T
end

function make_result(x)
    # Get values of accumulators
    ResultData(Pair[acc.id => val for (acc, val) in values(ComputeFramework._proc_accumulators)], x)
end

tuplize(t::Tuple) = t
tuplize(t) = (t,)
