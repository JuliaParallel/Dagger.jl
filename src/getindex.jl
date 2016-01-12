import Base.intersect

intersect(::Colon, ::Colon) = Colon()
intersect(::Colon, r) = r
intersect(r, ::Colon) = r

immutable GetIndex <: ComputeNode
    input::AbstractNode
    idx::Tuple
end

Base.getindex(x::AbstractNode, idx...) = GetIndex(x, idx)

function lookup_parts(ctx, ranges, subidx)
    Any[map((a, b) -> intersect(a, b) - (first(a)-1), r, subidx) for r in ranges]
end

function compute(ctx, node::GetIndex)
    inp = compute(ctx, node.input)
    parts = lookup_parts(ctx, metadata(inp), node.idx)
    partidx = distribute(parts)
    new_metadata = map(x->map((y, z)->y-first(z)+1, x, node.idx), parts)
    #@show new_metadata
    compute(ctx, mappart(inp, partidx) do part, idx
        getindex(part, idx[1]...)
    end; output_layout=layout(inp), output_metadata=new_metadata)
end

