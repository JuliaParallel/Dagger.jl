import Base: transpose

immutable Transpose <: ComputeNode
    input::AbstractNode
end

transpose(x::AbstractNode) = Transpose(x)

complement(::SliceDimension{1}) = cutdim(2)
complement(::SliceDimension{2}) = cutdim(1)

function compute(ctx, node::Transpose)
    inp = compute(ctx, node.input)
    @assert isa(inp, DistData) # for now
    @assert isa(layout(inp), SliceDimension) # for now

    part_transpose = compute(ctx, mappart(transpose, inp); output_layout=complement(layout(inp)), output_metadata=nothing)
    compute(ctx, redistribute(part_transpose, layout(inp)))
end
