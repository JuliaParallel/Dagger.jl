export redistribute, rotate, shift

immutable Redistribute{L1<:Nullable, L2, N<:AbstractNode} <: ComputeNode
    input::N
    to_layout::L2
    gather_layout::L1
end
redistribute(input, to_layout) = Redistribute(input, to_layout, Nullable{AbstractLayout}())
redistribute(input, to_layout, gather_layout) = Redistribute(input, to_layout, Nullable(gather_layout))

"""Scatter parts to workers to realize a new layout"""
function scatter_parts(ctx, part, from_layout, to_layout)
    # Send parts of this chunk of data to other processes
    compute(ctx, Distribute(part, to_layout))
end

"""Assemble scattered parts from workers to create local chunk"""
function gather_parts(ctx, parts, from_layout, to_layout)
    # collate localparts received using the original distribution
    gather(ctx, from_layout, parts)
end

function compute(ctx, node::Redistribute)
    inp = compute(ctx, node.input)
    @assert isa(inp, DistData) # for now

    from_layout = inp.layout
    to_layout = node.to_layout
    gather_layout = isnull(node.gather_layout) ? from_layout : get(node.gather_layout)

    parts = gather(ctx, mappart(part -> scatter_parts(ctx, part, from_layout, to_layout), inp))
    refmatrix = reduce(hcat, map(refs, parts))
    refparts = compute(ctx, Distribute(refmatrix, RowLayout()))

    assembly = mappart(refparts) do localparts
        data = [fetch(p[2]) for p in localparts]
        gather_parts(ctx, data, gather_layout, to_layout)
    end

    compute(ctx, assembly)
end

## Allgather

"""
Assemble chunks from every process into every other process
"""
allgather(x) = redistribute(x, Bcast())
allgather(x, joinlayout) = redistribute(x, Bcast(), joinlayout)

## Rotate and Shift

immutable Rotate <: ComputeNode
    input::AbstractNode
    step::Integer
end

rotate(input::AbstractNode, step) = Rotate(input, step)

function rotate_vec(xs, step)
    n = length(xs)
    step = ((step % n) + n) % n
    vcat(xs[n-step+1:n], xs[1:n-step])
end

function compute(ctx, node::Rotate)
    # Rotate refs
    inp = compute(ctx, node.input)
    rs = map(x->x[2], rotate_vec(refs(inp), node.step))
    DistData(map(=>, chunk_targets(ctx, node), rs), inp.layout)
end

immutable Shift{T} <: ComputeNode
    input::AbstractNode
    zero::T
    step::Integer
end

shift(x, z) = Shift(x, z, 1)
shift(input, zero, step) = Shift(input, zero, step)

function zero_parts(z, rs)
    Pair[r[1] => (@spawnat r[1] z) for r in rs]
end

function compute(ctx, node::Shift)
    # Rotate refs
    inp = compute(ctx, node.input)
    rs = refs(inp)
    n = length(rs)
    step = node.step
    shifted = step > 0 ?
        vcat(zero_parts(node.zero, rs[n-step+1:n]), rs[1:n-step]) :
        vcat(rs[-step+1:n], zero_parts(node.zero, rs[1:-step]))
    DistData(map(=>, chunk_targets(ctx, node), map(x->x[2], shifted)), inp.layout)
end
