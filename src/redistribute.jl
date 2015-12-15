export Repartition, allgather, rotate, shift

immutable Repartition{P, N<:AbstractNode} <: ComputeNode
    partition::P
    input::N
end

"""Scatter parts to workers to realize a new partition"""
function scatter_parts(ctx, part, from_partition, to_partition)
    # Send parts of this chunk of data to other processes
    compute(ctx, Partitioned(part, to_partition))
end

"""Assemble scattered parts from workers to create local chunk"""
function gather_parts(ctx, parts, from_partition, to_partition)
    # collate localparts received using the original distribution
    gather(ctx, from_partition, parts)
end

function compute(ctx, node::Repartition)
    inp = compute(ctx, node.input)
    @assert isa(inp, DistMemory) # for now

    from_partition = inp.partition
    to_partition = node.partition
    from_partition == to_partition && return inp

    parts = gather(ctx, mappart(part -> scatter_parts(ctx, part, from_partition, to_partition), inp))
    refmatrix = reduce(hcat, map(refs, parts))
    refparts = compute(ctx, Partitioned(refmatrix, CutDim{1}()))

    assembly = mappart(refparts) do localparts
        data = [fetch(p[2]) for p in localparts]
        gather_parts(ctx, data, from_partition, to_partition)
    end

    compute(ctx, assembly)
end

## Allgather

"""
Assemble chunks from every process into every other process
"""
allgather(x) = Repartition(Bcast(), x)

immutable Transpose <: ComputeNode
    input::AbstractNode
end

Base.transpose(x::AbstractNode) = Transpose(x)

complement(::CutDim{1}) = CutDim{2}()
complement(::CutDim{2}) = CutDim{1}()

function compute(ctx, node::Transpose)
    inp = compute(ctx, node.input)
    @assert isa(inp, DistMemory) # for now
    @assert isa(inp.partition, CutDim)

    DistMemory(refs(compute(ctx, mappart(transpose, inp))), complement(inp.partition))
end

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
    DistMemory(map(=>, chunk_targets(ctx, node), rs), inp.partition)
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
    DistMemory(map(=>, chunk_targets(ctx, node), map(x->x[2], shifted)), inp.partition)
end
