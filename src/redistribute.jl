export Repartition

immutable Repartition{P, N<:AbstractNode} <: ComputeNode
    partition::P
    input::N
end

"""Scatter parts to workers"""
function scatter_parts{m,n}(ctx, part, from_partition::CutDim{m}, to_partition::CutDim{n})
    # Send parts of this chunk of data to other processes
    compute(ctx, Partitioned(part, to_partition))
end

"""Gather parts from workers"""
function gather_parts{m,n}(ctx, parts, from_partition::CutDim{m}, to_partition::CutDim{n})
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

immutable Transpose <: ComputeNode
    input::AbstractNode
end

Base.transpose(x::AbstractNode) = Transpose(x)

complement(::CutDim{1}) = CutDim{2}()
complement(::CutDim{2}) = CutDim{1}()

function compute(ctx, node::Transpose)
    inp = compute(ctx, node.input)
    @assert isa(inp, DistMemory) # for now
    @assert isa(inp.partition, CutDim) # for now

    DistMemory(refs(compute(ctx, mappart(transpose, inp))), complement(inp.partition))
end
