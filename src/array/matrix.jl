
import Base: transpose

immutable Transpose <: Computation
    input::Computation
end

transpose(x::AbstractPart) = Thunk(transpose, (x,))
transpose(x::Computation) = Transpose(x)
transpose(x::BlockPartition) = BlockPartition((x.blocksize[2], x.blocksize[1]))
function transpose(x::DenseDomain{2})
    d = indexes(x)
    DenseDomain(d[2], d[1])
end

function stage(ctx, node::Cat)
    node
end

function stage(ctx, node::Transpose)
    inp = cached_stage(ctx, node.input)
    dmn = domain(inp)
    @assert isa(dmn, DomainBranch)
    dmnT = DomainBranch(head(dmn)', dmn.children')
    thunks = Array(Thunk, size(dmnT.children))
    transpose!(thunks, inp.parts)
    Cat(inp.partition', parttype(inp), dmnT, thunks)
end

"""
This is a way of suggesting that stage should call
stage_operand with the operation and other arguments
"""
immutable PromotePartition{T} <: Computation
    data::T
end

export Distribute

immutable Distribute <: Computation
    partition::PartitionScheme
    data::Any
end

#=
todo
function auto_partition(data::AbstractArray, chsize)
    sz = sizeof(data) * B
    per_chunk = chsize/(sizeof(eltype(data))*B)
    n = floor(Int, sqrt(per_chunk))

    dims = size(data)
    if ndims(data) == 1
        BlockPartition((floor(Int, per_chunk),))
    elseif ndims(data)==2
        BlockPartition(per_chunk/dims[2], per_chunk/dims[1])
    end
end

function Distribute(data::AbstractArray; chsize=64MB)
    p = auto_partition(data, chsize)
    Distribute(p, data)
end
=#

function stage(ctx, d::Distribute)
    p = part(d.data)
    dmn = domain(p)
    branch = partition(d.partition, dmn)
    Cat(d.partition, typeof(d.data), branch, map(c -> sub(p, c), branch.children))
end


import Base: *, +

immutable MatMul <: Computation
    a::Computation
    b::Computation
end

(*)(a::Computation, b::Computation) = MatMul(a,b)
# Bonus method for matrix-vector multiplication
(*)(a::Computation, b::Vector) = MatMul(a,PromotePartition(b))

function (*)(a::ArrayDomain{2}, b::ArrayDomain{2})

    if size(a, 2) != size(b, 1)
        DimensionMismatch("The domains cannot be multiplied")
    end

    DenseDomain((indexes(a)[1], indexes(b)[2]))
end
function (*)(a::ArrayDomain{2}, b::ArrayDomain{1})
    if size(a, 2) != length(b)
        DimensionMismatch("The domains cannot be multiplied")
    end
    DenseDomain((indexes(a)[1],))
end

function (*)(a::DomainBranch, b::DomainBranch)
    try
        DomainBranch(head(a)*head(b), _mul(a.children, b.children))
    catch err
        if isa(err, DimensionMismatch)
            throw(DimensionMismatch("Objects being multiplied have incompatible block distributions"))
        else
            rethrow(err)
        end
    end
end

function (*)(a::BlockPartition{2}, b::BlockPartition{2})
    BlockPartition(a.blocksize[1], b.blocksize[2])
end
(*)(a::BlockPartition{2}, b::BlockPartition{1}) =
    BlockPartition((a.blocksize[1],))

function (+)(a::ArrayDomain, b::ArrayDomain)
    if a == b
        DimensionMismatch("The domains cannot be added")
    end
    a
end

(*)(a::AbstractPart, b::AbstractPart) = Thunk(*, (a,b))
(+)(a::AbstractPart, b::AbstractPart) = Thunk(+, (a,b))

# we define our own matmat and matvec multiply
# for computing the new domains and thunks.
function _mul(a::Matrix, b::Matrix; T=eltype(a))
    c = Array(T, (size(a,1), size(b,2)))
    for i=1:size(a,1)
        for j=1:size(b, 2)
            c[i,j] = reduce(+, map(*, a[i,:], b[:, j]))
        end
    end
    c
end

function _mul(a::Matrix, b::Vector; T=eltype(b))
    c = Array(T, size(a,1))
    for i=1:size(a,1)
        c[i] = reduce(+, map(*, a[i, :], b))
    end
    c
end

"""
an operand which should be distributed as per convenience
"""
function stage_operand{T<:AbstractVector}(ctx, ::MatMul, a, b::PromotePartition{T})
    p = part(b.data)
    scheme = partition(a)
    @assert isa(scheme, BlockPartition)
    # use scheme's column distribution here
    scheme_b = BlockPartition((scheme.blocksize[2],))
    cached_stage(ctx, Distribute(scheme_b, b.data))
    #=
    d = domain(a)
    part_domains = map(x -> DenseDomain((indexes(x)[2],)), d.children[1, :]')
    bd = DomainBranch(domain(p), part_domains)
    =#
end

function stage_operand(ctx, ::MatMul, a, b)
    cached_stage(ctx, b)
end

function stage(ctx, mul::MatMul)
    a = cached_stage(ctx, mul.a)
    b = stage_operand(ctx, mul, a, mul.b)

    pa = partition(a)::BlockPartition
    pb = partition(b)::BlockPartition
    p = pa*pb

    da = domain(a)
    db = domain(b)

    d = da*db
    @show d.children
    Cat(p, Any, d, _mul(a.parts, b.parts; T=Thunk))
end
