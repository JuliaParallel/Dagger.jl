
import Base: ctranspose, transpose, A_mul_Bt, At_mul_B, Ac_mul_B, At_mul_Bt, Ac_mul_Bc, A_mul_Bc

immutable Transpose{T,N} <: LazyArray{T,N}
    f::Function
    input::LazyArray
end

function Transpose(f,x::LazyArray)
    @assert 1 <= ndims(x) && ndims(x) <= 2
    Transpose{eltype(x), 2}(f,x)
end
function size(x::Transpose)
    sz = size(x.input)
    if length(sz) == 1
        (1, sz[1])
    else
        (sz[2], sz[1])
    end
end

ctranspose(x::LazyArray) = Transpose(ctranspose, x)
ctranspose(x::AbstractPart) = Thunk(ctranspose, (x,))

transpose(x::LazyArray) = Transpose(transpose, x)
transpose(x::AbstractPart) = Thunk(transpose, (x,))

function ctranspose(x::DenseDomain{2})
    d = indexes(x)
    DenseDomain(d[2], d[1])
end
function ctranspose(x::DenseDomain{1})
    d = indexes(x)
    DenseDomain(1, d[1])
end

function ctranspose(x::DomainSplit)
    DomainSplit(head(x)', parts(x)')
end

function _ctranspose(x::AbstractArray)
    Any[x[j,i]' for i=1:size(x,2), j=1:size(x,1)]
end

function stage(ctx, node::Transpose)
    inp = cached_stage(ctx, node.input)
    dmn = domain(inp)
    dmnT = dmn'
    thunks = _ctranspose(parts(inp))
    Cat(parttype(inp), dmnT, thunks)
end

export Distribute

immutable Distribute{N, T} <: LazyArray{N, T}
    domain::DomainSplit
    data::AbstractPart
end

Distribute(d::DomainSplit, p::AbstractPart) =
    Distribute{eltype(parttype(p)), ndims(d)}(d, p)

size(x::Distribute) = size(x.domain)


function Distribute(p::PartitionScheme, data)
    Distribute(partition(p, domain(data)), persist!(part(data)))
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
    Cat(parttype(d.data), d.domain, map(c -> sub(d.data, c), parts(d.domain)))
end


import Base: *, +

immutable MatMul{T, N} <: LazyArray{T, N}
    a::LazyArray
    b::LazyArray
end

function mul_size(a,b)
  if ndims(b) == 1
    (size(a,1),)
  else
    (size(a,1), size(b,2))
  end
end
size(x::MatMul) = mul_size(x.a, x.b)
MatMul(a,b) =
  MatMul{promote_type(eltype(a), eltype(b)), length(mul_size(a,b))}(a,b)
(*)(a::LazyArray, b::LazyArray) = MatMul(a,b)
# Bonus method for matrix-vector multiplication
(*)(a::LazyArray, b::Vector) = MatMul(a,PromotePartition(b))

function (*)(a::ArrayDomain{2}, b::ArrayDomain{2})

    if size(a, 2) != size(b, 1)
        throw(DimensionMismatch("The domains cannot be multiplied"))
    end

    DenseDomain((indexes(a)[1], indexes(b)[2]))
end
function (*)(a::ArrayDomain{2}, b::ArrayDomain{1})
    if size(a, 2) != length(b)
        throw(DimensionMismatch("The domains cannot be multiplied"))
    end
    DenseDomain((indexes(a)[1],))
end

function (*)(a::DomainSplit, b::DomainSplit)
    DomainSplit(head(a)*head(b), parts(a) * parts(b))
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
    n = size(a, 2)
    for i=1:size(a,1)
        for j=1:size(b, 2)
            c[i,j] = treereduce(+, map(*, reshape(a[i,:], (n,)), b[:, j]))
        end
    end
    c
end

function _mul(a::Matrix, b::Vector; T=eltype(b))
    c = Array(T, size(a,1))
    n = size(a,2)
    for i=1:size(a,1)
        c[i] = treereduce(+, map(*, reshape(a[i, :], (n,)), b))
    end
    c
end

function _mul(a::Vector, b::Vector; T=eltype(b))
    @assert length(b) == 1
    [x * b[1] for x in a]
end


"""
This is a way of suggesting that stage should call
stage_operand with the operation and other arguments
"""
immutable PromotePartition{T,N} <: LazyArray{T,N}
    data::AbstractArray{T,N}
end

size(p::PromotePartition) = size(domain(p.data))

"""
an operand which should be distributed as per convenience
"""
function stage_operand{T}(ctx, ::MatMul, a, b::PromotePartition{T,1})
    dmn_a = domain(a)
    dmn_b = domain(b.data)
    if size(dmn_a, 2) != size(dmn_b, 1)
        throw(DimensionMismatch("Cannot promote array of domain $(dmn2) to multiply with an array of size $(dmn)"))
    end
    ps = parts(dmn_a)
    dmn_out = DomainSplit(dmn_b, BlockedDomains((1,),(ps.cumlength[2],)))

    cached_stage(ctx, Distribute(dmn_out, part(b.data)))
end

function stage_operand(ctx, ::MatMul, a, b)
    cached_stage(ctx, b)
end

function stage(ctx, mul::MatMul)
    a = cached_stage(ctx, mul.a)
    b = stage_operand(ctx, mul, a, mul.b)

    da = domain(a)
    db = domain(b)

    d = da*db
    Cat(Any, d, _mul(parts(a), parts(b); T=Thunk))
end



### Scale

import Base.scale
immutable Scale{T,N} <: LazyArray{T,N}
    l::LazyArray
    r::LazyArray
end
Scale{Tl, Tr, N}(l::LazyArray{Tl}, r::LazyArray{Tr,N}) =
  Scale{promote_type(Tl, Tr), N}(l,r)

size(s::Scale) = size(s.l)

scale(l::Number, r::LazyArray) = BlockwiseOp(x->scale(l, x), (r,))
scale(l::Vector, r::LazyArray) = scale(PromotePartition(l), r)
(*)(l::Diagonal, r::LazyArray) = Scale(PromotePartition(l.diag), r)
scale(l::LazyArray, r::LazyArray) = Scale(l, r)

function stage_operand(ctx, ::Scale, a, b::PromotePartition)
    ps = parts(domain(a))
    b_parts = BlockedDomains((1,), (ps.cumlength[1],))
    head = DenseDomain(1:size(domain(a), 1))
    b_dmn = DomainSplit(head, b_parts)
    cached_stage(ctx, Distribute(b_dmn, part(b.data)))
end

function stage_operand(ctx, ::Scale, a, b)
    cached_stage(ctx, b)
end

function _scale(l, r)
    res = similar(r, Any)
    for i=1:length(l)
        res[i,:] = map(x->Thunk((a,b) -> Diagonal(a)*b, (l[i], x)), r[i,:])
    end
    res
end

function stage(ctx, scal::Scale)
    r = cached_stage(ctx, scal.r)
    l = stage_operand(ctx, scal, r, scal.l)

    @assert size(domain(r), 1) == size(domain(l), 1)

    scal_parts = _scale(parts(l), parts(r))
    Cat(Any, domain(r), scal_parts)
end

immutable Concat{T,N} <: LazyArray{T,N}
    axis::Int
    inputs::Tuple
end
Concat(axis::Int, inputs::Tuple) =
  Concat{promote_type(map(eltype, inputs)...),
         ndims(inputs[1])}(axis, inputs)

function size(c::Concat)
    sz = [size(c.inputs[1])...]
    sz[c.axis] = sum(map(x->size(x, c.axis), c.inputs))
    (sz...,)
end

function cat(idx::Int, ds::DomainSplit...)
    h = head(ds[1])
    out_idxs = [x for x in indexes(h)]
    len = sum(map(x->length(indexes(x)[idx]), ds))
    fst = first(out_idxs[idx])
    out_idxs[idx] = fst:(fst+len-1)
    out_head = DenseDomain(out_idxs)
    out_parts = cumulative_domains(cat(idx, map(parts, ds)...))
    DomainSplit(out_head, out_parts)
end

function stage(ctx, c::Concat)
    inp = Any[cached_stage(ctx, x) for x in c.inputs]

    dmns = map(domain, inp)
    dims = [[i == c.axis ? 0 : i for i in size(d)] for d in dmns]
    if !all(map(x -> x == dims[1], dims[2:end]))
        error("Inputs to cat do not have compatible dimensions.")
    end

    dmn = cat(c.axis, dmns...)
    thunks = cat(c.axis, map(parts, inp)...)
    T = promote_type(map(parttype, inp)...)
    Cat(T, dmn, thunks)
end

Base.cat(idx::Int, x::LazyArray, xs::LazyArray...) =
    Concat(idx, (x, xs...))

Base.hcat(xs::LazyArray...) = cat(2, xs...)
Base.vcat(xs::LazyArray...) = cat(1, xs...)

A_mul_Bt(x::LazyArray, y::LazyArray) = MatMul(x, y')
At_mul_B(x::LazyArray, y::LazyArray) = MatMul(x', y)
Ac_mul_B(x::LazyArray, y::LazyArray) = MatMul(x', y)
At_mul_Bt(x::LazyArray, y::LazyArray) = MatMul(x', y')
Ac_mul_Bc(x::LazyArray, y::LazyArray) = MatMul(x', y')
A_mul_Bc(x::LazyArray, y::LazyArray) = MatMul(x, y')
