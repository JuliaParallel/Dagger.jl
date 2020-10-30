struct Transpose{T,N} <: ArrayOp{T,N}
    f::Function
    input::ArrayOp
end

function Transpose(f,x::ArrayOp)
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

transpose(x::ArrayOp) = Transpose(transpose, x)
transpose(x::Union{Chunk, Thunk}) = Thunk(transpose, x)

adjoint(x::ArrayOp) = Transpose(adjoint, x)
adjoint(x::Union{Chunk, Thunk}) = Thunk(adjoint, x)

function adjoint(x::ArrayDomain{2})
    d = indexes(x)
    ArrayDomain(d[2], d[1])
end
function adjoint(x::ArrayDomain{1})
    d = indexes(x)
    ArrayDomain(1, d[1])
end

function _ctranspose(x::AbstractArray)
    Any[delayed(adjoint)(x[j,i]) for i=1:size(x,2), j=1:size(x,1)]
end

function stage(ctx::Context, node::Transpose)
    inp = cached_stage(ctx, node.input)
    thunks = _ctranspose(chunks(inp))
    DArray(eltype(inp), domain(inp)', domainchunks(inp)', thunks)
end

import Base: *, +

struct MatMul{T, N} <: ArrayOp{T, N}
    a::ArrayOp
    b::ArrayOp
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
(*)(a::ArrayOp, b::ArrayOp) = MatMul(a,b)
# Bonus method for matrix-vector multiplication
(*)(a::ArrayOp, b::Vector) = MatMul(a,PromotePartition(b))
(*)(a::AbstractArray, b::ArrayOp) = MatMul(PromotePartition(a), b)

function (*)(a::ArrayDomain{2}, b::ArrayDomain{2})

    if size(a, 2) != size(b, 1)
        throw(DimensionMismatch("The domains cannot be multiplied"))
    end

    ArrayDomain((indexes(a)[1], indexes(b)[2]))
end
function (*)(a::ArrayDomain{2}, b::ArrayDomain{1})
    if size(a, 2) != length(b)
        throw(DimensionMismatch("The domains cannot be multiplied"))
    end
    ArrayDomain((indexes(a)[1],))
end

function (*)(a::Blocks{2}, b::Blocks{2})
    Blocks(a.blocksize[1], b.blocksize[2])
end
(*)(a::Blocks{2}, b::Blocks{1}) =
    Blocks((a.blocksize[1],))

function (+)(a::ArrayDomain, b::ArrayDomain)
    if a == b
        DimensionMismatch("The domains cannot be added")
    end
    a
end

(*)(a::Union{Chunk, Thunk}, b::Union{Chunk, Thunk}) = Thunk(*, a,b)
(+)(a::Union{Chunk, Thunk}, b::Union{Chunk, Thunk}) = Thunk(+, a,b)

# we define our own matmat and matvec multiply
# for computing the new domains and thunks.
function _mul(a::Matrix, b::Matrix; T=eltype(a))
    c = Array{T}(undef, (size(a,1), size(b,2)))
    n = size(a, 2)
    for i=1:size(a,1)
        for j=1:size(b, 2)
            c[i,j] = treereduce(+, map(*, reshape(a[i,:], (n,)), b[:, j]))
        end
    end
    c
end

function _mul(a::Matrix, b::Vector; T=eltype(b))
    c = Array{T}(undef, size(a,1))
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

function promote_distribution(ctx::Context, m::MatMul, a,b)
    iscompat = try domain(a) * domain(b); true
               catch e; false end
    if iscompat
        return a,b
    end

    pa = domainchunks(a)
    pb = domainchunks(b)

    d = DomainBlocks((1,1), (pa.cumlength[2], pb.cumlength[2])) # FIXME: this is not generic
    a, cached_stage(ctx, Distribute(d, b))
end

function stage_operands(ctx::Context, m::MatMul, a, b)
    if size(a, 2) != size(b, 1)
        error(DimensionMismatch("Inputs to * have incompatible size"))
    end
    # take the row distribution of a and get b onto that.

    stg_a = cached_stage(ctx, a)
    stg_b = cached_stage(ctx, b)
    promote_distribution(ctx, m, stg_a, stg_b)
end

"An operand which should be distributed as per convenience"
function stage_operands(ctx::Context, ::MatMul, a::ArrayOp, b::PromotePartition{T,1}) where T
    stg_a = cached_stage(ctx, a)
    dmn_a = domain(stg_a)
    dchunks_a = domainchunks(stg_a)
    dmn_b = domain(b.data)
    if size(dmn_a, 2) != size(dmn_b, 1)
        throw(DimensionMismatch("Cannot promote array of domain $(dmn_b) to multiply with an array of size $(dmn_a)"))
    end
    dmn_out = DomainBlocks((1,),(dchunks_a.cumlength[2],))

    stg_a, cached_stage(ctx, Distribute(dmn_out, b.data))
end

function stage_operands(ctx::Context, ::MatMul, a::PromotePartition, b::ArrayOp)

    if size(a, 2) != size(b, 1)
        throw(DimensionMismatch("Cannot promote array of domain $(dmn_b) to multiply with an array of size $(dmn_a)"))
    end
    stg_b = cached_stage(ctx, b)

    ps = domainchunks(stg_b)
    dmn_out = DomainBlocks((1,1),([size(a.data, 1)], ps.cumlength[1],))
    cached_stage(ctx, Distribute(dmn_out, a.data)), stg_b
end

function stage(ctx::Context, mul::MatMul)
    a, b = stage_operands(ctx, mul, mul.a, mul.b)
    d = domain(a)*domain(b)
    DArray(Any, d, domainchunks(a)*domainchunks(b),
                          _mul(chunks(a), chunks(b); T=Thunk))
end

Base.power_by_squaring(x::DArray, i::Int) = foldl(*, ntuple(idx->x, i))


### Scale

struct Scale{T,N} <: ArrayOp{T,N}
    l::ArrayOp
    r::ArrayOp
end
Scale(l::ArrayOp{Tl}, r::ArrayOp{Tr,N}) where {Tl, Tr, N} =
  Scale{promote_type(Tl, Tr), N}(l,r)

size(s::Scale) = size(s.l)

scale(l::Number, r::ArrayOp) = BlockwiseOp(x->scale(l, x), (r,))
scale(l::Vector, r::ArrayOp) = scale(PromotePartition(l), r)
(*)(l::Diagonal, r::ArrayOp) = Scale(PromotePartition(l.diag), r)
scale(l::ArrayOp, r::ArrayOp) = Scale(l, r)

function stage_operand(ctx::Context, ::Scale, a, b::PromotePartition)
    ps = domainchunks(a)
    b_parts = DomainBlocks((1,), (ps.cumlength[1],))
    cached_stage(ctx, Distribute(b_parts, b.data))
end

function stage_operand(ctx::Context, ::Scale, a, b)
    cached_stage(ctx, b)
end

function _scale(l, r)
    res = similar(r, Any)
    for i=1:length(l)
        res[i,:] = map(x->Thunk((a,b) -> Diagonal(a)*b, l[i], x), r[i,:])
    end
    res
end

function stage(ctx::Context, scal::Scale)
    r = cached_stage(ctx, scal.r)
    l = stage_operand(ctx, scal, r, scal.l)

    @assert size(domain(r), 1) == size(domain(l), 1)

    scal_parts = _scale(chunks(l), chunks(r))
    DArray(Any, domain(r), domainchunks(r), scal_parts)
end

struct Concat{T,N} <: ArrayOp{T,N}
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

function Base.cat(d::ArrayDomain, ds::ArrayDomain...; dims::Int)
    h = (d)
    out_idxs = [x for x in indexes(h)]
    len = sum(map(x->length(indexes(x)[dims]), (d, ds...)))
    fst = first(out_idxs[dims])
    out_idxs[dims] = fst:(fst+len-1)
    ArrayDomain(out_idxs)
end

function stage(ctx::Context, c::Concat)
    inp = Any[cached_stage(ctx, x) for x in c.inputs]

    dmns = map(domain, inp)
    dims = [[i == c.axis ? 0 : i for i in size(d)] for d in dmns]
    if !all(map(x -> x == dims[1], dims[2:end]))
        error("Inputs to cat do not have compatible dimensions.")
    end

    dmn = cat(dmns..., dims = c.axis)
    dmnchunks = cumulative_domains(cat(map(domainchunks, inp)..., dims = c.axis))
    thunks = cat(map(chunks, inp)..., dims = c.axis)
    T = promote_type(map(eltype, inp)...)
    DArray(T, dmn, dmnchunks, thunks)
end

Base.cat(x::ArrayOp, xs::ArrayOp...; dims::Int) = Concat(dims, (x, xs...))

Base.hcat(xs::ArrayOp...) = cat(xs..., dims=2)
Base.vcat(xs::ArrayOp...) = cat(xs..., dims=1)
