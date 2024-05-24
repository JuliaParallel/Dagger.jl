# Transpose/Adjoint

function copydiag(f, A::DArray{T, 2}) where T
    Ac = A.chunks
    Ac_copy = Matrix{Any}(undef, size(Ac, 2), size(Ac, 1))
    _copytile(f, Ac) = copy(f(Ac))
    for i in 1:size(Ac, 1), j in 1:size(Ac, 2)
        Ac_copy[j, i] = Dagger.@spawn _copytile(f, Ac[i, j])
    end
    return DArray(T, ArrayDomain(1:size(A,2), 1:size(A,1)), domainchunks(A)', Ac_copy, A.partitioning)
end
Base.fetch(A::Adjoint{T, <:DArray{T, 2}}) where T = copydiag(Adjoint, parent(A))
Base.fetch(A::Transpose{T, <:DArray{T, 2}}) where T = copydiag(Transpose, parent(A))
Base.copy(A::Adjoint{T, <:DArray{T, 2}}) where T = fetch(A)
Base.copy(A::Transpose{T, <:DArray{T, 2}}) where T = fetch(A)
Base.collect(A::Adjoint{T, <:DArray{T, 2}}) where T = collect(copy(A))
Base.collect(A::Transpose{T, <:DArray{T, 2}}) where T = collect(copy(A))

# Matrix-(Matrix/Vector) multiply

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

# Bonus method for matrix-vector multiplication
(*)(a::ArrayOp, b::Vector) = _to_darray(MatMul(a,PromotePartition(b)))

function (*)(a::ArrayDomain{2}, b::ArrayDomain{2})
    if size(a, 2) != size(b, 1)
        throw(DimensionMismatch("The domains cannot be multiplied"))
    end
    return ArrayDomain((indexes(a)[1], indexes(b)[2]))
end
function (*)(a::ArrayDomain{2}, b::ArrayDomain{1})
    if size(a, 2) != length(b)
        throw(DimensionMismatch("The domains cannot be multiplied"))
    end
    return ArrayDomain((indexes(a)[1],))
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
    return a
end

struct BinaryComputeOp{F} end
BinaryComputeOp{F}(x::Union{Chunk,DTask}, y::Union{Chunk,DTask}) where F = @spawn F(x, y)
BinaryComputeOp{F}(x, y) where F = F(x, y)

const AddComputeOp = BinaryComputeOp{+}
const MulComputeOp = BinaryComputeOp{*}

# we define our own matmat and matvec multiply
# for computing the new domains and thunks.
function _mul(a::Matrix, b::Matrix; T=eltype(a))
    c = Array{T}(undef, (size(a,1), size(b,2)))
    n = size(a, 2)
    for i=1:size(a,1)
        for j=1:size(b, 2)
            c[i,j] = treereduce(AddComputeOp, map(MulComputeOp, reshape(a[i,:], (n,)), b[:, j]))
        end
    end
    c
end

function _mul(a::Matrix, b::Vector; T=eltype(b))
    c = Array{T}(undef, size(a,1))
    n = size(a,2)
    for i=1:size(a,1)
        c[i] = treereduce(AddComputeOp, map(MulComputeOp, reshape(a[i, :], (n,)), b))
    end
    c
end

function _mul(a::Vector, b::Vector; T=eltype(b))
    @assert length(b) == 1
    [MulComputeOp(x, b[1]) for x in a]
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
    a, stage(ctx, Distribute(d, b))
end

function stage_operands(ctx::Context, m::MatMul, a, b)
    if size(a, 2) != size(b, 1)
        error(DimensionMismatch("Inputs to * have incompatible size"))
    end
    # take the row distribution of a and get b onto that.

    stg_a = stage(ctx, a)
    stg_b = stage(ctx, b)
    promote_distribution(ctx, m, stg_a, stg_b)
end

"An operand which should be distributed as per convenience"
function stage_operands(ctx::Context, ::MatMul, a::ArrayOp, b::PromotePartition{T,1}) where T
    stg_a = stage(ctx, a)
    dmn_a = domain(stg_a)
    dchunks_a = domainchunks(stg_a)
    dmn_b = domain(b.data)
    if size(dmn_a, 2) != size(dmn_b, 1)
        throw(DimensionMismatch("Cannot promote array of domain $(dmn_b) to multiply with an array of size $(dmn_a)"))
    end
    dmn_out = DomainBlocks((1,),(dchunks_a.cumlength[2],))

    stg_a, stage(ctx, Distribute(dmn_out, b.data))
end

function stage_operands(ctx::Context, ::MatMul, a::PromotePartition, b::ArrayOp)

    if size(a, 2) != size(b, 1)
        throw(DimensionMismatch("Cannot promote array of domain $(dmn_b) to multiply with an array of size $(dmn_a)"))
    end
    stg_b = stage(ctx, b)

    ps = domainchunks(stg_b)
    dmn_out = DomainBlocks((1,1),([size(a.data, 1)], ps.cumlength[1],))
    stage(ctx, Distribute(dmn_out, a.data)), stg_b
end

function stage(ctx::Context, mul::MatMul{T,N}) where {T,N}
    a, b = stage_operands(ctx, mul, mul.a, mul.b)
    d = domain(a)*domain(b)
    ET = Base.promote_type(eltype(a), eltype(b))
    # TODO: Pick a better partitioning
    p = ndims(a) == N ? a.partitioning : b.partitioning
    DArray(ET, d, domainchunks(a)*domainchunks(b),
           _mul(chunks(a), chunks(b); T=Any), p)
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
(*)(l::Diagonal, r::ArrayOp) = _to_darray(Scale(PromotePartition(l.diag), r))
scale(l::ArrayOp, r::ArrayOp) = _to_darray(Scale(l, r))

function stage_operand(ctx::Context, ::Scale, a, b::PromotePartition)
    ps = domainchunks(a)
    b_parts = DomainBlocks((1,), (ps.cumlength[1],))
    stage(ctx, Distribute(b_parts, b.data))
end

function stage_operand(ctx::Context, ::Scale, a, b)
    stage(ctx, b)
end

function _scale(l, r)
    res = similar(r, Any)
    for i=1:length(l)
        res[i,:] = map(x->Dagger.spawn((a,b) -> Diagonal(a)*b, l[i], x), r[i,:])
    end
    res
end

function stage(ctx::Context, scal::Scale)
    r = stage(ctx, scal.r)
    l = stage_operand(ctx, scal, r, scal.l)

    @assert size(domain(r), 1) == size(domain(l), 1)

    scal_parts = _scale(chunks(l), chunks(r))
    # TODO: Concrete eltype
    DArray(Any, domain(r), domainchunks(r), scal_parts, r.partitioning)
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
    inp = Any[stage(ctx, x) for x in c.inputs]

    dmns = map(domain, inp)
    dims = [[i == c.axis ? 0 : i for i in size(d)] for d in dmns]
    if !all(map(x -> x == dims[1], dims[2:end]))
        error("Inputs to cat do not have compatible dimensions.")
    end

    dmn = cat(dmns..., dims = c.axis)
    dmnchunks = cumulative_domains(cat(map(domainchunks, inp)..., dims = c.axis))
    thunks = cat(map(chunks, inp)..., dims = c.axis)
    T = promote_type(map(eltype, inp)...)
    # TODO: Select partitioning better
    DArray(T, dmn, dmnchunks, thunks,
           inp[1].partitioning, inp[1].concat)
end

Base.cat(x::ArrayOp, xs::ArrayOp...; dims::Int) =
    _to_darray(Concat(dims, (x, xs...)))

Base.hcat(xs::ArrayOp...) = cat(xs..., dims=2)
Base.vcat(xs::ArrayOp...) = cat(xs..., dims=1)
