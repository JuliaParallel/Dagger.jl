import Base: cat
using SparseArrays, Random
import SparseArrays: sprand
export partition

mutable struct AllocateArray{T,N} <: ArrayOp{T,N}
    eltype::Type{T}
    f::Function
    domain::ArrayDomain{N}
    domainchunks
    partitioning::AbstractBlocks
end
size(a::AllocateArray) = size(a.domain)

function _cumlength(len, step)
    nice_pieces = div(len, step)
    extra = rem(len, step)
    ps = [step for i=1:nice_pieces]
    cumsum(extra > 0 ? vcat(ps, extra) : ps)
end

function partition(p::AbstractBlocks, dom::ArrayDomain)
    DomainBlocks(map(first, indexes(dom)),
        map(_cumlength, map(length, indexes(dom)), p.blocksize))
end

_alloc_array(f, T, idx, sz) = f(idx, T, sz)
function stage(ctx, a::AllocateArray)
    thunks = [Dagger.@spawn _alloc_array(a.f, a.eltype, i, size(x)) for (i, x) in enumerate(a.domainchunks)]
    return DArray(a.eltype, a.domain, a.domainchunks, thunks, a.partitioning)
end

struct _AllocArrayNoIdx{F,T} <: Function end
(::_AllocArrayNoIdx{F,T})(_, _, sz...) where {F,T} = F(T, sz...)

function _alloc_array_noidx(f, p::Blocks, ::Type{ET}, dims) where ET
    d = ArrayDomain(map(x->1:x, dims))
    inner_f = _AllocArrayNoIdx{f,ET}()
    a = AllocateArray(ET, inner_f, d, partition(p, d), p)
    return _to_darray(a)
end
Base.rand(p::Blocks, eltype::Type, dims) = _alloc_array_noidx(rand, p, eltype, dims)
Base.rand(p::Blocks, t::Type, dims::Integer...) = rand(p, t, dims)
Base.rand(p::Blocks, dims::Integer...) = rand(p, Float64, dims)
Base.rand(p::Blocks, dims::Tuple) = rand(p, Float64, dims)

Base.randn(p::Blocks, eltype::Type, dims) = _alloc_array_noidx(randn, p, eltype, dims)
Base.randn(p::Blocks, t::Type, dims::Integer...) = randn(p, t, dims)
Base.randn(p::Blocks, dims::Integer...) = randn(p, dims)
Base.randn(p::Blocks, dims::Tuple) = randn(p, Float64, dims)

Base.ones(p::Blocks, eltype::Type, dims) = _alloc_array_noidx(ones, p, eltype, dims)
Base.ones(p::Blocks, t::Type, dims::Integer...) = ones(p, t, dims)
Base.ones(p::Blocks, dims::Integer...) = ones(p, Float64, dims)
Base.ones(p::Blocks, dims::Tuple) = ones(p, Float64, dims)

Base.zeros(p::Blocks, eltype::Type, dims) = _alloc_array_noidx(zeros, p, eltype, dims)
Base.zeros(p::Blocks, t::Type, dims::Integer...) = zeros(p, t, dims)
Base.zeros(p::Blocks, dims::Integer...) = zeros(p, Float64, dims)
Base.zeros(p::Blocks, dims::Tuple) = zeros(p, Float64, dims)

function Base.zero(x::DArray{T,N}) where {T,N}
    dims = ntuple(i->x.domain.indexes[i].stop, N)
    sd = first(x.subdomains)
    part_size = ntuple(i->sd.indexes[i].stop, N)
    a = zeros(Blocks(part_size...), T, dims)
    return _to_darray(a)
end

function Base.view(A::AbstractArray{T,N}, p::Blocks{N}) where {T,N}
    d = ArrayDomain(Base.index_shape(A))
    dc = partition(p, d)
    # N.B. We use `tochunk` because we only want to take the view locally, and
    # taking views should be very fast
    chunks = [tochunk(view(A, x.indexes...)) for x in dc]
    return DArray(T, d, dc, chunks, p)
end

function sprand(p::Blocks, m::Integer, n::Integer, sparsity::Real)
    s = rand(UInt)
    f = function (idx, t,sz)
        sprand(MersenneTwister(s+idx), sz...,sparsity)
    end
    d = ArrayDomain((1:m, 1:n))
    a = AllocateArray(Float64, f, d, partition(p, d), p)
    return _to_darray(a)
end

function sprand(p::Blocks, n::Integer, sparsity::Real)
    s = rand(UInt)
    f = function (idx,t,sz)
        sprand(MersenneTwister(s+idx), sz...,sparsity)
    end
    a = AllocateArray(Float64, f, d, partition(p, ArrayDomain((1:n,))), p)
    return _to_darray(a)
end
