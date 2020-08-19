import Base: cat
using SparseArrays, Random
import SparseArrays: sprand
export partition

mutable struct AllocateArray{T,N} <: ArrayOp{T,N}
    eltype::Type{T}
    f::Function
    domain::ArrayDomain{N}
    domainchunks
end
size(a::AllocateArray) = size(a.domain)

function _cumlength(len, step)
    nice_pieces = div(len, step)
    extra = rem(len, step)
    ps = [step for i=1:nice_pieces]
    cumsum(extra > 0 ? vcat(ps, extra) : ps)
end

function partition(p::Blocks, dom::ArrayDomain)
    DomainBlocks(map(first, indexes(dom)),
        map(_cumlength, map(length, indexes(dom)), p.blocksize))
end

function stage(ctx, a::AllocateArray)
    alloc(idx, sz) = a.f(idx, a.eltype, sz)
    thunks = [delayed(alloc)(i, size(x)) for (i, x) in enumerate(a.domainchunks)]
    DArray(a.eltype,a.domain, a.domainchunks, thunks)
end

function Base.rand(p::Blocks, eltype::Type, dims)
    s = rand(UInt)
    f = function (idx, x...)
        rand(MersenneTwister(s+idx), x...)
    end
    d = ArrayDomain(map(x->1:x, dims))
    AllocateArray(eltype, f, d, partition(p, d))
end

Base.rand(p::Blocks, t::Type, dims::Integer...) = rand(p, t, dims)
Base.rand(p::Blocks, dims::Integer...) = rand(p, Float64, dims)
Base.rand(p::Blocks, dims::Tuple) = rand(p, Float64, dims)

function Base.randn(p::Blocks, dims)
    s = rand(UInt)
    f = function (idx, x...)
        randn(MersenneTwister(s+idx), x...)
    end
    d = ArrayDomain(map(x->1:x, dims))
    AllocateArray(Float64, f, d, partition(p, d))
end
Base.randn(p::Blocks, dims::Integer...) = randn(p, dims)

function Base.ones(p::Blocks, eltype::Type, dims)
    d = ArrayDomain(map(x->1:x, dims))
    AllocateArray(eltype, (_, x...) -> ones(x...), d, partition(p, d))
end
Base.ones(p::Blocks, t::Type, dims::Integer...) = ones(p, t, dims)
Base.ones(p::Blocks, dims::Integer...) = ones(p, Float64, dims)
Base.ones(p::Blocks, dims::Tuple) = ones(p, Float64, dims)

function Base.zeros(p::Blocks, eltype::Type, dims)
    d = ArrayDomain(map(x->1:x, dims))
    AllocateArray(eltype, (_, x...) -> zeros(x...), d, partition(p, d))
end
Base.zeros(p::Blocks, t::Type, dims::Integer...) = zeros(p, t, dims)
Base.zeros(p::Blocks, dims::Integer...) = zeros(p, Float64, dims)
Base.zeros(p::Blocks, dims::Tuple) = zeros(p, Float64, dims)

function sprand(p::Blocks, m::Integer, n::Integer, sparsity::Real)
    s = rand(UInt)
    f = function (idx, t,sz)
        sprand(MersenneTwister(s+idx), sz...,sparsity)
    end
    d = ArrayDomain((1:m, 1:n))
    AllocateArray(Float64, f, d, partition(p, d))
end

function sprand(p::Blocks, n::Integer, sparsity::Real)
    s = rand(UInt)
    f = function (idx,t,sz)
        sprand(MersenneTwister(s+idx), sz...,sparsity)
    end
    AllocateArray(Float64, f, d, partition(p, ArrayDomain((1:n,))))
end
