import Base: cat
export partition

type AllocateArray{T,N} <: LazyArray{T,N}
    eltype::Type{T}
    f::Function
    domain::DomainSplit{DenseDomain{N}}
end
size(a::AllocateArray) = size(a.domain)

export BlockPartition, Blocks

immutable Blocks{N}
    blocksize::NTuple{N, Int}
end
Blocks(xs::Int...) = Blocks(xs)

function _cumlength(len, step)
    nice_pieces = div(len, step)
    extra = rem(len, step)
    ps = [step for i=1:nice_pieces]
    cumsum(extra > 0 ? vcat(ps, extra) : ps)
end

function partition(p::Blocks, dom::ArrayDomain)
    ps = BlockedDomains(map(first, indexes(dom)),
        map(_cumlength, map(length, indexes(dom)), p.blocksize))
    DomainSplit(dom, ps)
end

Base.@deprecate BlockPartition Blocks

function stage(ctx, a::AllocateArray)
    branch = a.domain
    dims = length(indexes(a.domain))
    alloc = let eltype = a.eltype, f = a.f
        _alloc(idx, sz) = f(idx,eltype, sz)
    end

    subdomains = chunks(branch)
    thunks = similar(subdomains, Thunk)
    for i=1:length(subdomains)
        thunks[i] = Thunk(alloc, (i, size(subdomains[i])))
    end
    Cat(Array{a.eltype, dims}, branch, thunks)
end

function Base.rand(p::Blocks, eltype::Type, dims)
    s = rand(UInt)
    f = function (idx, x...)
        rand(MersenneTwister(s+idx), x...)
    end
    d = DenseDomain(map(x->1:x, dims))
    AllocateArray(eltype, f, partition(p, d))
end

Base.rand(p::Blocks, t::Type, dims::Integer...) = rand(p, t, dims)
Base.rand(p::Blocks, dims::Integer...) = rand(p, Float64, dims)
Base.rand(p::Blocks, dims::Tuple) = rand(p, Float64, dims)

function Base.randn(p::Blocks, dims)
    s = rand(UInt)
    f = function (idx, x...)
        randn(MersenneTwister(s+idx), x...)
    end
    d = DenseDomain(map(x->1:x, dims))
    AllocateArray(Float64, f, partition(p, d))
end
Base.randn(p::Blocks, dims::Integer...) = randn(p, dims)

function Base.ones(p::Blocks, eltype::Type, dims)
    d = DenseDomain(map(x->1:x, dims))
    AllocateArray(eltype, (_, x...) -> ones(x...), partition(p, d))
end
Base.ones(p::Blocks, t::Type, dims::Integer...) = ones(p, t, dims)
Base.ones(p::Blocks, dims::Integer...) = ones(p, Float64, dims)
Base.ones(p::Blocks, dims::Tuple) = ones(p, Float64, dims)

function Base.zeros(p::Blocks, eltype::Type, dims)
    AllocateArray(eltype, (_, x...) -> zeros(x...), DenseDomain(map(x->1:x, dims)), p)
end
Base.zeros(p::Blocks, t::Type, dims::Integer...) = zeros(p, t, dims)
Base.zeros(p::Blocks, dims::Integer...) = zeros(p, Float64, dims)
Base.zeros(p::Blocks, dims::Tuple) = zeros(p, Float64, dims)

function Base.sprand(p::Blocks, m::Integer, n::Integer, sparsity::Real)
    s = rand(UInt)
    f = function (idx, t,sz)
        sprand(MersenneTwister(s+idx), sz...,sparsity)
    end
    AllocateArray(Float64, f, partition(p, DenseDomain((1:m, 1:n))))
end

function Base.sprand(p::Blocks, n::Integer, sparsity::Real)
    s = rand(UInt)
    f = function (idx,t,sz)
        sprand(MersenneTwister(s+idx), sz...,sparsity)
    end
    AllocateArray(Float64, f, partition(p, DenseDomain((1:n,))))
end
