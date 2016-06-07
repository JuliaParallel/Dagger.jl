
type AllocateArray <: Computation
    eltype::Type
    f::Function
    domain::DenseDomain
    partition::PartitionScheme
end

function stage(ctx, a::AllocateArray)
    branch = partition(a.partition, a.domain)
    dims = length(indexes(a.domain))
    alloc = let eltype = a.eltype, f = a.f
        _alloc(sz) = f(eltype, sz)
    end

    subdomains = parts(branch)
    thunks = similar(subdomains, Thunk)
    for i=eachindex(subdomains)
        thunks[i] = Thunk(alloc, (size(subdomains[i]),))
    end
    cat(a.partition, Array{a.eltype, dims}, branch, thunks)
end

function Base.rand(p::PartitionScheme, eltype::Type, dims)
    AllocateArray(eltype, rand, DenseDomain(map(x->1:x, dims)), p)
end

Base.rand(p::PartitionScheme, t::Type, dims::Integer...) = rand(p, t, dims)
Base.rand(p::PartitionScheme, dims::Integer...) = rand(p, Float64, dims)
Base.rand(p::PartitionScheme, dims::Tuple) = rand(p, Float64, dims)

function Base.randn(p::PartitionScheme, dims)
    AllocateArray(Float64, (t,sz)->randn(sz), DenseDomain(map(x->1:x, dims)), p)
end
Base.randn(p::PartitionScheme, dims::Integer...) = randn(p, dims)

function Base.ones(p::PartitionScheme, eltype::Type, dims)
    AllocateArray(eltype, ones, DenseDomain(map(x->1:x, dims)), p)
end
Base.ones(p::PartitionScheme, t::Type, dims::Integer...) = ones(p, t, dims)
Base.ones(p::PartitionScheme, dims::Integer...) = ones(p, Float64, dims)
Base.ones(p::PartitionScheme, dims::Tuple) = ones(p, Float64, dims)

function Base.zeros(p::PartitionScheme, eltype::Type, dims)
    AllocateArray(eltype, zeros, DenseDomain(map(x->1:x, dims)), p)
end
Base.zeros(p::PartitionScheme, t::Type, dims::Integer...) = zeros(p, t, dims)
Base.zeros(p::PartitionScheme, dims::Integer...) = zeros(p, Float64, dims)
Base.zeros(p::PartitionScheme, dims::Tuple) = zeros(p, Float64, dims)


function Base.sprand(p::PartitionScheme, m::Integer, n::Integer, sparsity::Real)
    AllocateArray(Float64, (t,sz) -> sprand(sz...,sparsity), DenseDomain((1:m, 1:n)), p)
end

function Base.sprand(p::PartitionScheme, n::Integer, sparsity::Real)
    AllocateArray(Float64, (t,sz) -> sprand(sz...,sparsity), DenseDomain((1:n,)), p)
end
