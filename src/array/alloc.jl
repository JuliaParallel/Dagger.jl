
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

    subdomains = branch.children
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
