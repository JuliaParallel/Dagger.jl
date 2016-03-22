
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

function Base.ones(p::PartitionScheme, eltype::Type, dims)
    AllocateArray(eltype, ones, DenseDomain(map(x->1:x, dims)), p)
end
