export stage, compute

abstract ComputeNode <: AbstractPart

compute(ctx, x::ComputeNode) = compute(ctx, stage(ctx, x))

#=
function compute(ctx, x::Cat)
    Cat(x.partition, x.parttype, x.domain, compute(ctx, x.children))
end

function compute(ctx, x::Sub)
    Sub(parttype(x), domain(x), compute(ctx, x.chunk))
end
=#

immutable AllocateArray <: ComputeNode
    eltype::Type
    f::Function
    domain::DenseDomain
    partition::PartitionScheme
end

function stage(ctx, a::AllocateArray)
    branch = partition(a.partition, a.domain)
    dims = length(indexes(a.domain))
    alloc = let eltype = a.eltype, f = a.f
        sz -> f(eltype, sz)
    end

    subdomains = branch.children
    thunks = similar(subdomains, Thunk)
    for i=eachindex(subdomains)
        thunks[i] = Thunk(alloc, size(subdomains[i]))
    end
    Cat(a.partition, Array{a.eltype, dims}, branch, thunks)
end

function Base.rand(p::PartitionScheme, eltype::Type, dims)
    AllocateArray(eltype, rand, DenseDomain(map(x->1:x, dims)), p)
end

import Base: transpose

immutable Transpose <: ComputeNode
    input::AbstractPart
end

transpose(x::AbstractPart) = Transpose(x)
transpose(x::Thunk) = Thunk(transpose, x)
function transpose(x::DenseDomain{2})
    d = indexes(x)
    DenseDomain(d[2], d[1])
end

function stage(ctx, node::Transpose)
    inp = stage(ctx, node.input)
    dmn = domain(inp)
    @assert isa(dmn, DomainBranch)
    dmnT = DomainBranch(head(dmn)', dmn.children')
    Cat(inp.partition, parttype(inp), dmnT, inp.children')
end
