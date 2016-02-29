
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

import Base: transpose

immutable Transpose <: Computation
    input::AbstractPart
end

global _stage_cache = WeakKeyDict()

function cached_stage(ctx, x)
    isimmutable(x) && return stage(ctx, x)
    if haskey(_stage_cache, x)
        _stage_cache[x]
    else
        _stage_cache[x] = stage(ctx, x)
    end
end

transpose(x::AbstractPart) = Thunk(transpose, (x,))
transpose(x::Computation) = Transpose(x)
function transpose(x::DenseDomain{2})
    d = indexes(x)
    DenseDomain(d[2], d[1])
end

function stage(ctx, node::Cat)
    node
end

function stage(ctx, node::Transpose)
    inp = cached_stage(ctx, node.input)
    dmn = domain(inp)
    @assert isa(dmn, DomainBranch)
    dmnT = DomainBranch(head(dmn)', dmn.children')
    Cat(inp.partition, parttype(inp), dmnT, inp.parts')
end



immutable Save <: Computation
    input::AbstractPart
    name::AbstractString
end

function save(p::Computation, name::AbstractString)
    Save(p, name)
end

function stage(ctx, s::Save)
    x = cached_stage(ctx, s.input)
    save_part(p) = save(ctx, part(p), tempname())
    saved_parts = map(x.parts) do p
        Thunk(save_part, (p,))
    end
    function save_cat_meta(parts...)
        f = open(s.name, "w")
        saved_parts = AbstractPart[c for c in parts]
        res = save(ctx, f, x, s.name, saved_parts)
        close(f)
        res
    end

    # The DAG has to block till saving is complete.
    res = Thunk(save_cat_meta, (saved_parts...); meta=true)
end
stage(ctx, x::Thunk) = Thunk
