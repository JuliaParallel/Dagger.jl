
import Base: sub
export part, gather

"""
A Part is a recipe to read an object. The Part type holds information about the
domain spanned by the part on its own and metadata to read the part from its
memory / storage / network location.

`gather(reader, handle)` will bring the data to memory on the caller
"""
abstract AbstractPart

parts(x::AbstractPart) = x

"""
    gather(context, part::AbstractPart)

Get the data stored in a part
"""
function gather end


###### Part ######

abstract PartIO
"""
A part with some data
"""
type Part{I<:PartIO} <: AbstractPart
    parttype::Type
    domain::Domain
    handle::I
    persist::Bool
end

domain(c::Part) = c.domain
parttype(c::Part) = c.parttype
persist!(t::Part) = (t.persist=true; t)
shouldpersist(p::Part) = t.persist
function gather(ctx, part::Part)
    # delegate fetching to handle by default.
    gather(ctx, part.handle)
end


### PartIO
include("../lib/dumbref.jl")

immutable DistMem <: PartIO
    ref::MemToken
end
gather(ctx, io::DistMem) = fetch(io.ref)

"""
Create a part from a sequential object.
"""
function part(x)
    ref = make_token(x)
    Part(typeof(x), domain(x), DistMem(ref), false)
end
part(x::AbstractPart) = x


"""
A **view** into an AbstractPart

Fields:
 - domain: The domain of this part on its own
 - subdomain: The subdomain viewd in `part`
 - part: The part being viewed
"""
type Sub{T<:AbstractPart} <: AbstractPart
    parttype::Type
    domain::Domain
    subdomain::Domain
    part::T
end

domain(c::Sub) = c.domain
parttype(c::Sub) = c.parttype
persist!(x::Sub) = persist!(x.part)

function gather(ctx, s::Sub)
    # A Sub{T<:Chunk{X}} can try to make this efficient for X
    gather(ctx, s.part)[s.subdomain]
end
# optimized subindexing on DistMem
function gather(ctx, s::Sub{Part{DistMem}})
    ref = s.part.handle.ref
    pid = ref.where
    let d = s.subdomain
        remotecall_fetch(x -> fetch(x)[d], pid, ref)
    end
end

"""
    `sub(a::Part, d::Domain)`

Returns the `Sub` object which represents a sub part of `a`
"""
function sub(p::Part, d::Domain, T=parttype(p))

    if domain(p) == d
        return p
    end

    Sub(T, alignfirst(d), d, p)
end
Base.getindex(x::AbstractPart, idx::Domain) = sub(x, idx)

function sub(s::Sub, d)
    dprime = s.subdomain[d] # collapse subindex
    sub(s.part, dprime)
end

"""
A collection of Parts put together to form a bigger logical part

Fields:
 - parttype: The type of the data represented by the Cat
 - domain: The domain of the combined part and parts (`DomainSplit`)
 - parts: the parts which form the parts of the Cat
"""
type Cat <: AbstractPart
    parttype::Type
    domain::DomainSplit
    parts::AbstractArray
end

domain(c::Cat) = c.domain
parttype(c::Cat) = c.parttype
parts(x::Cat) = x.parts
persist!(x::Cat) = (for p in parts(x); persist!(p); end)

function gather(ctx, part::Cat)
    cat_data(parttype(part), part.domain, map(c->(gather(ctx,c)), parts(part)))
end

"""
`sub` of a `Cat` part returns a `Cat` of sub parts
"""
function sub(c::Cat, d)
    sub_parts, subdomains = lookup_parts(parts(c), parts(domain(c)), d)
    if length(sub_parts) == 1
        sub_parts[1]
    else
        Cat(parttype(c), DomainSplit(alignfirst(d), subdomains), sub_parts)
    end
end

function group_indices(cumlength, idxs,at=1, acc=Any[])
    at > length(idxs) && return acc
    f = idxs[at]
    fidx = searchsortedfirst(cumlength, f)
    current_block = (get(cumlength, fidx-1,0)+1):cumlength[fidx]
    start_at = at
    end_at = at
    for i=(at+1):length(idxs)
        if idxs[i] in current_block
            end_at += 1
            at += 1
        else
            break
        end
    end
    push!(acc, fidx=>idxs[start_at:end_at])
    group_indices(cumlength, idxs, at+1, acc)
end

function group_indices(cumlength, idx::Int)
    group_indices(cumlength, [idx])
end

function group_indices(cumlength, idxs::Range)
    f = searchsortedfirst(cumlength, first(idxs))
    l = searchsortedfirst(cumlength, last(idxs))
    out = cumlength[f:l]
    out[end] = last(idxs)
    out-=(f-1)
    map(=>, f:l, map(UnitRange, vcat(first(idxs), out[1:end-1]+1), out))
end

_cumsum(x::AbstractArray) = length(x) == 0 ? Int[] : cumsum(x)
function lookup_parts{N}(ps::AbstractArray, subdmns::BlockedDomains{N}, d::DenseDomain{N})
    groups = map(group_indices, subdmns.cumlength, indexes(d))
    sz = map(length, groups)
    pieces = Array(AbstractPart, sz)
    for i = CartesianRange(sz)
        idx_and_dmn = map(getindex, groups, i.I)
        idx = map(x->x[1], idx_and_dmn)
        dmn = DenseDomain(map(x->x[2], idx_and_dmn))
        pieces[i] = sub(ps[idx...], project(subdmns[idx...], dmn))
    end
    out_cumlength = map(g->_cumsum(map(x->length(x[2]), g)), groups)
    out_dmn = BlockedDomains(ntuple(x->1,Val{N}), out_cumlength)
    pieces, out_dmn
end

function free!(x::Cat, force=true)
    for p in parts(x)
        free!(p, force)
    end
end
# Check to see if the node is set to persist
# if it is foce can override it
function free!(s::Part{DistMem}, force=true)
    if force || !s.persist
        release_token(s.handle.ref)
    end
end
free!(s::AbstractPart, force=true) = nothing
free!(s::Sub, force=true) = nothing
