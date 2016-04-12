
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
immutable Part{I<:PartIO} <: AbstractPart
    parttype::Type
    domain::Domain
    handle::I
end

domain(c::Part) = c.domain
parttype(c::Part) = c.parttype
function gather(ctx, part::Part)
    # delegate fetching to handle by default.
    gather(ctx, part.handle)
end


### PartIO
include("lib/dumbref.jl")

immutable DistMem <: PartIO
    ref::MemToken
end
gather(ctx, io::DistMem) = fetch(io.ref)

"""
Create a part from a sequential object.
"""
function part(x)
    ref = make_token(x)
    Part(typeof(x), domain(x), DistMem(ref))
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
 - domain: The domain of the combined part and children (`DomainBranch`)
 - partition: The partition scheme used to divide child parts into a big part
 - children: the parts which form the parts of the Cat
"""
type Cat{P<:PartitionScheme} <: AbstractPart
    partition::P
    parttype::Type
    domain::DomainBranch
    parts::AbstractArray
end

domain(c::Cat) = c.domain
parttype(c::Cat) = c.parttype
partition(c::Cat) = c.partition
parts(x::Cat) = x.parts

function gather(ctx, part::Cat)

    cat_data(partition(part),
        part.domain,
        map(c->gather(ctx,c), parts(part)))
end

"""
Concatenate parts according to some partition
"""
cat(p::PartitionScheme, T::Type, d::Domain, parts::AbstractArray) =
        Cat(p, T, d, parts)

"""
`sub` of a `Cat` part returns a `Cat` of sub parts
"""
function sub(c::Cat, d)
    c_parts, subdomains = lookup_parts(parts(c), children(domain(c)), d)
    if length(c_parts) == 1
        return c_parts[1]
    end

    cat(partition(c), parttype(c), DomainBranch(d, subdomains), c_parts)
end

function getdim(vec)
    dim = 0
    for el in vec
        if el != 0 && dim != 0
            @assert dim == el
        elseif el != 0 && dim == 0
            dim = el
        end
    end
    dim
end

Base.(:*)(a::Range, b::Range) = ((last(a) + 1):(last(a)+length(b)))
@generated function cumulative_domains{N}(::Val{N}, arr::Array)
    quote
        Base.@nexprs $N dim->R_dim = cumprod(map(x->indexes(x)[dim], arr), dim)
        Base.@ncall $N map DenseDomain R
    end
end

function extend_dim{T,N}(x::Array{T,N})
    Nd = ndims(x[1])
    if Nd > N
        sz = tuple(size(x)..., [1 for i=1:(Nd-N)]...)
        return Val{Nd}(), reshape(x, sz)
    else
        return Val{N}(), x
    end
end
cumulative_domains(x::Array) = cumulative_domains(extend_dim(x)...)

function lookup_parts{T,N}(parts, part_domains::Array{T,N}, d)
    intersects = map(pd -> intersect(d, pd), part_domains)
    found = map(x -> !isempty(x), intersects)

    sz = ntuple(dim -> getdim(sum(found, dim)), N)
    idxs = find(found)
    interesting_parts = reshape(parts[idxs], sz)
    interesting_domains = reshape(part_domains[idxs], sz)
    intersects2 = reshape(intersects[idxs], sz)
    subparts = map(intersects2, interesting_parts, interesting_domains) do subd, part, dmn
        sub(part, project(dmn, subd))
    end
    subparts, cumulative_domains(map(alignfirst, intersects2))
end
