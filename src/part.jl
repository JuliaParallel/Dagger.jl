
import Base: sub
export part, gather

"""
A Part is a recipe to read an object. The Part type holds information about the
domain spanned by the part on its own and metadata to read the part from its
memory / storage / network location.

`gather(reader, handle)` will bring the data to memory on the caller
"""
abstract AbstractPart

"""
    gather(context, part::AbstractPart)

Get the data stored in a part
"""
function gather end


###### PartSpec ######

abstract PartIO
"""
A part with some data
"""
immutable PartSpec{I<:PartIO} <: AbstractPart
    parttype::Type
    domain::Domain
    handle::I
end

domain(c::PartSpec) = c.domain
parttype(c::PartSpec) = c.parttype
function gather(ctx, part::PartSpec)
    # delegate fetching to handle by default.
    gather(ctx, part.handle)
end


### PartIO


immutable DistMem <: PartIO
    ref::RemoteChannel
end
gather(ctx, io::DistMem) = fetch(io.ref)

"""
Create a part from a sequential object.
"""
function part(x)
    ref = RemoteChannel()
    put!(ref, x)
    PartSpec(typeof(x), domain(x), DistMem(ref))
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

"""
    `sub(a::PartSpec, d::Domain)`

Returns the `Sub` object which represents a sub part of `a`
"""
function sub(p::PartSpec, d::Domain, T=parttype(p))

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

function gather(ctx, part::Cat)

    cat_data(partition(part),
        part.domain,
        map(c->gather(ctx,c), part.parts))
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
    parts = AbstractPart[]
    subdomains = Domain[]

    l = length(c.domain.children)

    for i in 1:l
        dmn = c.domain.children[i]
        ch = c.parts[i]

        if dmn == d
            return ch
        end

        subd = intersect(d, dmn)
        if !isempty(subd)
            push!(parts, sub(ch, project(dmn, subd)))
            push!(subdomains, subd)
        end
    end

    cat(partition(c), parttype(c), DomainBranch(d, subdomains), parts)
end
