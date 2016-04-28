
export domain, Domain, UnitDomain, project, alignfirst, DenseDomain

import Base: isempty, getindex, intersect,
             ==, size, length

###### Domain ######

"""
The domain represents the set of all indexes of an object.

The basic means of distribution in ComputeFramework is
to partition the domain of an object into smaller subdomains
which can then be stored in many memory locations or processed
on different processing units.
"""
abstract Domain

"Is a domain empty?"
@unimplemented isempty(d::Domain)

###### Indexing primitives ######

"""
Find the intersection of two domains. For example,

    intersect(DenseDomain(1:100, 50:100), DenseDomain(50:150, 1:75))
    #=> DenseDomain((50:100, 50:75))
"""
@unimplemented intersect{D<:Domain}(a::D, b::D)

"""
   project(a::Domain, b::Domain)

Align `b` relative to `a`. For example,

    project(DenseDomain(15:20, 30:40), DenseDomain(11:25, 21:100)
    # => DenseDomain{2}((-3:11,-8:71))
"""
@unimplemented project{D<:Domain}(d::D, b::D)

"""
   getindex(a::Domain, b::Domain)

Align `a` relative to `b`. For example,

    getindex(DenseDomain(11:25, 21:100), DenseDomain(5:10, 10:40))
    DataParallelBase.DenseDomain{2}((15:20,30:60))
"""
@unimplemented Base.getindex{D<:Domain}(d::D, b::D)

"""
    alignfirst(a)

Make a subdomain a standalone domain. For example,

    alignfirst(DenseDomain(11:25, 21:100))
    # => DenseDomain((1:15), (1:80))
"""
@unimplemented alignfirst(a::Domain)

"""
Domain of a scalar value
"""
immutable UnitDomain <: Domain end

"""
    domain(x::T)

The domain is the range of indexes/keys of an object.
The domain is partitioned when you distribute an object.

`domain(x)` returns the domain of `x`.

when an object is partitioned, subsets of the domain are
assigned to different processes or files.

see also `partition`.
"""
function domain end

"""
If no `domain` method is defined on an object, then
we use the `UnitDomain` on it. A `UnitDomain` is indivisible.
"""
domain(x::Any) = UnitDomain()

isempty(::UnitDomain) = false
# intersect, project and getindex are unsupported,
# and effectively `sub` are unsupported for UnitDomain


"""
A domain split into sub-domains
"""
immutable DomainBranch{D<:Domain} <: Domain
    head::D
    children::AbstractArray
end
head(b::DomainBranch) = b.head

isempty(a::DomainBranch) = isempty(head(a))
intersect{D<:Domain}(a::DomainBranch{D}, b::D) = intersect(head(a), b)
project{D<:Domain}(a::DomainBranch{D}, b::D) = intersect(head(a), b)
getindex{D<:Domain}(a::DomainBranch{D}, b::D) = getindex(head(a), b)
children(x::DomainBranch) = x.children



###### Array Domains ######

abstract ArrayDomain{N} <: Domain

@unimplemented indexes(a::ArrayDomain)

###### Domain for DenseArray ######

immutable DenseDomain{N} <: ArrayDomain{N}
    indexes::NTuple{N}
end
DenseDomain(xs...) = DenseDomain(xs)
DenseDomain(xs::Array) = DenseDomain((xs...,))

indexes(a::DenseDomain) = a.indexes
children(a::DenseDomain) = a

domain(x::DenseArray) = DenseDomain(map(l -> 1:l, size(x)))

(==)(a::ArrayDomain, b::ArrayDomain) = indexes(a) == indexes(b)
Base.getindex(arr::AbstractArray, d::ArrayDomain) = arr[indexes(d)...]

function intersect(a::ArrayDomain, b::ArrayDomain)
    if a === b
        return a
    end
    DenseDomain(map((x, y) -> intersect(x, y), indexes(a), indexes(b)))
end

function project(a::ArrayDomain, b::ArrayDomain)
    map(indexes(a), indexes(b)) do p, q
        q - (first(p) - 1)
    end |> DenseDomain
end

getindex(a::ArrayDomain, b::ArrayDomain) =
    DenseDomain(map(getindex, indexes(a), indexes(b)))

alignfirst(a::ArrayDomain) =
    DenseDomain(map(r->1:length(r), indexes(a)))

function alignfirst{T<:ArrayDomain}(a::DomainBranch{T})
    h = alignfirst(head(a))
    cdren = map(alignfirst, children(a))
    DomainBranch(h, cumulative_domains(cdren))
end

# Some utility functions specific to domains of arrays
size(a::ArrayDomain) = map(length, indexes(a))
function size(a::ArrayDomain, dim)
    idxs = indexes(a)
    length(idxs) > dim ? 1 : length(idxs[dim])
end
length(a::ArrayDomain) = prod(size(a))
ndims(a::ArrayDomain) = length(size(a))
isempty(a::DenseDomain) = length(a) == 0


indexes{T<:ArrayDomain}(a::DomainBranch{T}) = indexes(a.head)
size{T<:ArrayDomain}(a::DomainBranch{T}, dim...) = size(a.head, dim...)
length{T<:ArrayDomain}(a::DomainBranch{T}) = prod(size(a))
ndims{T<:ArrayDomain}(a::DomainBranch{T}) = length(size(a))

"The domain of an array is a DenseDomain"
domain(x::AbstractArray) = DenseDomain([1:l for l in size(x)])
