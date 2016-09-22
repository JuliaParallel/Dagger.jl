
export domain, Domain, UnitDomain, project, alignfirst, DenseDomain

import Base: isempty, getindex, intersect,
             ==, size, length, ndims

###### Domain ######

"""
The domain represents the set of all indexes of an object.

The basic means of distribution in Dagger is
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

    julia> project(DenseDomain(11:25, 21:100), DenseDomain(15:20, 30:40))
    Dagger.DenseDomain{2}((5:10,10:20))
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
alignfirst(a::Domain) = a

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
immutable DomainSplit{D<:Domain} <: Domain
    head::D
    parts::AbstractArray
end
head(b::DomainSplit) = b.head

isempty(a::DomainSplit) = isempty(head(a))
intersect{D<:Domain}(a::DomainSplit{D}, b::D) = intersect(head(a), b)
project{D<:Domain}(a::DomainSplit{D}, b::D) = intersect(head(a), b)
getindex{D<:Domain}(a::DomainSplit{D}, b::D) = getindex(head(a), b)
parts(x::DomainSplit) = x.parts



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
parts{N}(a::DenseDomain{N}) = BlockedDomains(
    ntuple(i->first(indexes(a)[i]), Val{N}), map(x->[length(x)], indexes(a)))

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

function size(a::ArrayDomain, dim)
    idxs = indexes(a)
    length(idxs) < dim ? 1 : length(idxs[dim])
end
size(a::ArrayDomain) = map(length, indexes(a))
length(a::ArrayDomain) = prod(size(a))
ndims(a::ArrayDomain) = length(size(a))
isempty(a::DenseDomain) = length(a) == 0


indexes{T<:ArrayDomain}(a::DomainSplit{T}) = indexes(a.head)
size{T<:ArrayDomain}(a::DomainSplit{T}, dim...) = size(a.head, dim...)
length{T<:ArrayDomain}(a::DomainSplit{T}) = prod(size(a))
ndims{T<:ArrayDomain}(a::DomainSplit{T}) = ndims(head(a))

"The domain of an array is a DenseDomain"
domain(x::AbstractArray) = DenseDomain([1:l for l in size(x)])


Base.@deprecate_binding DomainBranch DomainSplit
Base.@deprecate children(x::Domain) parts(x)

cat_data(::Type{Any}, dom::DomainSplit, ps) =
    cat_data(typeof(ps[1]), dom, ps)

function cat_data{T<:AbstractArray}(::Type{T}, dom, ps)

    arr = Array(eltype(T), size(dom))

    if isempty(ps)
        @assert isempty(dom)
        return arr
    end

    for (d, part) in zip(parts(dom), ps)
        setindex!(arr, part, indexes(d)...)
    end
    arr
end

function cat_data{T<:SparseMatrixCSC}(::Type{T}, dom, ps)

    if isempty(ps)
        @assert isempty(dom)
        return spzeros(T.parameters..., size(dom)...)
    end

    m, n = size(parts(dom))

    psT = Any[ps[j,i] for i=1:size(ps,2), j=1:size(ps,1)]
    hvcat(ntuple(x->n, m), psT...)
end
