
export domain, Domain, UnitDomain, project, alignfirst, ArrayDomain

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
@compat abstract type Domain end

#=

"Is a domain empty?"
@unimplemented isempty(d::Domain)

###### Indexing primitives ######

"""
Find the intersection of two domains. For example,

    intersect(ArrayDomain(1:100, 50:100), ArrayDomain(50:150, 1:75))
    # => ArrayDomain((50:100, 50:75))
"""
@unimplemented intersect{D<:Domain}(a::D, b::D)

"""
   project(a::Domain, b::Domain)

Align `b` relative to `a`. For example,

    julia> project(ArrayDomain(11:25, 21:100), ArrayDomain(15:20, 30:40))
    Dagger.ArrayDomain{2}((5:10,10:20))
"""
@unimplemented project{D<:Domain}(d::D, b::D)

"""
   getindex(a::Domain, b::Domain)

Align `a` relative to `b`. For example,

    getindex(ArrayDomain(11:25, 21:100), ArrayDomain(5:10, 10:40))
    DataParallelBase.ArrayDomain{2}((15:20,30:60))
"""
@unimplemented Base.getindex{D<:Domain}(d::D, b::D)

=#

"""
    alignfirst(a)

Make a subdomain a standalone domain. For example,

    alignfirst(ArrayDomain(11:25, 21:100))
    # => ArrayDomain((1:15), (1:80))
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
# and effectively `view` are unsupported for UnitDomain


###### Array Domains ######

if VERSION >= v"0.6.0-dev"
    # TODO: Fix this better!
    immutable ArrayDomain{N} <: Domain
        indexes::NTuple{N, Any}
    end
else
    immutable ArrayDomain{N} <: Domain
        indexes::NTuple{N}
    end
end

ArrayDomain(xs...) = ArrayDomain(xs)
ArrayDomain(xs::Array) = ArrayDomain((xs...,))

indexes(a::ArrayDomain) = a.indexes
chunks{N}(a::ArrayDomain{N}) = DomainBlocks(
    ntuple(i->first(indexes(a)[i]), Val{N}), map(x->[length(x)], indexes(a)))

domain(x::DenseArray) = ArrayDomain(map(l -> 1:l, size(x)))

(==)(a::ArrayDomain, b::ArrayDomain) = indexes(a) == indexes(b)
Base.getindex(arr::AbstractArray, d::ArrayDomain) = arr[indexes(d)...]

function intersect(a::ArrayDomain, b::ArrayDomain)
    if a === b
        return a
    end
    ArrayDomain(map((x, y) -> intersect(x, y), indexes(a), indexes(b)))
end

function project(a::ArrayDomain, b::ArrayDomain)
    map(indexes(a), indexes(b)) do p, q
        q - (first(p) - 1)
    end |> ArrayDomain
end

function getindex(a::ArrayDomain, b::ArrayDomain)
    ArrayDomain(map(getindex, indexes(a), indexes(b)))
end

alignfirst(a::ArrayDomain) =
    ArrayDomain(map(r->1:length(r), indexes(a)))

function size(a::ArrayDomain, dim)
    idxs = indexes(a)
    length(idxs) < dim ? 1 : length(idxs[dim])
end
size(a::ArrayDomain) = map(length, indexes(a))
length(a::ArrayDomain) = prod(size(a))
ndims(a::ArrayDomain) = length(size(a))
isempty(a::ArrayDomain) = length(a) == 0


"The domain of an array is a ArrayDomain"
domain(x::AbstractArray) = ArrayDomain([1:l for l in size(x)])


cat_data(::Type{Any}, dom::Domain, subdomains, ps) =
    cat_data(typeof(ps[1]), dom, subdomains, ps)

function emptyarray{T<:Array}(::Type{T}, dims...)
    T(dims...)
end

function emptyarray{Tv,Ti}(::Type{SparseMatrixCSC{Tv,Ti}}, m,n)
    spzeros(Tv, Ti, m, n)
end

function emptyarray{Tv,Ti}(::Type{SparseVector{Tv,Ti}}, n)
    SparseVector(n, Ti[], Tv[])
end

function cat_data{T<:AbstractArray}(::Type{T}, dom, subdoms, ps)

    if isempty(ps)
        return emptyarray(T, size(dom)...)
    end

    arr = similar(ps[1], size(dom)...)

    for (d, chunk) in zip(subdoms, ps)
        setindex!(arr, chunk, indexes(d)...)
    end
    arr
end

function cat_data{T<:SparseMatrixCSC}(::Type{T}, dom, ps)

    if isempty(ps)
        @assert isempty(dom)
        return spzeros(T.parameters..., size(dom)...)
    end

    m, n = size(chunks(dom))

    psT = Any[ps[j,i] for i=1:size(ps,2), j=1:size(ps,1)]
    hvcat(ntuple(x->n, m), psT...)
end
