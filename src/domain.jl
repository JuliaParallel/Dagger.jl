
export domain, UnitDomain, project, alignfirst, ArrayDomain

import Base: isempty, getindex, intersect,
             ==, size, length, ndims

"""
Default domain
"""
immutable UnitDomain end

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


###### Array Domains ######

if VERSION >= v"0.6.0-dev"
    # TODO: Fix this better!
    immutable ArrayDomain{N}
        indexes::NTuple{N, Any}
    end
else
    immutable ArrayDomain{N}
        indexes::NTuple{N}
    end
end

ArrayDomain(xs...) = ArrayDomain(xs)
ArrayDomain(xs::Array) = ArrayDomain((xs...,))

indexes(a::ArrayDomain) = a.indexes
chunks{N}(a::ArrayDomain{N}) = DomainBlocks(
    ntuple(i->first(indexes(a)[i]), Val{N}), map(x->[length(x)], indexes(a)))

(==)(a::ArrayDomain, b::ArrayDomain) = indexes(a) == indexes(b)
Base.getindex(arr::AbstractArray, d::ArrayDomain) = arr[indexes(d)...]

function intersect(a::ArrayDomain, b::ArrayDomain)
    if a === b
        return a
    end
    ArrayDomain(map((x, y) -> _intersect(x, y), indexes(a), indexes(b)))
end

function project(a::ArrayDomain, b::ArrayDomain)
    map(indexes(a), indexes(b)) do p, q
        q - (first(p) - 1)
    end |> ArrayDomain
end

function getindex(a::ArrayDomain, b::ArrayDomain)
    ArrayDomain(map(getindex, indexes(a), indexes(b)))
end

"""
    alignfirst(a)

Make a subdomain a standalone domain. For example,

    alignfirst(ArrayDomain(11:25, 21:100))
    # => ArrayDomain((1:15), (1:80))
"""
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

