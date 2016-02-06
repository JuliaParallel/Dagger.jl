
export domain,
       hasoverlap,
       alignfirst

###### Domain ######

"""
The domain represents the set of all indexes of an object.

The basic means of distribution in ComputeFramework is
to partition the domain of an object into smaller subdomains
which can then be stored in many memory locations or processed
on different processing units.
"""
abstract Domain

immutable ScalarDomain <: Domain end
immutable ArrayDomain <: Domain
    indexranges::Array{Range}
end

"""
A domain split into sub-domains
"""
immutable DomainBranch <: Domain
    head::Domain
    children::AbstractArray
end

"""
    domain(x::T)

The domain is the range of indexes/keys of an object.
The domain is partitioned when you distribute an object.

returns the domain of `x`.

for example,
1. domain of an array `arr` is `[1:l for l in size(arr)]`
2. domain of a dictionary `dict` is `keys(dict)`

when an object is partitioned, subsets of the domain are
assigned to different processes or files.

see also `partition_domain`.
"""
function domain end

domain(x::Number) = ScalarDomain()
domain(x::AbstractArray) = ArrayDomain([1:l for l in size(x)])


###### Some set algebra on domains ######

"""
Check to see if a given domain is fully inside another.
"""
function Base.in(a::ArrayDomain, b::ArrayDomain)
    if length(a.indexranges) == length(b.indexranges)
        all(map(in, a.indexranges, b.indexranges))
    else
        false
    end
end

function Base.in(a::Domain, b::DomainBranch)
    a in b.head
end

"""
Find the intersection of two domains
"""
function Base.intersect(a::ArrayDomain, b::ArrayDomain)
    map(intersect, a.indexranges, b.indexranges)
end

"""
Check if two ArrayDomains overlap
"""
function hasoverlap(a::ArrayDomain, b::ArrayDomain)
    length(intersect(a,b).indexranges) > 0
end

"""
Make a subdomain into a standalone domain
"""
function alignfirst(a::ArrayDomain)
    ArrayDomain([1:length(r) for r in a.indexranges])
end

# TODO: Add domain for Dicts
