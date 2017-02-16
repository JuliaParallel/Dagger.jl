
import Base: cat
export partition

"""
A PartitionScheme defines how data is partitioned. It is used
by the `partition` function to slice up sequential data structure
and `cat(p::PartitionScheme, a, b)` to put the slices
back together.

see `partition` and `gather`
"""
@compat abstract type PartitionScheme end

"""
    partition(p::PartitionScheme, domain::Domain)

Partitions the `domain` into `nparts` approximately equal chunks
according to the `dist` data distribution.

returns a `DomainSplit` object.

see also `distribute`. Note that by default `distribute` calls
`partition_domain` on the domain of the input.
"""
@unimplemented partition(p::PartitionScheme, domain::Domain)

## General schemes

"""
Broadcast layout denotes putting the whole data
on each processing unit.
"""
immutable Broadcast <: PartitionScheme end

partition(b::Broadcast, dom::Domain, nparts::Int) =
    DomainSplit(dom, [dom for _ in 1:nparts])

###### Array Partitioning ######

#### Block partition

export BlockPartition

immutable BlockPartition{N} <: PartitionScheme
    blocksize::NTuple{N, Int}
end
BlockPartition(xs...) = BlockPartition(xs)

function _cumlength(len, step)
    nice_pieces = div(len, step)
    extra = rem(len, step)
    ps = [step for i=1:nice_pieces]
    cumsum(extra > 0 ? vcat(ps, extra) : ps)
end

include("../lib/blocked-domains.jl")

function partition(p::BlockPartition, dom::ArrayDomain)
    ps = BlockedDomains(map(first, indexes(dom)),
        map(_cumlength, map(length, indexes(dom)), p.blocksize))
    DomainSplit(dom, ps)
end
