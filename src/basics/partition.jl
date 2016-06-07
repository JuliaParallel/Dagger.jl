
import Base: cat
export partition

"""
A PartitionScheme defines how data is partitioned. It is used
by the `partition` function to slice up sequential data structure
and `cat(p::PartitionScheme, a, b)` to put the slices
back together.

see `partition` and `gather`
"""
abstract PartitionScheme

"""
    partition(p::PartitionScheme, domain::Domain)

Partitions the `domain` into `nparts` approximately equal parts
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

@generated function partition{N}(p::BlockPartition{N}, dom::ArrayDomain{N})
    sym(n) = Symbol("i$n")

    forspec = [:($(sym(i)) = split_range_interval(
            idxs[$i], p.blocksize[$i])) for i=1:N]

    subdmn = Expr(:call, :DenseDomain, [sym(n) for n=1:N]...)
    body = Expr(:comprehension, subdmn, forspec...)
    Expr(:block, :(idxs = indexes(dom)), :(DomainSplit(dom, $body)))
end

