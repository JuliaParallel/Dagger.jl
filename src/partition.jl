
import Base: cat
export partition, SliceDimension

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

returns a `DomainBranch` object.

see also `distribute`. Note that by default `distribute` calls
`partition_domain` on the domain of the input.
"""
@unimplemented partition(p::PartitionScheme, domain::Domain)

"""
    cat_data(p::PartitionScheme, a...)

Put data objects back together as if they were split using a `PartitionScheme`
"""
@unimplemented cat_data(p::PartitionScheme, t::Type, dom::Domain, parts)


## General schemes

"""
Broadcast layout denotes putting the whole data
on each processing unit.
"""
immutable Broadcast <: PartitionScheme end

partition(b::Broadcast, dom::Domain, nparts::Int) =
    DomainBranch(dom, [dom for _ in 1:nparts])

cat(p::Broadcast, dom::Domain, a, b) = a


"""
    Reducer(op, indentity)

Reducer layout denotes putting two parts together
by using a reducer operator and an identity value.
"""
immutable Reducer{F} <: PartitionScheme
    op::F
    v0::Any
end
cat(p::Reducer, ::UnitDomain, a, b) = p.op(a,b)
cat(p::Reducer, ::UnitDomain) = p.v0

partition(b::Reducer, dom::Domain, nparts::Int) =
    error("Cannot partition using a reducer")


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
    Expr(:block, :(idxs = indexes(dom)), :(DomainBranch(dom, $body)))
end

function cat_data(p::BlockPartition, dom::DomainBranch, parts::AbstractArray)
    if isa(parts[1], SparseMatrixCSC)
        return sparse_cat_data(parts)
    end
    T = eltype(parts[1])
    arr = Array(T, size(dom))
    for (d, part) in zip(children(dom), parts)
        setindex!(arr, part, indexes(d)...)
    end
    arr
end

function sparse_cat_data(parts)
    hblocks = Any[hcat(parts[i, :]...) for i=1:size(parts,1)]

    vcat(hblocks...)
end
