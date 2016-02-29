
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
    partition(p::PartitionScheme, domain::Domain, nparts::Int)

Partitions the `domain` into `nparts` approximately equal parts
according to the `dist` data distribution.

returns a `DomainBranch` object.

see also `distribute`. Note that by default `distribute` calls
`partition_domain` on the domain of the input.
"""
@unimplemented partition(p::PartitionScheme, domain::Domain, nparts::Int)

"""
    cat(p::PartitionScheme, a...)

Put data objects back together as if they were split using a `PartitionScheme`
"""
@unimplemented cat(p::PartitionScheme, t::Type, dom::Domain, parts)


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

"""
Slice an AbstractArray along a given dimension.
"""
immutable SliceDimension{D} <: PartitionScheme end
SliceDimension(n) = SliceDimension{n}()

typealias RowBlock    SliceDimension{1}
typealias ColumnBlock SliceDimension{2}

"""
Concatenate two arrays based on SliceDimension layout
"""
cat{d}(::SliceDimension{d}, a::AbstractArray, b::AbstractArray) = cat(d, a, b)

function partition{d}(::SliceDimension{d}, dom::ArrayDomain, nparts::Int)
    # Slice an array along a dimension

    dimrange = indexes(dom)[d] # Range along sliced dimension

    ranges = split_range(dimrange, nparts)
    chunks = Array(Any, nparts)

    dom_array = [d for d in indexes(dom)]

    DomainBranch(dom, [begin
        chunkidx = copy(dom_array)
        chunkidx[d] = ranges[i]
        DenseDomain(chunkidx)
     end for i in 1:nparts])
end

function partition{d}(p::SliceDimension{d}, dom::ArrayDomain, elsize::Bytes, chsize::Bytes)
    # Slice an array along a dimension

    dimrange = indexes(dom)[d] # Range along sliced dimension
    unit_length = (length(dom) / length(dimrange))

    nunits = length(dom) / unit_length

    unit_size = unit_length * elsize
    if unit_size > chsize
        error("Chunk size is too small to fit a single unit of partitioned data")
    end
    units_per_chunk = floor(Int, chsize / unit_size)
    partition(p, dom, ceil(Int, npieces))
end


#### Block partition
# TODO: have this supersede slicedimension and other partition methods

export BlockPartition

immutable BlockPartition{N} <: PartitionScheme
    blocksize::NTuple{N, Int}
end
BlockPartition(xs...) = BlockPartition(xs)

@generated function partition{N}(p::BlockPartition{N}, dom::ArrayDomain{N})
    sym(n) = symbol("i$n")

    forspec = [:($(sym(i)) = split_range_interval(
            idxs[$i], p.blocksize[$i])) for i=1:N]

    subdmn = Expr(:call, :DenseDomain, [sym(n) for n=1:N]...)
    body = Expr(:comprehension, subdmn, forspec...)
    Expr(:block, :(idxs = indexes(dom)), :(DomainBranch(dom, $body)))
end

function cat_data(p::BlockPartition, dom::DomainBranch, parts::AbstractArray)
    T = eltype(parts[1])
    arr = Array(T, size(dom))
    for (d, part) in zip(dom.children, parts)
        arr[indexes(d)...] = part
    end
    arr
end
