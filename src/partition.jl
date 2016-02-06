###### Partitioning scheme definitions ######

export partition_domain,
       SliceDimension

"""
A PartitionScheme defines how data is partitioned. It is used
by the `partition` function to slice up a sequential data
and `cat(context, p::PartitionScheme, a, b)` to put the slices
back together.

see `partition` and `gather`
"""
abstract PartitionScheme

## Arrays

"""
Slice an AbstractArray along a given dimension.
"""
immutable SliceDimension{D} <: PartitionScheme end
SliceDimension(n) = SliceDimension{n}()

typealias Columnwise SliceDimension{2}
typealias Rowwise SliceDimension{1}

## General schemes

"""
Broadcast layout denotes putting the whole data
on each processing unit.
"""
immutable Broadcast <: PartitionScheme end

###### Domain partitioning routines ######

"""
    partition_domain(ctx::Context, domain, dist::PartitionScheme)

Given the Context `ctx` containing list of processors
and other configuration, partitions the `domain` to correspond
to many processes according to the `dist` data distribution.

returns a `DomainSplit` object.

see also `distribute`. Note that by default `distribute` calls
`partition_domain` on the domain of the input.
"""
function partition_domain end

function partition_domain{d}(ctx, dom, ::SliceDimension{d})
    # Slice an array along a dimension

    dimrange = dom[d] # Range along sliced dimension
    targets = chunk_targets(ctx)
    nparts = length(targets)

    ranges = split_range(dimrange, nparts)
    chunks = Array(Any, nparts)

    dom_array = [d for d in dom]

    DomainSplit(dom, [begin
        chunkidx = copy(dom_array)
        chunkidx[d] = ranges[i]
        chunkidx
     end for i in 1:nparts])
end

function partition_domain(ctx, x, b::Broadcast)
    d = domain(x)
    [d for _ in 1:length(chunk_targets(ctx))]
end

