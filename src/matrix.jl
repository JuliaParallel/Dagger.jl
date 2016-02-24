abstract ComputePart <: AbstractPart

immutable Distribute <: ComputePart
    node::AbstractPart
    partition::Nullable{PartitionScheme}
end

Distribute(x::AbstractPart) =
    Distribute(x, Nullable{PartitionScheme}())
Distribute(x::Any) =
    Distribute(part(x), Nullable{PartitionScheme}())

function optimal_partition(part)
    optimal_partition(chunktype(part), domain(part))
end

function optimal_partition{T<:DenseArray}(ctx, ::Type{T}, dom)
    elsize = sizeof(eltype(T))*B
    nproc = length(procs(ctx))
    n = nprocs
    chsize = length(dom)*elsize / nproc
    while chsize > 8MB
        n *= 2
        chsize = chsize / 2
    end
    dims = length(indexes(dom))
    b = ceil(Int, pow(chsize/elsize, 1/dims))
    partition(BlockPartition((b, b)), dom)
end

function compute(ctx, d::Distribute)
    p = if isnull(d.partition)
        optimal_partition(ctx, d.x)
    else
        get(d.partition)
    end
end

function distribute(x, chunks)
    xparts = partition(BlockPartition(chunks), domain(x))
    subparts = AbstractChunk[x[c] for c in xparts.children]
    cat(BlockPartition(chunks), chunktype(x), domain(x), subparts)
end

function blockwise(f, x::Cat)
    cat(x.partition, chunktype(x), domain(x), [@par f(c)
        for c in x.children])
end

function elemwise(f, x)
    blockwise(a -> map(f, a), x)
end

function arrayinit(f, size, chunks)
    dmn = DenseDomain(map(x->1:x, size))
    xparts = partition(BlockPartition(chunks), dmn)
    subparts = AbstractChunk[@par 
