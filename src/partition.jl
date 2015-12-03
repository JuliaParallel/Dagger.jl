
"""
A partition pattern. Implements `slice` and `gather` methods
"""
abstract AbstractPartition

immutable CutDim{d} <: AbstractPartition end
cutdim(n) = CutDim{n}()

function index_splits(len, parts)
    starts = len >= parts ?
        round(Int, linspace(1, len+1, parts+1)) :
        [[1:(len+1);], zeros(Int, parts-len);]
    map((x,y) -> x:y, starts[1:end-1], starts[2:end] .- 1)
end

function slice{d}(ctx, arr::AbstractArray, ::CutDim{d}, targets)
    # Slice an array along a dimension
    dimlen = size(arr, d) # Length of the dimension
    parts = length(targets)

    ranges = index_splits(dimlen, parts)
    idxs = Array(Any, ndims(arr))
    fill!(idxs, :)
    chunks = Array(Any, length(targets))
    for i in 1:length(targets)
        chunkidx = copy(idxs)
        chunkidx[d] = ranges[i]
        chunks[i] = getindex(arr, chunkidx...)
    end
    chunks
end

function gather{d}(ctx, partition::CutDim{d}, xs::Vector)
    reduce((acc, x) -> cat(d, acc, x), xs[1], xs[2:end])
end


immutable Bcast <: AbstractPartition end

function slice{T}(ctx, x::T, ::Bcast, targets)
    T[x for i in 1:length(targets)]
end

function gather(ctx, ::Bcast, parts)
    return parts
end
