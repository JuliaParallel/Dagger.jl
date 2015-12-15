export cutdim, BCast

immutable CutDim{d} <: AbstractPartition end
cutdim(n) = CutDim{n}()

function index_splits(len, parts)
    starts = len >= parts ?
        round(Int, linspace(1, len+1, parts+1)) :
        [[1:(len+1);], zeros(Int, parts-len);]

    map((x,y) -> x:y, starts[1:end-1], starts[2:end] .- 1)
end

function slice_indexes{d}(ctx, dims::Tuple, ::CutDim{d}, targets)
    # Slice an array along a dimension
    dimlen = dims[d] # Length of the dimension
    parts = length(targets)

    ranges = index_splits(dimlen, parts)
    idxs = [1:len for len in dims]
    chunks = Array(Any, parts)

    [begin
        chunkidx = copy(idxs)
        chunkidx[d] = ranges[i]
        chunkidx
     end for i in 1:parts]
end

function slice{d}(ctx, arr::AbstractArray, p::CutDim{d}, targets)
    # Slice an array along a dimension
    [getindex(arr, idx...) for idx in slice_indexes(ctx, size(arr), p, targets)]
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
