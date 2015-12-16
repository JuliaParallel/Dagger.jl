export cutdim, BCast
 
immutable CutDimension{d} <: AbstractLayout end
typealias ColumnLayout CutDimension{2}
typealias RowLayout CutDimension{1}

"""
Cut an array along a given dimension
"""
cutdim(n) = CutDimension{n}()

function index_splits(len, parts)
    starts = len >= parts ?
        round(Int, linspace(1, len+1, parts+1)) :
        [[1:(len+1);], zeros(Int, parts-len);]

    map((x,y) -> x:y, starts[1:end-1], starts[2:end] .- 1)
end

"""
Given a size tuple of an array, CutDimension layout
and targets return the index ranges for each chunk.
"""
function slice_indexes{d}(ctx, dims::Tuple, ::CutDimension{d}, targets)
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

function slice{d}(ctx, arr::AbstractArray, p::CutDimension{d}, targets)
    # Slice an array along a dimension
    [getindex(arr, idx...) for idx in slice_indexes(ctx, size(arr), p, targets)]
end

function gather{d}(ctx, layout::CutDimension{d}, xs::Vector)
    reduce((acc, x) -> cat(d, acc, x), xs[1], xs[2:end])
end


immutable Bcast <: AbstractLayout end

function slice{T}(ctx, x::T, ::Bcast, targets)
    T[x for i in 1:length(targets)]
end

function gather(ctx, ::Bcast, parts)
    return parts
end
