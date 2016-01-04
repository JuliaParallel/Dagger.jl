export cutdim, Bcast, typelayout

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

    map(UnitRange, starts[1:end-1], starts[2:end] .- 1)
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

### Hash table layouts

immutable HashBucket <: AbstractLayout
    hash::Function
end
HashBucket() = HashBucket(hash)

key(x) = x[1]
value(x) = x[2]

function slice(ctx, obj, hash::HashBucket, targets)
    n = length(targets)
    buckets = [Any[] for k in 1:n]
    for x in obj
        target = (hash.hash(key(x)) % n) + 1
        push!(buckets[target], key(x) => value(x))
    end
    buckets
end

immutable BucketToMatch{N<:AbstractNode} <: AbstractLayout
    reference::N
end

function slice(ctx, obj, b::BucketToMatch, targets)

end


## Sort layout

immutable SortLayout <: AbstractLayout
    options::Dict
end

function slice(ctx, obj, layout::SortLayout, targets)
    sorted = sort(obj ;layout.options...)
    [getindex(obj, idx...) for idx in index_splits(length(obj), length(targets))]
end

function gather(ctx, layout::SortLayout, xs::Vector)
    reduce(vcat, [], xs)
end


## Type Layout.

immutable TypeLayout{T} <: AbstractLayout
    layouts::Vector
end
getlayouts(x::TypeLayout) = x.layouts

"""
Allows the slicing and gathering of a user-defined type. Requires an array describing
the layouts of the each of the type's fields.
"""
typelayout{T<:DataType,L<:AbstractLayout}(x::T, xs::Vector{L}) = TypeLayout{x}(xs)

function slice{T}(ctx, obj, tlayout::TypeLayout{T}, targets)
    items = [getfield(obj, field) for field in fieldnames(T)]
    field_chunks = [slice(ctx,item,layout,targets) for (item,layout) in zip(items,getlayouts(tlayout))]
    [T(t...) for t in zip(field_chunks...)]
end

function gather{T}(ctx, tlayout::TypeLayout{T}, xs::Vector)
    item_chunks = [[getfield(x, field) for x in xs] for field in fieldnames(T)]
    field_chunks = [gather(ctx, layout, items) for (items,layout) in zip(item_chunks,getlayouts(tlayout))]
    T(field_chunks...)
end
