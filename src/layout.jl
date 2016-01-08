export cutdim, BCast


"""
The domain of an object in relation with a partitioning scheme
"""
function domain(obj, layout)
    error("Cannot figure out the domain of $(typeof(obj)) for $(typeof(layout)) partition")
end

"""
The domain of an array is the index range that spans its elements
"""
function domain(arr::AbstractArray, p)
    map(x -> 1:x, size(arr))
end


######## Unknown layout ########

immutable UnknownLayout <: AbstractLayout end
immutable Chunks
    xs
end

function partition(ctx, obj, ::UnknownLayout)
    error("Cannot distribute with UnknownLayout")
end

gather(ctx, ::UnknownLayout, xs) = Chunks(xs)


######## Array partitioning ########

immutable SliceDimension{d} <: AbstractLayout end
typealias ColumnLayout SliceDimension{2}
typealias RowLayout SliceDimension{1}

"""
Cut an array along a given dimension
"""
cutdim(n) = SliceDimension{n}()

"""
Given an n-tuple of index ranges, cut the index range along dimension `d`
"""
function partition_domain{d}(ctx, dims, ::SliceDimension{d})
    # Slice an array along a dimension

    dimrange = dims[d] # Range along sliced dimension
    @show targets = chunk_targets(ctx)
    parts = length(targets)

    ranges = split_range(dimrange, parts)
    chunks = Array(Any, parts)

    dims_array = [d for d in dims]

    [begin
        chunkidx = copy(dims_array)
        chunkidx[d] = ranges[i]
        chunkidx
     end for i in 1:parts]
end


function partition(ctx, obj, p=default_layout(obj))
    # Slice an array along a dimension
    partitions = partition_domain(ctx, domain(obj, p), p)
    [getindex(obj, idx...) for idx in partitions], partitions
end

gather{d}(ctx, layout::SliceDimension{d}, xs::Vector) = cat(d, xs...)

"""
Utility function to divide the range `range` into `n` parts
"""
function split_range(range, n)
    len = length(range)

    starts = len >= n ?
        round(Int, linspace(first(range), last(range)+1, n+1)) :
        [[first(range):(last(range)+1);], zeros(Int, n-len);]

    map(UnitRange, starts[1:end-1], starts[2:end] .- 1)
end

immutable Bcast <: AbstractLayout end

function partition_domain(ctx, x, ::Bcast)
    [domain(x) for i in 1:n]
end

function partition{T}(ctx, x::T, p::Bcast)
    n = length(chunk_targets(ctx))
    T[x for i in 1:n], partition_domain(ctx, x, p)
end

function gather(ctx, ::Bcast, parts)
    return parts[1]
end


### Hash table layouts

immutable HashBucket <: AbstractLayout
    hash::Function
end
HashBucket() = HashBucket(hash)

key(x) = x[1]
value(x) = x[2]

function domain(obj::AbstractVector, ::HashBucket)
    1:length(obj)
end

function domain(obj, ::HashBucket)
    map(key, obj)
end

function partition(ctx, obj, hash::HashBucket)
    targets = chunk_targets(ctx)
    n = length(targets)
    buckets = [Any[] for k in 1:n]
    for x in domain(obj)
        target = (hash.hash(key(x)) % n) + 1
        push!(buckets[target], x => obj[x])
    end
    buckets
end

## Sort layout

immutable SortLayout <: AbstractLayout
    options::Dict
end

function partition(ctx, obj, layout::SortLayout)
    sorted = sort(obj ;layout.options...)
    partition(ctx, sorted)
end

gather(ctx, layout::SortLayout, xs::Vector) = vcat(xs...)
