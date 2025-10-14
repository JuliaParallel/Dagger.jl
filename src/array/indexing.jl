#get index 

const GETINDEX_CACHE = TaskLocalValue{Dict{Tuple,Any}}(()->Dict{Tuple,Any}())
const GETINDEX_CACHE_SIZE = ScopedValue{Int}(0)
with_index_caching(f, size::Integer=1) = with(f, GETINDEX_CACHE_SIZE=>size)
@inline function Base.getindex(A::DArray{T,N}, idx::NTuple{N,Int}) where {T,N}
    # Scalar indexing check
    assert_allowscalar()

    # Boundscheck
    Base.@boundscheck checkbounds(A, idx...)

    # Find the associated partition and offset within it
    part_idx, offset_idx = partition_for(A, idx)

    # If the partition is cached, use that for lookup
    cache = GETINDEX_CACHE[]
    cache_size = GETINDEX_CACHE_SIZE[]
    if cache_size > 0 && haskey(cache, part_idx)
        return cache[part_idx][offset_idx...]
    end

    # Uncached, fetch the partition
    part = fetch(A.chunks[part_idx...])

    # Insert the partition into the cache
    if cache_size > 0
        if length(cache) >= cache_size
            # Evict a random entry
            key = rand(keys(cache))
            delete!(cache, key)
        end
        cache[part_idx] = part
    end

    # Return the value
    return part[offset_idx...]
end

function partition_for(A::DArray, idx::NTuple{N,Int}) where N
    part_idx = zeros(Int, N)
    offset_idx = zeros(Int, N)
    for dim in 1:N
        part_idx_slice = @view part_idx[1:(dim-1)]
        trailing_idx_slice = ntuple(i->Colon(), N-dim)
        sds = @view A.subdomains[part_idx_slice..., :, trailing_idx_slice...]
        for (sd_idx, sd) in enumerate(sds)
            sd_range = (sd.indexes::NTuple{N,UnitRange{Int}})[dim]
            if sd_range.start <= idx[dim] <= sd_range.stop
                part_idx[dim] = sd_idx
                offset_idx[dim] = idx[dim] - sd_range.start + 1
                break
            end
        end
    end
    return (part_idx...,), (offset_idx...,)
end
Base.getindex(A::DArray, idx::Integer...) =
    getindex(A, idx)
Base.getindex(A::DArray, idx::Integer) =
    getindex(A, Base._ind2sub(A, idx))
Base.getindex(A::DArray, idx::CartesianIndex) =
    getindex(A, Tuple(idx))
function Base.getindex(A::DArray{T,N}, idxs::Dims{S}) where {T,N,S}
    if S > N
        if all(idxs[(N+1):end] .== 1)
            return getindex(A, idxs[1:N])
        else
            throw(BoundsError(A, idxs))
        end
    elseif S < N
        throw(BoundsError(A, idxs))
    end
    error()
end
function Base.getindex(A::DArray, idx...)
    inds = to_indices(A, idx)
    A_view = view(A, inds...)
    nd = length(inds)
    sz = ntuple(i->length(inds[i]), nd)
    # TODO: Pad out to same number of dims?
    part = nd == length(A.partitioning.blocksize) ? A.partitioning : auto_blocks(sz)
    B = zeros(part, eltype(A), sz) # FIXME: Use undef initializer
    copyto!(B, A_view)
    if size(A_view) != sz
        # N.B. Base automatically transposes a row vector to a column vector
        return DArray(reshape(B, size(A_view)))
    end
    return B
end
Base.getindex(A::DArray, idx::ArrayDomain) =
    getindex(A, indexes(idx)...)

### setindex!

@inline function Base.setindex!(A::DArray{T,N}, value, idx::NTuple{N,Int}) where {T,N}
    # Scalar indexing check
    assert_allowscalar()

    # Boundscheck
    Base.@boundscheck checkbounds(A, idx...)

    # Find the associated partition and offset within it
    part_idx, offset_idx = partition_for(A, idx)

    # If the partition is cached, evict it
    cache = GETINDEX_CACHE[]
    if haskey(cache, part_idx)
        delete!(cache, part_idx)
    end

    # Set the value
    part = A.chunks[part_idx...]
    space = memory_space(part)
    scope = UnionScope(map(ExactScope, collect(processors(space))))
    return fetch(Dagger.@spawn scope=scope setindex!(part, value, offset_idx...))
end
Base.setindex!(A::DArray, value, idx::Integer...) =
    setindex!(A, value, idx)
Base.setindex!(A::DArray, value, idx::Integer) =
    setindex!(A, value, Base._ind2sub(A, idx))
Base.setindex!(A::DArray, value, idx::CartesianIndex) =
    setindex!(A, value, Tuple(idx))
function Base.setindex!(A::DArray{T,N}, value, idxs::Dims{S}) where {T,N,S}
    if S > N
        if all(idxs[(N+1):end] .== 1)
            return setindex!(A, value, idxs[1:N])
        else
            throw(BoundsError(A, idxs))
        end
    elseif S < N
        throw(BoundsError(A, idxs))
    end
    error()
end
function Base.setindex!(A::DArray, value, idx...)
    inds = to_indices(A, idx)
    A_view = view(A, inds...)
    copyto!(A_view, value)
    return value
end

### Allow/disallow scalar indexing

const ALLOWSCALAR_TASK = TaskLocalValue{Bool}(()->true)
const ALLOWSCALAR_SCOPE = ScopedValue{Bool}(false)
isallowscalar() = ALLOWSCALAR_TASK[] || ALLOWSCALAR_SCOPE[]
function assert_allowscalar()
    if !isallowscalar()
        throw(ArgumentError("Scalar indexing is disallowed\nSee `allowscalar` and `allowscalar!` for ways to disable this check, if necessary"))
    end
end
"Allow/disallow scalar indexing for the current task."
function allowscalar!(allow::Bool=true)
    ALLOWSCALAR_TASK[] = allow
end
"Allow/disallow scalar indexing for the duration of executing `f`."
function allowscalar(f, allow::Bool=true)
    old = ALLOWSCALAR_TASK[]
    allowscalar!(allow)
    try
        return with(f, ALLOWSCALAR_SCOPE=>allow)
    finally
        allowscalar!(old)
    end
end
