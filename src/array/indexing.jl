### getindex

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
    return GPUArraysCore.@allowscalar part[offset_idx...]
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
# Convert a `to_indices` entry for a same-ndims copy. Integers become `i:i`
# (keep the dimension so `copyto!` can zip parent indices). StepRange and
# LogicalIndex become `Vector{Int}` so the copy uses the scattered tile-pair
# path instead of the intentional StepRange `copyto!` throw.
_as_getindex_index(x::Integer) = Int(x):Int(x)
_as_getindex_index(x::StepRange{<:Integer}) = collect(Int, x)
_as_getindex_index(x::Base.LogicalIndex) = collect(Int, x)
_as_getindex_index(x::Base.Slice) = Int(first(x)):Int(last(x))
_as_getindex_index(x::Base.OneTo) = UnitRange{Int}(x)
_as_getindex_index(x::UnitRange{<:Integer}) = UnitRange{Int}(x)
# `[]` is `Vector{Any}`, not `AbstractVector{<:Integer}`.
_as_getindex_index(x::AbstractVector) = x isa Vector{Int} ? x : collect(Int, x)
_as_getindex_index(x) = x

_slice_index_length(x::Integer) = 1
_slice_index_length(x) = length(x)

function Base.getindex(A::DArray, idx...)
    inds = to_indices(A, idx)
    Base.@boundscheck checkbounds(A, inds...)

    # Linear range / vector into an N>1 array (`A[2:5]`, `A[:]`).
    if ndims(A) > 1 && length(inds) == 1
        return _getindex_linear(A, inds[1])
    end

    copy_inds = ntuple(i -> _as_getindex_index(inds[i]), length(inds))
    drop = Int[i for i in 1:length(inds) if inds[i] isa Integer]
    sz = ntuple(i -> _slice_index_length(copy_inds[i]), length(copy_inds))
    part = length(sz) == length(A.partitioning.blocksize) ? A.partitioning : auto_blocks(sz)
    B = _allocate_slice(A, part, sz)
    copyto!(B, view(A, copy_inds...))
    if !isempty(drop)
        return _drop_unit_dims(B, (drop...,))
    end
    return B
end
function _getindex_linear(A::DArray{T}, ind) where T
    lin = _as_getindex_index(ind)
    sz = (length(lin),)
    B = _allocate_slice(A, auto_blocks(sz), sz)
    copyto!(B, view(A, lin))
    return B
end
# Empty slices have no values to densify; `AllocateArray` / sparse tile
# setup is unhappy with a zero-length `DomainBlocks`, so keep the dense
# `undef` constructor there. Non-empty slices use `allocate_tiled`.
function _allocate_slice(A::DArray, part::Blocks{N}, sz::Dims{N}) where N
    if any(iszero, sz)
        return DArray{eltype(A)}(undef, part, sz)
    end
    return allocate_tiled(darray_tiletype(A), eltype(A), part, sz)
end
Base.getindex(A::DArray, idx::ArrayDomain) =
    getindex(A, indexes(idx)...)

# Squeeze length-1 dimensions that came from an integer index, matching Base
# (`A[:, 5]` is a `DVector`, `A[:, 5:5]` stays a 1-column `DMatrix`). Tiles
# stay the remaining `Blocks` components so a column of a `Blocks(m, n)`
# matrix is `Blocks(m)`, not a silent gather.
function _kept_dims(::Val{N}, drop::NTuple{D,Int}) where {N,D}
    ntuple(Val(N - D)) do j
        count = 0
        for i in 1:N
            if !(i in drop)
                count += 1
                count == j && return i
            end
        end
        throw(ArgumentError("internal: dropdims keep overflow"))
    end
end
function _dropdims_domainblocks(sd::DomainBlocks{N}, drop::NTuple{D,Int}) where {N,D}
    keep = _kept_dims(Val(N), drop)
    K = N - D
    DomainBlocks(
        ntuple(i -> sd.start[keep[i]], K),
        ntuple(i -> sd.cumlength[keep[i]], K)
    )
end
function _dropdims_tile(x, drop)
    return dropdims(x; dims=drop)
end
function _drop_unit_dims(A::DArray{T,N}, drop::NTuple{D,Int}) where {T,N,D}
    D == 0 && return A
    for d in drop
        size(A, d) == 1 || throw(ArgumentError(
            "cannot drop dimension $d of size $(size(A, d)) (expected 1)"))
    end
    K = N - D
    K == 0 && throw(ArgumentError("internal: dim-drop produced a 0-d DArray"))
    keep = _kept_dims(Val(N), drop)
    new_sz = ntuple(i -> size(A, keep[i]), K)
    new_part = Blocks(ntuple(i -> A.partitioning.blocksize[keep[i]], K))
    squeezed = dropdims(A.chunks; dims=drop)
    new_chunks = map(squeezed) do c
        Dagger.@spawn _dropdims_tile(c, drop)
    end
    new_subdomains = _dropdims_domainblocks(A.subdomains, drop)
    new_domain = ArrayDomain(ntuple(i -> 1:new_sz[i], K))
    return DArray(T, new_domain, new_subdomains, new_chunks, new_part, A.concat)
end

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
    return fetch(Dagger.@spawn scope=scope setindex_allowscalar!(part, value, offset_idx...))
end
function setindex_allowscalar!(part, value, offset_idx...)
    GPUArraysCore.@allowscalar setindex!(part, value, offset_idx...)
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
    if value isa Number
        fill!(A_view, value)
        return value
    end
    copyto!(A_view, value)
    return value
end

function Base.fill!(A::SubArray{T,N,<:DArray}, x) where {T,N}
    isempty(A) && return A
    tmp = allocate_tiled(darray_tiletype(parent(A)), T, auto_blocks(size(A)), size(A))
    fill!(tmp, x)
    copyto!(A, tmp)
    return A
end
function Base.fill!(A::LinearDArrayView{T}, x) where T
    isempty(A) && return A
    tmp = allocate_tiled(darray_tiletype(_linear_view_parent(A)), T, auto_blocks(size(A)), size(A))
    fill!(tmp, x)
    copyto!(A, tmp)
    return A
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
