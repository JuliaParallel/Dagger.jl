# FIXME: Remove me
const Read = In
const Write = Out
const ReadWrite = InOut

function validate_neigh_dist(neigh_dist)
    if !(neigh_dist isa Integer) && !(neigh_dist isa Tuple)
        throw(ArgumentError("Neighborhood distance ($neigh_dist) must be an Integer or Tuple"))
    end
    if any(neigh_dist .<= 0)
        throw(ArgumentError("Neighborhood distance ($neigh_dist) must be greater than 0"))
    end
end
function validate_neigh_dist(neigh_dist, size)
    validate_neigh_dist(neigh_dist)
    if any(size .< neigh_dist)
        throw(ArgumentError("Neighborhood distance ($neigh_dist) must not be larger than the chunk size ($size)"))
    end
end
# Overload for checking only tuple-length compatibility, called outside @spawn where only
# ndims is known (not actual chunk sizes). The full size check still runs inside @spawn.
function validate_neigh_dist(neigh_dist, N::Int)
    validate_neigh_dist(neigh_dist)
    if neigh_dist isa Tuple && length(neigh_dist) != N
        throw(ArgumentError("Neighborhood distance tuple length ($(length(neigh_dist))) must match array ndims ($N)"))
    end
end

get_neigh_dist(neigh_dist::Integer, i::Int) = neigh_dist
get_neigh_dist(neigh_dist::Tuple, i::Int) = neigh_dist[i]

# Get boundary condition for dimension i (supports single boundary condition or tuple of boundary conditions)
get_boundary(boundary, i::Int) = boundary
get_boundary(boundary::Tuple, i::Int) = boundary[i]

# Load a halo region from a neighboring chunk
# region_code: N-tuple where each element is -1 (low), 0 (full extent), or +1 (high)
# For dimensions with code 0, we take the full extent of the array
# For dimensions with code -1, we take the last neigh_dist elements (to go to neighbor's low side)
# For dimensions with code +1, we take the first neigh_dist elements (to go to neighbor's high side)
function load_neighbor_region(arr, region_code::NTuple{N,Int}, neigh_dist) where N
    validate_neigh_dist(neigh_dist, size(arr))
    start_idx = CartesianIndex(ntuple(N) do i
        if region_code[i] == -1
            lastindex(arr, i) - get_neigh_dist(neigh_dist, i) + 1
        else
            firstindex(arr, i)
        end
    end)
    stop_idx = CartesianIndex(ntuple(N) do i
        if region_code[i] == +1
            firstindex(arr, i) + get_neigh_dist(neigh_dist, i) - 1
        else
            lastindex(arr, i)
        end
    end)
    return move(task_processor(), copy(@view arr[start_idx:stop_idx]))
end

is_past_boundary(size, idx) = any(ntuple(i -> idx[i] < 1 || idx[i] > size[i], length(size)))

#############################################################################
# Boundary Condition Interface
#############################################################################
#
# To implement a custom boundary condition, define a struct and implement:
#
# Required:
#   - boundary_has_transition(::MyBoundary) -> Bool
#   - load_boundary_region(::MyBoundary, arr, region_code, neigh_dist, boundary_dims)
#
# Required if boundary_has_transition returns true:
#   - boundary_transition(::MyBoundary, idx, size) -> CartesianIndex
#
# Required for mixed boundary condition support (when used in a tuple with other boundary conditions):
#   - boundary_source_index(::MyBoundary, arr, rc, nd, idx_d, d) -> Int
#   - apply_boundary_value(::MyBoundary, value, arr, rc, nd, idx_d, src_idx, d) [optional, default returns value unchanged]
#
#############################################################################

# Default implementations for boundary_source_index and apply_boundary_value
# These are used when a boundary condition is part of a mixed boundary condition tuple

"""
    boundary_source_index(boundary, arr, rc, nd, idx_d, d) -> Int

Compute the source index for dimension `d` when the boundary condition is used in a mixed boundary condition tuple.
- `boundary`: The boundary condition
- `arr`: The array being accessed
- `rc`: Region code for this dimension (-1, 0, or +1)
- `nd`: Neighborhood distance for this dimension
- `idx_d`: The index in the result array for this dimension
- `d`: The dimension number

Default implementation clamps to valid array range.
"""
boundary_source_index(::Any, arr, rc, nd, idx_d, d) = clamp(idx_d, firstindex(arr, d), lastindex(arr, d))

"""
    apply_boundary_value(boundary, value, arr, rc, nd, idx_d, src_idx, d)

Apply the boundary condition's value transformation for dimension `d` when used in a mixed boundary condition tuple.
- `boundary`: The boundary condition
- `value`: The current value from the source array
- `arr`: The array being accessed
- `rc`: Region code for this dimension (-1, 0, or +1)
- `nd`: Neighborhood distance for this dimension
- `idx_d`: The index in the result array for this dimension
- `src_idx`: The full source index tuple
- `d`: The dimension number

Default implementation returns the value unchanged.
"""
apply_boundary_value(::Any, value, arr, rc, nd, idx_d, src_idx, d) = value

#############################################################################
# Wrap Boundary Condition
#############################################################################

"""
Wrap boundary condition. Non-local accesses wrap around to the other side of the array.
"""
struct Wrap end

boundary_has_transition(::Wrap) = true

boundary_transition(::Wrap, idx, size) =
    CartesianIndex(ntuple(i -> mod1(idx[i], size[i]), length(size)))

load_boundary_region(::Wrap, arr, region_code, neigh_dist, boundary_dims) =
    load_neighbor_region(arr, region_code, neigh_dist)

function boundary_source_index(::Wrap, arr, rc, nd, idx_d, d)
    if rc == -1
        return lastindex(arr, d) - nd + idx_d
    elseif rc == +1
        return firstindex(arr, d) + idx_d - 1
    else
        return idx_d
    end
end

#############################################################################
# Pad Boundary Condition
#############################################################################

"""
Pad boundary condition. Non-local accesses are padded with a specified value.
"""
struct Pad{T}
    padval::T
end

boundary_has_transition(::Pad) = false

function load_boundary_region(pad::Pad, arr, region_code::NTuple{N,Int}, neigh_dist, boundary_dims::NTuple{N,Bool}) where N
    # Compute the size of this halo region
    # For dimensions with code 0, use full array size
    # For dimensions with code -1 or +1, use neigh_dist
    region_size = ntuple(N) do i
        region_code[i] == 0 ? size(arr, i) : get_neigh_dist(neigh_dist, i)
    end
    result = similar(arr, region_size...)
    fill!(result, pad.padval)
    return move(task_processor(), result)
end

# Use edge as source index (value will be overridden by apply_boundary_value)
boundary_source_index(::Pad, arr, rc, nd, idx_d, d) =
    rc == -1 ? firstindex(arr, d) : (rc == +1 ? lastindex(arr, d) : idx_d)

# Override with pad value
apply_boundary_value(p::Pad, value, arr, rc, nd, idx_d, src_idx, d) = p.padval

#############################################################################
# Clamp Boundary Condition
#############################################################################

"""
Clamp boundary condition. Non-local accesses are clamped to the boundary value.
For example, an array [1,2,3,4] with neighborhood distance 2 would be extended as [1,1,1,2,3,4,4,4].
"""
struct Clamp end

boundary_has_transition(::Clamp) = true

# Clamp to valid chunk indices - we stay at the boundary chunk
boundary_transition(::Clamp, idx, size) =
    CartesianIndex(ntuple(i -> clamp(idx[i], 1, size[i]), length(size)))

@kernel function load_boundary_region_kernel(::Clamp, result, arr, region_code::NTuple{N,Int}, neigh_dist, boundary_dims::NTuple{N,Bool}) where N
    raw_idx = @index(Global, Linear)

    # Convert linear index to Cartesian index
    idx = CartesianIndices(result)[raw_idx]

    # Compute source index for each dimension
    src_idx = CartesianIndex(ntuple(N) do i
        nd = get_neigh_dist(neigh_dist, i)
        if boundary_dims[i] && region_code[i] == -1
            # Low boundary - clamp to first element
            firstindex(arr, i)
        elseif boundary_dims[i] && region_code[i] == +1
            # High boundary - clamp to last element
            lastindex(arr, i)
        elseif region_code[i] == -1
            # Not at boundary but loading from low side of neighbor
            lastindex(arr, i) - nd + idx[i]
        elseif region_code[i] == +1
            # Not at boundary but loading from high side of neighbor
            firstindex(arr, i) + idx[i] - 1
        else
            # Full extent
            idx[i]
        end
    end)
    result[idx] = arr[src_idx]
end
function load_boundary_region(::Clamp, arr, region_code::NTuple{N,Int}, neigh_dist, boundary_dims::NTuple{N,Bool}) where N
    # Compute the size of this halo region
    region_size = ntuple(N) do i
        region_code[i] == 0 ? size(arr, i) : get_neigh_dist(neigh_dist, i)
    end

    result = similar(arr, region_size)

    Kernel(load_boundary_region_kernel)(Clamp(), result, arr, region_code, neigh_dist, boundary_dims; ndrange=length(result))

    return move(task_processor(), result)
end

function boundary_source_index(::Clamp, arr, rc, nd, idx_d, d)
    if rc == -1
        return firstindex(arr, d)
    elseif rc == +1
        return lastindex(arr, d)
    else
        return idx_d
    end
end

#############################################################################
# LinearExtrapolate Boundary Condition
#############################################################################

"""
LinearExtrapolate boundary condition. Non-local accesses are extrapolated linearly
using the slope at the boundary. Only supports arrays with `Real` element types.

For multi-dimensional arrays, extrapolation is applied along the first out-of-bounds
dimension only (other out-of-bounds dimensions are clamped).
"""
struct LinearExtrapolate end

boundary_has_transition(::LinearExtrapolate) = true

# Clamp to valid chunk indices - we stay at the boundary chunk
boundary_transition(::LinearExtrapolate, idx, size) =
    CartesianIndex(ntuple(i -> clamp(idx[i], 1, size[i]), length(size)))

@kernel function load_boundary_region_kernel(::LinearExtrapolate, result, arr, region_code::NTuple{N,Int}, neigh_dist, boundary_dims::NTuple{N,Bool}, ::Val{extrap_dim}, ::Val{nd}) where {N,extrap_dim,nd}
    raw_idx = @index(Global, Linear)

    # Convert linear index to Cartesian index
    idx = CartesianIndices(result)[raw_idx]

    if extrap_dim == 0
        # No boundary dimensions - normal neighbor access
        src_idx = CartesianIndex(ntuple(Val(N)) do i
            ndi = get_neigh_dist(neigh_dist, i)::Int
            if region_code[i] == -1
                lastindex(arr, i) - ndi + idx[i]
            elseif region_code[i] == +1
                firstindex(arr, i) + idx[i] - 1
            else
                idx[i]
            end
        end)
        result[idx] = arr[src_idx]
    else
        # Compute base index (for other dimensions, clamp if at boundary)
        base_idx = ntuple(Val(N)) do i
            ndi = get_neigh_dist(neigh_dist, i)
            if i == extrap_dim
                # Will be set for slope computation
                region_code[i] == -1 ? firstindex(arr, i) : lastindex(arr, i)
            elseif boundary_dims[i] && region_code[i] == -1
                firstindex(arr, i)
            elseif boundary_dims[i] && region_code[i] == +1
                lastindex(arr, i)
            elseif region_code[i] == -1
                lastindex(arr, i) - ndi + idx[i]
            elseif region_code[i] == +1
                firstindex(arr, i) + idx[i] - 1
            else
                idx[i]
            end
        end

        # Compute slope at boundary
        if region_code[extrap_dim] == -1
            # Low boundary: slope = arr[2] - arr[1]
            idx1 = ntuple(i -> i == extrap_dim ? firstindex(arr, i) : base_idx[i], Val(N))
            idx2 = ntuple(i -> i == extrap_dim ? firstindex(arr, i) + 1 : base_idx[i], Val(N))
            slope = arr[CartesianIndex(idx2)] - arr[CartesianIndex(idx1)]
            dist = -(nd - idx[extrap_dim] + 1)
            result[idx] = arr[CartesianIndex(idx1)] + slope * dist
        else
            # High boundary: slope = arr[end] - arr[end-1]
            idx1 = ntuple(i -> i == extrap_dim ? lastindex(arr, i) - 1 : base_idx[i], Val(N))
            idx2 = ntuple(i -> i == extrap_dim ? lastindex(arr, i) : base_idx[i], Val(N))
            slope = arr[CartesianIndex(idx2)] - arr[CartesianIndex(idx1)]
            dist = idx[extrap_dim]
            result[idx] = arr[CartesianIndex(idx2)] + slope * dist
        end
    end
end
function load_boundary_region(::LinearExtrapolate, arr::AbstractArray{T}, region_code::NTuple{N,Int}, neigh_dist, boundary_dims::NTuple{N,Bool}) where {T<:Real,N}
    # Compute the size of this halo region
    region_size = ntuple(N) do i
        region_code[i] == 0 ? size(arr, i) : get_neigh_dist(neigh_dist, i)
    end

    result = similar(arr, region_size)

    # Find the first boundary dimension that needs extrapolation
    extrap_dim = 0
    for d in 1:N
        if boundary_dims[d] && region_code[d] != 0
            extrap_dim = d
            break
        end
    end

    # Extrapolate along extrap_dim, clamp other boundary dimensions
    nd = get_neigh_dist(neigh_dist, extrap_dim)

    Kernel(load_boundary_region_kernel)(LinearExtrapolate(), result, arr, region_code, neigh_dist, boundary_dims, Val(extrap_dim), Val(nd); ndrange=length(result))

    return move(task_processor(), result)
end

# Use edge as source index (value will be computed by apply_boundary_value)
boundary_source_index(::LinearExtrapolate, arr, rc, nd, idx_d, d) =
    rc == -1 ? firstindex(arr, d) : (rc == +1 ? lastindex(arr, d) : idx_d)

function apply_boundary_value(::LinearExtrapolate, value, arr::AbstractArray{T}, rc, nd, idx_d, src_idx, d) where T<:Real
    if rc == -1
        # Low boundary: extrapolate using slope from arr[1] to arr[2]
        idx1 = ntuple(i -> i == d ? firstindex(arr, i) : src_idx[i], length(src_idx))
        idx2 = ntuple(i -> i == d ? firstindex(arr, i) + 1 : src_idx[i], length(src_idx))
        slope = arr[CartesianIndex(idx2)] - arr[CartesianIndex(idx1)]
        dist = -(nd - idx_d + 1)
        return arr[CartesianIndex(idx1)] + slope * dist
    elseif rc == +1
        # High boundary: extrapolate using slope from arr[end-1] to arr[end]
        idx1 = ntuple(i -> i == d ? lastindex(arr, i) - 1 : src_idx[i], length(src_idx))
        idx2 = ntuple(i -> i == d ? lastindex(arr, i) : src_idx[i], length(src_idx))
        slope = arr[CartesianIndex(idx2)] - arr[CartesianIndex(idx1)]
        dist = idx_d
        return arr[CartesianIndex(idx2)] + slope * dist
    else
        return value
    end
end

#############################################################################
# Reflect Boundary Condition
#############################################################################

"""
Reflect boundary condition. Non-local accesses are reflected back into the array.
If `symm` is true, the reflected values include the nearest center elements.
If `symm` is false, the reflected values do not include the nearest center elements.
"""
struct Reflect{Symmetric} end
Reflect(symm::Bool) = Reflect{symm}()

boundary_has_transition(::Reflect) = true

# Clamp to valid chunk indices - we stay at the boundary chunk
boundary_transition(::Reflect, idx, size) =
    CartesianIndex(ntuple(i -> clamp(idx[i], 1, size[i]), length(size)))

function load_boundary_region(::Reflect{Symm}, arr, region_code::NTuple{N,Int}, neigh_dist, boundary_dims::NTuple{N,Bool}) where {N, Symm}
    # Only flip region_code for dimensions that are BOTH:
    # 1. Non-zero in region_code (we're accessing a neighbor in that dimension)
    # 2. Actually past boundary (boundary_dims[i] is true)
    # For dimensions not past boundary, keep the original region_code behavior
    flipped_code = ntuple(N) do i
        if region_code[i] != 0 && boundary_dims[i]
            # This dimension needs reflection - flip the code
            -region_code[i]
        else
            # Keep original code (either 0, or not past boundary)
            region_code[i]
        end
    end

    # For non-symmetric (mirror), skip 1 element to exclude the edge
    # For symmetric, include the edge element (skip = 0)
    # Only apply skip to dimensions that are being reflected
    skip = Symm ? 0 : 1

    # Compute region indices
    start_idx = CartesianIndex(ntuple(N) do i
        needs_skip = boundary_dims[i] && region_code[i] != 0
        actual_skip = needs_skip ? skip : 0
        if flipped_code[i] == -1
            # Taking from end (high side)
            lastindex(arr, i) - get_neigh_dist(neigh_dist, i) + 1 - actual_skip
        elseif flipped_code[i] == +1
            # Taking from start (low side)
            firstindex(arr, i) + actual_skip
        else
            firstindex(arr, i)
        end
    end)
    stop_idx = CartesianIndex(ntuple(N) do i
        needs_skip = boundary_dims[i] && region_code[i] != 0
        actual_skip = needs_skip ? skip : 0
        if flipped_code[i] == +1
            firstindex(arr, i) + get_neigh_dist(neigh_dist, i) - 1 + actual_skip
        elseif flipped_code[i] == -1
            lastindex(arr, i) - actual_skip
        else
            lastindex(arr, i)
        end
    end)

    region = move(task_processor(), copy(@view arr[start_idx:stop_idx]))

    # Reverse only along dimensions that are actually being reflected
    # (both non-zero in region_code AND past boundary)
    for i in 1:N
        # FIXME: allowscalar because some GPU backends don't overload reverse
        GPUArraysCore.@allowscalar if region_code[i] != 0 && boundary_dims[i]
            region = reverse(region, dims=i)
        end
    end

    return region
end

function boundary_source_index(::Reflect{Symm}, arr, rc, nd, idx_d, d) where Symm
    skip = Symm ? 0 : 1
    if rc == -1
        # Reflecting from low boundary - source from start of array
        return firstindex(arr, d) + skip + (nd - idx_d)
    elseif rc == +1
        # Reflecting from high boundary - source from end of array
        return lastindex(arr, d) - skip - (idx_d - 1)
    else
        return idx_d
    end
end

#############################################################################
# Mixed Boundary Conditions (Tuple of Boundary Conditions)
#############################################################################

# Mixed boundary condition support: check if any dimension has a transition boundary condition
boundary_has_transition(boundary::Tuple) = any(boundary_has_transition, boundary)

# Mixed boundary condition support: apply per-dimension transitions
function boundary_transition(boundary::Tuple, idx, size)
    CartesianIndex(ntuple(length(size)) do i
        dim_boundary = get_boundary(boundary, i)
        if boundary_has_transition(dim_boundary)
            # Apply the boundary condition's transition for this dimension only
            single_idx = CartesianIndex(idx[i])
            single_size = (size[i],)
            boundary_transition(dim_boundary, single_idx, single_size)[1]
        else
            # No transition - clamp to valid range (stay at current chunk)
            clamp(idx[i], 1, size[i])
        end
    end)
end

# Internal helper: compute source index for a single dimension based on its boundary condition
function compute_source_index_for_dim(dim_boundary, arr, region_code, neigh_dist, boundary_dims, idx, d)
    N = length(region_code)
    nd = get_neigh_dist(neigh_dist, d)

    if !boundary_dims[d]
        # Not at boundary - normal neighbor region access
        if region_code[d] == -1
            return lastindex(arr, d) - nd + idx[d]
        elseif region_code[d] == +1
            return firstindex(arr, d) + idx[d] - 1
        else
            return idx[d]
        end
    end

    # At boundary - apply boundary condition-specific logic
    return boundary_source_index(dim_boundary, arr, region_code[d], nd, idx[d], d)
end

# Internal helper: compute the final value, handling special boundary conditions like Pad and LinearExtrapolate
function compute_boundary_value(boundary, arr, region_code, neigh_dist, boundary_dims, idx, src_idx)
    N = length(region_code)
    base_value = arr[CartesianIndex(src_idx)]

    # Check if any boundary dimension has a special boundary condition that overrides the value
    for d in 1:N
        if boundary_dims[d] && region_code[d] != 0
            dim_boundary = get_boundary(boundary, d)
            base_value = apply_boundary_value(dim_boundary, base_value, arr, region_code[d], get_neigh_dist(neigh_dist, d), idx[d], src_idx, d)
        end
    end

    return base_value
end

# GPU-compatible helper: recursively apply boundary value transformations for mixed boundaries.
# Uses Val{d} to ensure boundary[d] is resolved at compile time (avoiding type instability
# from indexing a heterogeneous Tuple with a runtime variable).
@inline function _fold_boundary_value(boundary::Tuple, base_value, arr, region_code::NTuple{N,Int}, neigh_dist, boundary_dims::NTuple{N,Bool}, idx, src_idx, ::Val{d}) where {N, d}
    if boundary_dims[d] && region_code[d] != 0
        dim_boundary = boundary[d]
        base_value = apply_boundary_value(dim_boundary, base_value, arr, region_code[d], get_neigh_dist(neigh_dist, d), idx[d], src_idx, d)
    end
    if d < N
        return _fold_boundary_value(boundary, base_value, arr, region_code, neigh_dist, boundary_dims, idx, src_idx, Val(d + 1))
    end
    return base_value
end

@kernel function load_boundary_region_kernel(boundary::B, result, arr, region_code::NTuple{N,Int}, neigh_dist, boundary_dims::NTuple{N,Bool}) where {B<:Tuple, N}
    raw_idx = @index(Global, Linear)

    # Convert linear index to Cartesian index
    idx = CartesianIndices(result)[raw_idx]

    # Compute source index for each dimension
    src_idx = ntuple(Val(N)) do d
        dim_boundary = boundary[d]
        nd = get_neigh_dist(neigh_dist, d)
        if !boundary_dims[d]
            if region_code[d] == -1
                lastindex(arr, d) - nd + idx[d]
            elseif region_code[d] == +1
                firstindex(arr, d) + idx[d] - 1
            else
                idx[d]
            end
        else
            boundary_source_index(dim_boundary, arr, region_code[d], nd, idx[d], d)
        end
    end

    # Get base value and apply boundary transformations dimension by dimension
    base_value = arr[CartesianIndex(src_idx)]
    result[idx] = _fold_boundary_value(boundary, base_value, arr, region_code, neigh_dist, boundary_dims, idx, src_idx, Val(1))
end

"""
Mixed boundary conditions. When a Tuple of boundary conditions is provided, each dimension uses its own boundary condition.
"""
function load_boundary_region(boundary::Tuple, arr, region_code::NTuple{N,Int}, neigh_dist, boundary_dims::NTuple{N,Bool}) where N
    # Compute the size of this halo region
    region_size = ntuple(N) do i
        region_code[i] == 0 ? size(arr, i) : get_neigh_dist(neigh_dist, i)
    end

    result = similar(arr, region_size)

    Kernel(load_boundary_region_kernel)(boundary, result, arr, region_code, neigh_dist, boundary_dims; ndrange=length(result))

    return move(task_processor(), result)
end

#############################################################################
# Chunk Selection and Halo Building
#############################################################################

function load_neighborhood_halos(chunks, idx, neigh_dist, boundary)
    validate_neigh_dist(neigh_dist)

    N = ndims(chunks)
    chunk_dist = 1
    nhalos = 3^N - 1
    halos = Vector{Any}(undef, nhalos)
    h = 0

    for i in 0:(3^N - 1)
        region_code = ntuple(N) do d
            ((i ÷ 3^(d-1)) % 3) - 1
        end
        all(==(0), region_code) && continue
        h += 1

        chunk_offset = CartesianIndex(ntuple(N) do d
            region_code[d] * chunk_dist
        end)
        new_idx = idx + chunk_offset

        if is_past_boundary(size(chunks), new_idx)
            boundary_dims = ntuple(N) do d
                new_idx[d] < 1 || new_idx[d] > size(chunks)[d]
            end
            if boundary_has_transition(boundary)
                new_idx = boundary_transition(boundary, new_idx, size(chunks))
            else
                new_idx = idx
            end
            chunk = chunks[new_idx]
            halos[h] = load_boundary_region(boundary, chunk, region_code, neigh_dist, boundary_dims)
        else
            chunk = chunks[new_idx]
            halos[h] = load_neighbor_region(chunk, region_code, neigh_dist)
        end
    end

    @assert h == nhalos
    return Tuple(halos)
end

function load_neighborhood_halos_from_deps(deps, idx, chunk_size, neigh_dist, boundary)
    validate_neigh_dist(neigh_dist)

    N = length(chunk_size)
    chunk_dist = 1
    nhalos = 3^N - 1
    halos = Vector{Any}(undef, nhalos)
    h = 0

    for i in 0:(3^N - 1)
        region_code = ntuple(N) do d
            ((i ÷ 3^(d-1)) % 3) - 1
        end
        all(==(0), region_code) && continue
        h += 1

        chunk_offset = CartesianIndex(ntuple(N) do d
            region_code[d] * chunk_dist
        end)
        new_idx = idx + chunk_offset

        chunk = deps[h+1]
        if is_past_boundary(chunk_size, new_idx)
            boundary_dims = ntuple(N) do d
                new_idx[d] < 1 || new_idx[d] > chunk_size[d]
            end
            halos[h] = load_boundary_region(boundary, chunk, region_code, neigh_dist, boundary_dims)
        else
            halos[h] = load_neighbor_region(chunk, region_code, neigh_dist)
        end
    end

    @assert h == nhalos
    return Tuple(halos)
end

function select_neighborhood_chunk_deps(chunks, idx, neigh_dist, boundary)
    validate_neigh_dist(neigh_dist)

    N = ndims(chunks)
    chunk_dist = 1

    accesses = Any[chunks[idx]]

    for i in 0:(3^N - 1)
        region_code = ntuple(N) do d
            ((i ÷ 3^(d-1)) % 3) - 1
        end
        all(==(0), region_code) && continue

        chunk_offset = CartesianIndex(ntuple(N) do d
            region_code[d] * chunk_dist
        end)
        new_idx = idx + chunk_offset

        if is_past_boundary(size(chunks), new_idx)
            if boundary_has_transition(boundary)
                new_idx = boundary_transition(boundary, new_idx, size(chunks))
            else
                new_idx = idx
            end
        end
        push!(accesses, chunks[new_idx])
    end

    @assert length(accesses) == 3^N
    return accesses
end

function build_chunk_halo(neigh_dist, boundary, idx, chunk_size, own_center::Bool, read_deps...)
    center = read_deps[1]
    halos = load_neighborhood_halos_from_deps(read_deps, idx, chunk_size, neigh_dist, boundary)
    return build_halo(neigh_dist, boundary, center, halos...; own_center=own_center)
end

function select_neighborhood_chunks(chunks, idx, neigh_dist, boundary)
    validate_neigh_dist(neigh_dist)

    N = ndims(chunks)
    # FIXME: Depends on neigh_dist and chunk size
    chunk_dist = 1

    # Get the center
    accesses = Any[chunks[idx]]

    # Iterate over all 3^N - 1 halo regions (excluding center)
    # Each region is identified by a code tuple where each element is -1, 0, or +1
    for i in 0:(3^N - 1)
        region_code = ntuple(N) do d
            ((i ÷ 3^(d-1)) % 3) - 1  # Maps 0,1,2 -> -1,0,+1
        end
        all(==(0), region_code) && continue  # Skip center

        # Compute the chunk offset for this region
        # For each dimension: -1 means go to previous chunk, +1 means go to next chunk, 0 means same chunk
        chunk_offset = CartesianIndex(ntuple(N) do d
            region_code[d] * chunk_dist
        end)
        new_idx = idx + chunk_offset

        if is_past_boundary(size(chunks), new_idx)
            # Compute which dimensions are actually past boundary
            boundary_dims = ntuple(N) do d
                new_idx[d] < 1 || new_idx[d] > size(chunks)[d]
            end
            if boundary_has_transition(boundary)
                new_idx = boundary_transition(boundary, new_idx, size(chunks))
            else
                new_idx = idx
            end
            chunk = chunks[new_idx]
            push!(accesses, Dagger.@spawn load_boundary_region(boundary, chunk, region_code, neigh_dist, boundary_dims))
        else
            chunk = chunks[new_idx]
            push!(accesses, Dagger.@spawn load_neighbor_region(chunk, region_code, neigh_dist))
        end
    end

    @assert length(accesses) == 3^N "Accesses mismatch: expected $(3^N), got $(length(accesses))"
    return accesses
end

# Returns (region_metadata, neighbor_chunk_dtasks) without spawning intermediate load tasks.
# region_metadata: Vector of (region_code, is_boundary, boundary_dims).
# neighbor_chunk_dtasks: Vector of raw chunk DTasks (resolved to arrays when build_halo_new runs).
function select_neighborhood_info(chunks, idx, neigh_dist, boundary)
    validate_neigh_dist(neigh_dist)
    N = ndims(chunks)
    chunk_dist = 1
    region_metadata = Tuple[]
    neighbor_chunks = Any[]

    for i in 0:(3^N - 1)
        region_code = ntuple(N) do d
            ((i ÷ 3^(d-1)) % 3) - 1
        end
        all(==(0), region_code) && continue

        chunk_offset = CartesianIndex(ntuple(N) do d
            region_code[d] * chunk_dist
        end)
        new_idx = idx + chunk_offset

        if is_past_boundary(size(chunks), new_idx)
            boundary_dims = ntuple(N) do d
                new_idx[d] < 1 || new_idx[d] > size(chunks)[d]
            end
            if boundary_has_transition(boundary)
                new_idx = boundary_transition(boundary, new_idx, size(chunks))
            else
                new_idx = idx
            end
            push!(region_metadata, (region_code, true, boundary_dims))
        else
            push!(region_metadata, (region_code, false, ntuple(_ -> false, N)))
        end
        push!(neighbor_chunks, chunks[new_idx])
    end

    @assert length(region_metadata) == 3^N - 1
    return region_metadata, neighbor_chunks
end

function build_halo(neigh_dist, boundary, center, all_halos...; own_center::Bool=false)
    N = ndims(center)
    expected_halos = 3^N - 1
    @assert length(all_halos) == expected_halos "Halo mismatch: N=$N expected $expected_halos halos, got $(length(all_halos))"
    center_data = own_center ? copy(center) : center
    return HaloArray(center_data, (all_halos...,), ntuple(i->get_neigh_dist(neigh_dist, i), N); own_center)
end

#############################################################################
# Fused halo construction
#############################################################################
#
# The `@stencil` fast path builds a chunk's `HaloArray` inside the same task that
# sweeps it, from the neighboring chunks passed as `In` dependencies. Compared to
# filling a cached `HaloArray` in a separate task this avoids (a) a second task per
# chunk per expression and (b) copying the whole center chunk, which for a
# double-buffered stencil is as much memory traffic as the sweep itself.

struct ViewHalos end
struct CopyHalos end

# Whether a boundary condition's halo regions are plain slices of a neighboring
# chunk, in which case they can be `view`s rather than materialized copies.
@inline halo_build_style(@nospecialize(boundary)) = CopyHalos()
@inline halo_build_style(::Wrap) = ViewHalos()
@inline halo_build_style(boundary::Tuple) = _combine_halo_style(map(halo_build_style, boundary))
@inline _combine_halo_style(::Tuple{}) = ViewHalos()
@inline _combine_halo_style(styles::Tuple) =
    first(styles) isa ViewHalos ? _combine_halo_style(Base.tail(styles)) : CopyHalos()

@inline function neighbor_region_view(arr, region_code::NTuple{N,Int}, neigh_dist) where N
    ranges = ntuple(Val(N)) do i
        nd = get_neigh_dist(neigh_dist, i)
        start_i = region_code[i] == -1 ? lastindex(arr, i) - nd + 1 : firstindex(arr, i)
        stop_i = region_code[i] == +1 ? firstindex(arr, i) + nd - 1 : lastindex(arr, i)
        start_i:stop_i
    end
    return view(arr, ranges...)
end

@inline function _build_fused_halos(::ViewHalos, neigh_dist, boundary, region_metadata,
                                    neighbor_chunks::NTuple{NH,Any}) where NH
    return ntuple(Val(NH)) do i
        region_code, _, _ = region_metadata[i]
        neighbor_region_view(neighbor_chunks[i], region_code, neigh_dist)
    end
end
@inline function _build_fused_halos(::CopyHalos, neigh_dist, boundary, region_metadata,
                                    neighbor_chunks::NTuple{NH,Any}) where NH
    return ntuple(Val(NH)) do i
        region_code, is_boundary, boundary_dims = region_metadata[i]
        chunk = neighbor_chunks[i]
        if is_boundary
            load_boundary_region(boundary, chunk, region_code, neigh_dist, boundary_dims)
        else
            load_neighbor_region(chunk, region_code, neigh_dist)
        end
    end
end

"""
    build_fused_halo(neigh_dist, boundary, region_metadata, center, neighbor_chunks...)

Wraps `center` (used in place, not copied) plus halo regions taken from
`neighbor_chunks` into a `HaloArray`. `region_metadata` is the per-region
`(region_code, is_boundary, boundary_dims)` triple precomputed on the submitting
task by `select_neighborhood_info`.
"""
function build_fused_halo(neigh_dist, boundary, region_metadata,
                          center::AbstractArray{T,N}, neighbor_chunks::Vararg{Any,NH}) where {T,N,NH}
    validate_neigh_dist(neigh_dist, size(center))
    halo_width = ntuple(i -> get_neigh_dist(neigh_dist, i), Val(N))
    halos = _build_fused_halos(halo_build_style(boundary), neigh_dist, boundary,
                               region_metadata, neighbor_chunks)
    return HaloArray(center, halos, halo_width; own_center=false)
end

"""
    stencil_source_chunks(read_chunks, write_chunks) -> chunks

The chunk array a neighborhood read should be taken from. Normally that is
`read_chunks` itself, but when the expression writes back into the same chunks it
reads (`A[idx] = f(@neighbors(A[idx]))`), neighbors must come from a snapshot
taken before any chunk is overwritten.
"""
function stencil_source_chunks(read_chunks, write_chunks)
    write_set = IdDict{Any,Nothing}()
    for chunk in write_chunks
        write_set[chunk] = nothing
    end
    overlaps = any(chunk -> haskey(write_set, chunk), read_chunks)
    overlaps || return read_chunks
    snapshot_tasks = map(chunk -> Dagger.@spawn(name="stencil_snapshot", copy(chunk)), read_chunks)
    return map(task -> fetch(task; raw=true), snapshot_tasks)
end

@inline load_neighborhood(arr::HaloArray{T,N}, idx) where {T,N} =
    StencilNeighborhood(arr, idx, arr.halo_width)
@inline load_neighborhood(arr::HaloInterior{T,N}, idx) where {T,N} =
    StencilNeighborhood(arr.parent, idx, arr.halo_width)

function inner_stencil!(f, output, read_vars)
    processor = task_processor()
    inner_stencil_proc!(processor, f, output, read_vars)
    # HaloArray lifetime is now managed by the DArray finalizer registered in
    # get_halo_inner_cache; do not unsafe_free! here to avoid use-after-free on cache hits.
end

# Non-KA (for CPUs)
function inner_stencil_proc!(::ThreadProc, f, output, read_vars)
    cpu_stencil_sweep!(f, output, read_vars)
    return
end

# Widest halo required by any neighborhood variable, per dimension. Zero in every
# dimension means no variable is accessed through a halo, so the whole chunk can
# take the fast path.
@inline _widen_halo(w::NTuple{N,Int}, ::Any) where N = w
@inline _widen_halo(w::NTuple{N,Int}, A::HaloArray{T,N}) where {T,N} =
    ntuple(i -> max(w[i], A.halo_width[i]), Val(N))
@inline _max_halo_width(vars::Tuple{}, w::NTuple{N,Int}) where N = w
@inline _max_halo_width(vars::Tuple, w::NTuple{N,Int}) where N =
    _max_halo_width(Base.tail(vars), _widen_halo(w, first(vars)))

# Interior stand-ins: HaloArrays become HaloInterior (direct center indexing),
# everything else is passed through untouched.
@inline _interior_var(A::HaloArray) = HaloInterior(A.center, A.halo_width)
@inline _interior_var(A) = A

"""
    cpu_stencil_sweep!(f, output, read_vars)

Applies `f` at every index of `output`, splitting the chunk into an interior
where all neighborhood accesses land in the center array, and a thin boundary
shell where they may reach into halos.

The split is what makes stencils fast: the interior (essentially the whole chunk)
runs against plain arrays under `@inbounds @simd` with `f` force-inlined, while
the generic, branch-heavy `HaloArray` path is confined to the shell.
"""
function cpu_stencil_sweep!(f::F, output::AbstractArray{T,N}, read_vars::NamedTuple) where {F,T,N}
    w = _max_halo_width(values(read_vars), ntuple(_ -> 0, Val(N)))
    ax = axes(output)

    interior_ax = ntuple(Val(N)) do i
        (first(ax[i]) + w[i]):(last(ax[i]) - w[i])
    end
    if any(isempty, interior_ax)
        # Halos are as wide as the chunk itself; there is no interior to split off.
        for idx in CartesianIndices(output)
            @inline f(idx, output, read_vars)
        end
        return
    end

    interior_vars = map(_interior_var, read_vars)
    @inbounds @simd for idx in CartesianIndices(interior_ax)
        @inline f(idx, output, interior_vars)
    end

    all(iszero, w) && return

    # Boundary shell, decomposed into 2N disjoint slabs: for dimension `d`, the
    # low and high slabs span the full extent in dimensions after `d` and only the
    # interior extent in dimensions before `d`.
    for d in 1:N
        for low_side in (true, false)
            slab_ax = ntuple(Val(N)) do i
                if i < d
                    interior_ax[i]
                elseif i == d
                    low_side ? (first(ax[i]):(first(ax[i]) + w[i] - 1)) :
                               ((last(ax[i]) - w[i] + 1):last(ax[i]))
                else
                    first(ax[i]):last(ax[i])
                end
            end
            for idx in CartesianIndices(slab_ax)
                @inline f(idx, output, read_vars)
            end
        end
    end
    return
end

#############################################################################
# @stencil Macro
#############################################################################

"""
    @stencil begin body end

Allows the execution of stencil operations within a `spawn_datadeps` region.
The `idx` variable is used to iterate over one or more `DArray`s. An example
usage may look like:

```julia
import Dagger: @stencil, Wrap

A = zeros(Blocks(3, 3), Int, 9, 9)
A[5, 5] = 1
B = zeros(Blocks(3, 3), Int, 9, 9)
Dagger.spawn_datadeps() do
    @stencil begin
        # Increment all values by 1
        A[idx] = A[idx] + 1
        # Sum values of all neighbors with self and write to B
        B[idx] = sum(@neighbors(A[idx], 1, Wrap()))
        # Copy B back to A
        A[idx] = B[idx]
    end
end
```

Each expression within an `@stencil` region that performs an in-place indexing
expression like `A[idx] = ...` is transformed into a set of tasks that operate
on each chunk of `A` or any other arrays specified as `A[idx]`; within each
task, elements of that chunk of `A` can be accessed. Elements of multiple
`DArray`s can be accessed, such as `B[idx]`, so long as `B` has the same size,
shape, and chunk layout as `A`.

Additionally, the `@neighbors` macro can be used to access a neighborhood of
values around `A[idx]`, at a configurable distance (in this case, 1 element
distance) and with various kinds of boundary conditions (in this case, `Wrap()`
specifies wrapping behavior on the boundaries). Neighborhoods are computed with
respect to neighboring chunks as well - if a neighborhood would overflow from
the current chunk into a neighboring chunk, values from that neighboring chunk
will be included in the neighborhood.

Note that, while `@stencil` may look like a `for` loop, it does not follow the
same semantics; in particular, an expression within `@stencil` occurs "all at
once" (across all indices) before the next expression occurs. This means that
`A[idx] = A[idx] + 1` increments the values `A` by 1, which occurs before
`B[idx] = sum(@neighbors(A[idx], 1, Wrap()))` writes the sum of neighbors for
all `idx` values into `B[idx]`, and that occurs before any of the values are
copied to `A` in `A[idx] = B[idx]`. Of course, pipelining and other optimizations
may still occur, so long as they respect the sequential nature of `@stencil`
(just like with other operations in `spawn_datadeps`). Due to this behavior,
expressions like `A[idx] = sum(@neighbors(A[idx], 1, Wrap()))` are not valid,
as that would currently cause race conditions and lead to undefined behavior.
"""
macro stencil(orig_ex)
    if !Meta.isexpr(orig_ex, :block)
        orig_ex = Expr(:block, orig_ex)
    end

    # Collect access pattern information
    inners = []
    all_accessed_vars = Set{Symbol}()
    for inner_ex in orig_ex.args
        inner_ex isa LineNumberNode && continue

        # Lower update operators to standard assignments
        if inner_ex isa Expr && inner_ex.head in (:(+=), :(-=), :(*=), :(/=), :(\=), :(%=), :(^=), :(&=), :(|=), :(⊻=), :(<<=), :(>>=), :(>>>=))
            op = Symbol(string(inner_ex.head)[1:end-1])
            inner_ex = Expr(:(=), inner_ex.args[1], Expr(:call, op, inner_ex.args[1], inner_ex.args[2]))
        end

        # Determine if this is an assignment or a naked expression
        is_allocation = false
        if @capture(inner_ex, w_ex_ = r_ex_)
            if !@capture(w_ex, w_var_[w_idx_])
                throw(ArgumentError("Update expression requires a write to an index: $w_ex"))
            end
            write_var = w_var
            write_idx = w_idx
            read_ex = r_ex
        else
            read_ex = inner_ex
            is_allocation = true
            write_var = nothing
            write_idx = nothing
        end

        accessed_vars = Set{Symbol}()
        read_vars = Set{Symbol}()
        neighborhoods = Dict{Symbol, Tuple{Any, Any}}()
        source_var = nothing
        prewalk(read_ex) do read_inner_ex
            if @capture(read_inner_ex, r_var_[r_idx_])
                if isnothing(write_idx)
                    write_idx = r_idx
                end
                if isnothing(source_var)
                    source_var = r_var
                end
                if r_idx == write_idx
                    push!(accessed_vars, r_var)
                    push!(read_vars, r_var)
                end
            elseif @capture(read_inner_ex, @neighbors(r_var_[r_idx_], neigh_dist_, boundary_))
                if isnothing(write_idx)
                    write_idx = r_idx
                end
                if isnothing(source_var)
                    source_var = r_var
                end
                if r_idx == write_idx
                    push!(accessed_vars, r_var)
                    push!(read_vars, r_var)
                    neighborhoods[r_var] = (neigh_dist, boundary)
                else
                    throw(ArgumentError("Neighborhood access must be at the same index: $read_inner_ex"))
                end
            end
            return read_inner_ex
        end

        if isnothing(write_idx)
            throw(ArgumentError("Invalid stencil expression (no index found): $inner_ex"))
        end
        if is_allocation
            if isnothing(source_var)
                throw(ArgumentError("Could not find a source DArray in expression: $inner_ex"))
            end
            write_var = gensym("out")
            inner_ex = :($write_var[$write_idx] = $read_ex)
        end

        push!(accessed_vars, write_var)
        union!(all_accessed_vars, accessed_vars)
        push!(inners, (;inner_ex, accessed_vars, write_var, write_idx, read_ex, read_vars, neighborhoods, is_allocation, source_var))
    end

    # Codegen update functions
    final_ex = Expr(:block)

    # 1. Allocations (outside spawn_datadeps)
    for inner in inners
        if inner.is_allocation
            push!(final_ex.args, :($(inner.write_var) = similar($(inner.source_var))))
        end
    end

    # 2. Stencil operations: one spawn_datadeps region per expression.
    # Because spawn_datadeps blocks until all its tasks complete, each expression's
    # region fully finishes before the next expression's halo tasks are spawned.
    # This means HaloArray allocations can always live outside spawn_datadeps,
    # avoiding Datadeps aliasing issues unconditionally.
    for (;inner_ex, accessed_vars, write_var, write_idx, read_ex, read_vars, neighborhoods, is_allocation, source_var) in inners
        # Generate a variable for chunk access
        @gensym chunk_idx

        # Generate function with transformed body
        @gensym inner_vars inner_index_var inner_write_var
        new_inner_ex_body = prewalk(inner_ex) do old_inner_ex
            if @capture(old_inner_ex, read_var_[read_idx_]) && read_idx == write_idx
                # Direct access
                if read_var == write_var
                    return :($inner_write_var[$inner_index_var])
                else
                    return :($inner_vars.$read_var[$inner_index_var])
                end
            elseif @capture(old_inner_ex, @neighbors(read_var_[read_idx_], neigh_dist_, boundary_))
                # Neighborhood access
                return :($load_neighborhood($inner_vars.$read_var, $inner_index_var))
            end
            return old_inner_ex
        end
        new_inner_f = :(($inner_index_var, $inner_write_var, $inner_vars)->$new_inner_ex_body)
        actual_read_vars = filter(v -> (v != write_var) || (v in keys(neighborhoods)), collect(read_vars))

        # 2a. For each neighborhood read_var, pre-compute on the main task (so no DArray
        # is ever passed into @spawn) the chunk array to read neighbors from and, per
        # chunk, the region metadata plus the neighboring chunks themselves.
        #
        # `stencil_source_chunks` substitutes a snapshot when the expression writes back
        # into the chunks it reads (`A[idx] = f(@neighbors(A[idx]))`); everywhere else it
        # is the identity, and the neighbor chunks are read directly.
        neigh_sym_map = Dict{Symbol, NamedTuple}()
        for read_var in read_vars
            if read_var in keys(neighborhoods)
                neigh_dist, boundary = neighborhoods[read_var]
                @gensym region_info_table src_chunks region_meta neighbor_cks
                neigh_sym_map[read_var] = (; region_info_table, src_chunks)
                push!(final_ex.args, :($validate_neigh_dist($neigh_dist, ndims($read_var))))
                push!(final_ex.args, :($src_chunks = $stencil_source_chunks($chunks($read_var), $chunks($write_var))))
                push!(final_ex.args, :($region_info_table = Array{Any}(undef, size($src_chunks))))
                push!(final_ex.args, quote
                    for $chunk_idx in $CartesianIndices($src_chunks)
                        ($region_meta, $neighbor_cks) = $select_neighborhood_info($src_chunks, $chunk_idx, $neigh_dist, $boundary)
                        $region_info_table[$chunk_idx] = (tuple($region_meta...), $neighbor_cks)
                    end
                end)
            end
        end

        # 2b. Build the per-chunk task. Neighborhood variables are passed as their own
        # chunk followed by their `3^N - 1` neighboring chunks (a runtime-length group,
        # hence the positional splat); the task assembles the HaloArray itself.
        prologue_exs = Expr[]     # runs per chunk on the submitting task
        arg_exs = Any[]           # positional args of the spawned task
        unpack_exs = Expr[]       # runs inside the task, binds each read var
        local_vars = Any[]        # task-local binding holding each read var's chunk data
        @gensym task_args arg_offset
        for read_var in actual_read_vars
            # N.B. Bind into a gensym rather than `read_var` itself: the task closure is
            # nested inside the user's scope, so assigning `read_var` there would rebind
            # the user's DArray variable instead of creating a task-local binding.
            local_var = gensym(read_var)
            if read_var in keys(neighborhoods)
                neigh_dist, boundary = neighborhoods[read_var]
                syms = neigh_sym_map[read_var]
                @gensym region_meta neighbor_cks nneighbors
                push!(prologue_exs, quote
                    ($region_meta, $neighbor_cks) = $(syms.region_info_table)[$chunk_idx]
                    $nneighbors = length($neighbor_cks)
                end)
                push!(arg_exs, :($Read($(syms.src_chunks)[$chunk_idx])))
                push!(arg_exs, Expr(:..., :(map($Read, $neighbor_cks))))
                push!(unpack_exs, quote
                    $local_var = $build_fused_halo($neigh_dist, $boundary, $region_meta,
                                                   $task_args[$arg_offset + 1],
                                                   $task_args[($arg_offset + 2):($arg_offset + 1 + $nneighbors)]...)
                    $arg_offset += 1 + $nneighbors
                end)
            elseif read_var != write_var
                push!(arg_exs, :($Read($chunks($read_var)[$chunk_idx])))
                push!(unpack_exs, quote
                    $local_var = $task_args[$arg_offset + 1]
                    $arg_offset += 1
                end)
            end
            push!(local_vars, local_var)
        end
        write_dep_ex = if write_var in read_vars
            :($ReadWrite($chunks($write_var)[$chunk_idx]))
        else
            :($Write($chunks($write_var)[$chunk_idx]))
        end

        # The kernel body may mention an accessed variable bare (e.g. `length(A)`), which
        # would otherwise capture the user's DArray into the spawned closure -- illegal,
        # since Datadeps analyzes the closure's captures for aliasing. Shadow each
        # accessed name with a parameter bound to that chunk's local data, so the body
        # sees the chunk (with halos, where applicable) instead.
        shadow_params = copy(actual_read_vars)
        shadow_args = copy(local_vars)
        if !(write_var in actual_read_vars)
            push!(shadow_params, write_var)
            push!(shadow_args, inner_write_var)
        end
        shadow_body = quote
            $inner_vars = (;$([Expr(:kw, v, v) for v in actual_read_vars]...))
            $inner_stencil!($new_inner_f, $inner_write_var, $inner_vars)
        end
        shadow_fn = Expr(:->, Expr(:tuple, shadow_params...), shadow_body)
        inner_fn_body = quote
            $arg_offset = 0
            $(unpack_exs...)
            $shadow_fn($(shadow_args...))
        end
        inner_fn = Expr(:->, Expr(:tuple, inner_write_var, Expr(:..., task_args)), inner_fn_body)
        inner_spawn_ex = Expr(:block, prologue_exs...,
                              :(Dagger.@spawn name="stencil_inner_fn" $inner_fn($write_dep_ex, $(arg_exs...))))

        # 2c. One spawn_datadeps region per expression, one task per chunk. Because the
        # region blocks until all of its tasks finish, the next expression's tasks are
        # only submitted once this expression has been applied everywhere, which is what
        # gives `@stencil` its "all at once" semantics.
        push!(final_ex.args, :(Dagger.spawn_datadeps() do
            for $chunk_idx in $CartesianIndices($chunks($write_var))
                $inner_spawn_ex
            end
        end))
    end

    # 3. Return last allocated var if applicable
    if !isempty(inners) && inners[end].is_allocation
        push!(final_ex.args, inners[end].write_var)
    end

    return esc(final_ex)
end
