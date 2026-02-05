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
    # FIXME: Don't collect
    return move(task_processor(), collect(@view arr[start_idx:stop_idx]))
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
    # FIXME: return Fill(pad.padval, region_size)
    return move(task_processor(), fill(pad.padval, region_size))
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

function load_boundary_region(::Clamp, arr, region_code::NTuple{N,Int}, neigh_dist, boundary_dims::NTuple{N,Bool}) where N
    # Compute the size of this halo region
    region_size = ntuple(N) do i
        region_code[i] == 0 ? size(arr, i) : get_neigh_dist(neigh_dist, i)
    end

    result = similar(arr, region_size)

    for idx in CartesianIndices(result)
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

function load_boundary_region(::LinearExtrapolate, arr::AbstractArray{T}, region_code::NTuple{N,Int}, neigh_dist, boundary_dims::NTuple{N,Bool}) where {T<:Real,N}
    # Compute the size of this halo region
    region_size = ntuple(N) do i
        region_code[i] == 0 ? size(arr, i) : get_neigh_dist(neigh_dist, i)
    end

    result = similar(arr, region_size)

    for idx in CartesianIndices(result)
        # Find the first boundary dimension that needs extrapolation
        extrap_dim = 0
        for d in 1:N
            if boundary_dims[d] && region_code[d] != 0
                extrap_dim = d
                break
            end
        end

        if extrap_dim == 0
            # No boundary dimensions - normal neighbor access
            src_idx = CartesianIndex(ntuple(N) do i
                nd = get_neigh_dist(neigh_dist, i)
                if region_code[i] == -1
                    lastindex(arr, i) - nd + idx[i]
                elseif region_code[i] == +1
                    firstindex(arr, i) + idx[i] - 1
                else
                    idx[i]
                end
            end)
            result[idx] = arr[src_idx]
        else
            # Extrapolate along extrap_dim, clamp other boundary dimensions
            nd = get_neigh_dist(neigh_dist, extrap_dim)

            # Compute base index (for other dimensions, clamp if at boundary)
            base_idx = ntuple(N) do i
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
                idx1 = ntuple(i -> i == extrap_dim ? firstindex(arr, i) : base_idx[i], N)
                idx2 = ntuple(i -> i == extrap_dim ? firstindex(arr, i) + 1 : base_idx[i], N)
                slope = arr[CartesianIndex(idx2)] - arr[CartesianIndex(idx1)]
                dist = -(nd - idx[extrap_dim] + 1)
                result[idx] = arr[CartesianIndex(idx1)] + slope * dist
            else
                # High boundary: slope = arr[end] - arr[end-1]
                idx1 = ntuple(i -> i == extrap_dim ? lastindex(arr, i) - 1 : base_idx[i], N)
                idx2 = ntuple(i -> i == extrap_dim ? lastindex(arr, i) : base_idx[i], N)
                slope = arr[CartesianIndex(idx2)] - arr[CartesianIndex(idx1)]
                dist = idx[extrap_dim]
                result[idx] = arr[CartesianIndex(idx2)] + slope * dist
            end
        end
    end

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

    region = move(task_processor(), collect(@view arr[start_idx:stop_idx]))

    # Reverse only along dimensions that are actually being reflected
    # (both non-zero in region_code AND past boundary)
    for i in 1:N
        if region_code[i] != 0 && boundary_dims[i]
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

"""
Mixed boundary conditions. When a Tuple of boundary conditions is provided, each dimension uses its own boundary condition.
"""
function load_boundary_region(boundary::Tuple, arr, region_code::NTuple{N,Int}, neigh_dist, boundary_dims::NTuple{N,Bool}) where N
    # Compute the size of this halo region
    region_size = ntuple(N) do i
        region_code[i] == 0 ? size(arr, i) : get_neigh_dist(neigh_dist, i)
    end

    result = similar(arr, region_size)

    for idx in CartesianIndices(result)
        # For each element, compute its value based on per-dimension boundary conditions
        # Start by finding the source index in the array
        src_idx = ntuple(N) do d
            dim_boundary = get_boundary(boundary, d)
            compute_source_index_for_dim(dim_boundary, arr, region_code, neigh_dist, boundary_dims, idx, d)
        end

        # Compute the value using per-dimension logic
        value = compute_boundary_value(boundary, arr, region_code, neigh_dist, boundary_dims, idx, src_idx)
        result[idx] = value
    end

    return move(task_processor(), result)
end

#############################################################################
# Chunk Selection and Halo Building
#############################################################################

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

function build_halo(neigh_dist, boundary, center, all_halos...)
    N = ndims(center)
    expected_halos = 3^N - 1
    @assert length(all_halos) == expected_halos "Halo mismatch: N=$N expected $expected_halos halos, got $(length(all_halos))"
    return HaloArray(center, (all_halos...,), ntuple(i->get_neigh_dist(neigh_dist, i), N))
end

function load_neighborhood(arr::HaloArray{T,N}, idx) where {T,N}
    start_idx = idx - CartesianIndex(ntuple(i->arr.halo_width[i], ndims(arr)))
    stop_idx = idx + CartesianIndex(ntuple(i->arr.halo_width[i], ndims(arr)))
    return @view arr[start_idx:stop_idx]
end

function inner_stencil!(f, output, read_vars)
    processor = task_processor()
    inner_stencil_proc!(processor, f, output, read_vars)
end

# Non-KA (for CPUs)
function inner_stencil_proc!(::ThreadProc, f, output, read_vars)
    for idx in CartesianIndices(output)
        f(idx, output, read_vars)
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
        throw(ArgumentError("Invalid stencil block: $orig_ex"))
    end

    # Collect access pattern information
    inners = []
    all_accessed_vars = Set{Symbol}()
    for inner_ex in orig_ex.args
        inner_ex isa LineNumberNode && continue
        if !@capture(inner_ex, write_ex_ = read_ex_)
            throw(ArgumentError("Invalid update expression: $inner_ex"))
        end
        if !@capture(write_ex, write_var_[write_idx_])
            throw(ArgumentError("Update expression requires a write: $write_ex"))
        end
        accessed_vars = Set{Symbol}()
        read_vars = Set{Symbol}()
        neighborhoods = Dict{Symbol, Tuple{Any, Any}}()
        push!(accessed_vars, write_var)
        prewalk(read_ex) do read_inner_ex
            if @capture(read_inner_ex, read_var_[read_idx_]) && read_idx == write_idx
                push!(accessed_vars, read_var)
                push!(read_vars, read_var)
            elseif @capture(read_inner_ex, @neighbors(read_var_[read_idx_], neigh_dist_, boundary_))
                if read_idx != write_idx
                    throw(ArgumentError("Neighborhood access must be at the same index as the write: $read_inner_ex"))
                end
                if write_var == read_var
                    throw(ArgumentError("Cannot write to the same variable as the neighborhood access: $read_inner_ex"))
                end
                push!(accessed_vars, read_var)
                push!(read_vars, read_var)
                neighborhoods[read_var] = (neigh_dist, boundary)
            end
            return read_inner_ex
        end
        union!(all_accessed_vars, accessed_vars)
        push!(inners, (;inner_ex, accessed_vars, write_var, write_idx, read_ex, read_vars, neighborhoods))
    end

    # Codegen update functions
    final_ex = Expr(:block)
    @gensym chunk_idx
    for (;inner_ex, accessed_vars, write_var, write_idx, read_ex, read_vars, neighborhoods) in inners
        # Generate a variable for chunk access
        @gensym chunk_idx

        # Generate function with transformed body
        @gensym inner_vars inner_index_var
        new_inner_ex_body = prewalk(inner_ex) do old_inner_ex
            if @capture(old_inner_ex, read_var_[read_idx_]) && read_idx == write_idx
                # Direct access
                if read_var == write_var
                    return :($write_var[$inner_index_var])
                else
                    return :($inner_vars.$read_var[$inner_index_var])
                end
            elseif @capture(old_inner_ex, @neighbors(read_var_[read_idx_], neigh_dist_, boundary_))
                # Neighborhood access
                return :($load_neighborhood($inner_vars.$read_var, $inner_index_var))
            end
            return old_inner_ex
        end
        new_inner_f = :(($inner_index_var, $write_var, $inner_vars)->$new_inner_ex_body)
        new_inner_ex = quote
            $inner_vars = (;$(read_vars...))
            $inner_stencil!($new_inner_f, $write_var, $inner_vars)
        end
        inner_fn = Expr(:->, Expr(:tuple, Expr(:parameters, write_var, read_vars...)), new_inner_ex)

        # Generate @spawn call with appropriate vars and deps
        deps_ex = Any[]
        if write_var in read_vars
            push!(deps_ex, Expr(:kw, write_var, :($ReadWrite($chunks($write_var)[$chunk_idx]))))
        else
            push!(deps_ex, Expr(:kw, write_var, :($Write($chunks($write_var)[$chunk_idx]))))
        end
        neighbor_copy_all_ex = Expr(:block)
        for read_var in read_vars
            if read_var in keys(neighborhoods)
                # Generate a neighborhood copy operation
                neigh_dist, boundary = neighborhoods[read_var]
                deps_inner_ex = Expr(:block)
                @gensym neighbor_copy_var
                push!(neighbor_copy_all_ex.args, :($neighbor_copy_var = Dagger.@spawn name="stencil_build_halo" $build_halo($neigh_dist, $boundary, map($Read, $select_neighborhood_chunks($chunks($read_var), $chunk_idx, $neigh_dist, $boundary))...)))
                push!(deps_ex, Expr(:kw, read_var, :($Read($neighbor_copy_var))))
            else
                push!(deps_ex, Expr(:kw, read_var, :($Read($chunks($read_var)[$chunk_idx]))))
            end
        end
        spawn_ex = :(Dagger.@spawn name="stencil_inner_fn" $inner_fn(;$(deps_ex...)))

        # Generate loop
        push!(final_ex.args, quote
            for $chunk_idx in $CartesianIndices($chunks($write_var))
                $neighbor_copy_all_ex
                $spawn_ex
            end
        end)
    end


    return esc(final_ex)
end
