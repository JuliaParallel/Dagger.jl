# Define the HaloArray type with generalized halo storage for any dimensionality
# Each halo region is identified by a "region code" - an N-tuple where each element is:
#   -1 = low halo (index < 1)
#    0 = center (1 <= index <= center_size)
#   +1 = high halo (index > center_size)
# There are 3^N - 1 halo regions (excluding the all-zeros center code)

struct HaloArray{T,N,A<:AbstractArray{T,N},H<:Tuple} <: AbstractArray{T,N}
    center::A
    halos::H  # Tuple of 3^N - 1 arrays in canonical order
    halo_width::NTuple{N,Int}
end

# Number of halo regions for N dimensions
num_halo_regions(N::Int) = 3^N - 1

# Generate all region codes in canonical order (excluding center)
# Order: iterate through base-3 representation, skip the center (all zeros)
function all_region_codes(::Val{N}) where N
    codes = NTuple{N,Int}[]
    for i in 0:(3^N - 1)
        code = ntuple(N) do d
            ((i ÷ 3^(d-1)) % 3) - 1  # Maps 0,1,2 -> -1,0,+1
        end
        if !all(==(0), code)  # Skip center
            push!(codes, code)
        end
    end
    return Tuple(codes)
end

# Convert region code to flat index (1 to 3^N - 1)
# The index is based on treating the code as a base-3 number (with offset)
@inline function region_index(code::NTuple{N,Int}) where N
    # Map: -1 → 0, 0 → 1, +1 → 2, treat as base-3 number
    raw = 0
    for i in 1:N
        raw += (code[i] + 1) * 3^(i-1)
    end
    # Center (all zeros) maps to raw index where all digits are 1
    # center_raw = sum(1 * 3^(i-1) for i in 1:N) = (3^N - 1) / 2
    center_raw = (3^N - 1) ÷ 2
    # Adjust index to skip center
    return raw < center_raw ? raw + 1 : raw
end

# Compute the size of a halo region given its code
@inline function halo_region_size(center_size::NTuple{N,Int}, halo_width::NTuple{N,Int}, code::NTuple{N,Int}) where N
    return ntuple(N) do i
        code[i] == 0 ? center_size[i] : halo_width[i]
    end
end

# Helper function to create an empty HaloArray with generalized halo storage
function HaloArray{T,N}(center_size::NTuple{N,Int}, halo_width::NTuple{N,Int}) where {T,N}
    center = Array{T,N}(undef, center_size...)

    # Create all 3^N - 1 halo regions
    codes = all_region_codes(Val(N))
    halos = ntuple(length(codes)) do i
        code = codes[i]
        region_size = halo_region_size(center_size, halo_width, code)
        Array{T,N}(undef, region_size...)
    end

    return HaloArray{T,N,typeof(center),typeof(halos)}(center, halos, halo_width)
end

Base.size(tile::HaloArray) = size(tile.center) .+ 2 .* tile.halo_width
function Base.axes(tile::HaloArray{T,N}) where {T,N}
    ntuple(N) do i
        first_ind = 1 - tile.halo_width[i]
        last_ind = size(tile.center, i) + tile.halo_width[i]
        return first_ind:last_ind
    end
end
function Base.similar(tile::HaloArray{T,N}, ::Type{T}, dims::NTuple{N,Int}) where {T,N}
    center_size = dims
    halo_width = tile.halo_width
    return HaloArray{T,N}(center_size, halo_width)
end
function Base.copy(tile::HaloArray{T,N}) where {T,N}
    center = copy(tile.center)
    halos = ntuple(i -> copy(tile.halos[i]), length(tile.halos))
    halo_width = tile.halo_width
    return HaloArray(center, halos, halo_width)
end

# Compute the region code for a given index
@inline function compute_region_code(tile::HaloArray{T,N}, I::NTuple{N,Int}) where {T,N}
    return ntuple(N) do i
        if I[i] < 1
            -1
        elseif I[i] > size(tile.center, i)
            +1
        else
            0
        end
    end
end

# Compute local index within a halo region
@inline function compute_local_index(tile::HaloArray{T,N}, I::NTuple{N,Int}, code::NTuple{N,Int}) where {T,N}
    return ntuple(N) do i
        if code[i] == -1
            I[i] + tile.halo_width[i]
        elseif code[i] == +1
            I[i] - size(tile.center, i)
        else
            I[i]  # Inside this dimension, keep index
        end
    end
end

# Define getindex for HaloArray
@inline function Base.getindex(tile::HaloArray{T,N}, I::Vararg{Int,N}) where {T,N}
    Base.@boundscheck checkbounds(tile, I...)
    code = compute_region_code(tile, I)

    if all(==(0), code)
        # Center
        return @inbounds tile.center[I...]
    else
        # Halo region
        idx = region_index(code)
        local_idx = compute_local_index(tile, I, code)
        return @inbounds tile.halos[idx][local_idx...]
    end
end

# Define setindex! for HaloArray
@inline function Base.setindex!(tile::HaloArray{T,N}, value, I::Vararg{Int,N}) where {T,N}
    Base.@boundscheck checkbounds(tile, I...)
    code = compute_region_code(tile, I)

    if all(==(0), code)
        # Center
        return @inbounds tile.center[I...] = value
    else
        # Halo region
        idx = region_index(code)
        local_idx = compute_local_index(tile, I, code)
        return @inbounds tile.halos[idx][local_idx...] = value
    end
end

Adapt.adapt_structure(to, H::Dagger.HaloArray) =
    HaloArray(Adapt.adapt(to, H.center),
              Adapt.adapt.(Ref(to), H.halos),
              H.halo_width)

function aliasing(A::HaloArray)
    return CombinedAliasing([aliasing(A.center), map(aliasing, A.halos)...])
end
memory_space(A::HaloArray) = memory_space(A.center)

function move_rewrap(cache::AliasedObjectCache, from_proc::Processor, to_proc::Processor, from_space::MemorySpace, to_space::MemorySpace, A::HaloArray)
    center_chunk = rewrap_aliased_object!(cache, from_proc, to_proc, from_space, to_space, A.center)
    halo_chunks = ntuple(i -> rewrap_aliased_object!(cache, from_proc, to_proc, from_space, to_space, A.halos[i]), length(A.halos))
    halo_width = A.halo_width
    to_w = root_worker_id(to_proc)
    return remotecall_fetch(to_w, from_proc, to_proc, from_space, to_space, center_chunk, halo_chunks, halo_width) do from_proc, to_proc, from_space, to_space, center_chunk, halo_chunks, halo_width
        center_new = move(from_proc, to_proc, center_chunk)
        halos_new = ntuple(i -> move(from_proc, to_proc, halo_chunks[i]), length(halo_chunks))
        return tochunk(HaloArray(center_new, halos_new, halo_width), to_proc)
    end
end

function find_object_holding_ptr(object::HaloArray, ptr::UInt64)
    for i in 1:length(object.halos)
        halo = object.halos[i]
        span = LocalMemorySpan(pointer(halo), length(halo)*sizeof(eltype(halo)))
        if span_start(span) <= ptr <= span_end(span)
            return halo
        end
    end
    center = object.center
    span = LocalMemorySpan(pointer(center), length(center)*sizeof(eltype(center)))
    @assert span_start(span) <= ptr <= span_end(span) "Pointer $ptr not found in HaloArray"
    return center
end
