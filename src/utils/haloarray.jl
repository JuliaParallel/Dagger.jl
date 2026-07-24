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
    own_center::Bool
end

function HaloArray(center, halos::Tuple, halo_width::NTuple{N,Int}; own_center::Bool=false) where N
    T = eltype(center)
    return HaloArray{T,N,typeof(center),typeof(halos)}(center, halos, halo_width, own_center)
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

    return HaloArray(center, halos, halo_width; own_center=true)
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
    return HaloArray(center, halos, halo_width; own_center=true)
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

Base.IndexStyle(::Type{<:HaloArray}) = IndexCartesian()

# GPU-friendly reductions for SubArray views of HaloArray.
# The standard reduction path uses _foldl_impl/iterate which produces Union
# types that SPIR-V and other GPU compilers can't handle, and passes through
# keyword-argument forwarding that triggers dynamic dispatch on GPU.
# These overrides use CartesianIndices iteration which compiles cleanly.
@inline function Base.mapreduce(f::F, op::OP, A::SubArray{T,N,<:HaloArray}) where {F,OP,T,N}
    first_idx = CartesianIndex(ntuple(d -> firstindex(A, d), Val(N)))
    result = f(@inbounds A[first_idx])
    @inbounds for idx in CartesianIndices(A)
        idx == first_idx && continue
        result = op(result, f(A[idx]))
    end
    return result
end
@inline function Base.sum(A::SubArray{T,N,<:HaloArray}) where {T,N}
    first_idx = CartesianIndex(ntuple(d -> firstindex(A, d), Val(N)))
    result = @inbounds A[first_idx]
    @inbounds for idx in CartesianIndices(A)
        idx == first_idx && continue
        result += A[idx]
    end
    return result
end
@inline function Base.sum(f::F, A::SubArray{T,N,<:HaloArray}) where {F,T,N}
    first_idx = CartesianIndex(ntuple(d -> firstindex(A, d), Val(N)))
    result = f(@inbounds A[first_idx])
    @inbounds for idx in CartesianIndices(A)
        idx == first_idx && continue
        result += f(A[idx])
    end
    return result
end

Adapt.adapt_structure(to, H::Dagger.HaloArray) =
    HaloArray(Adapt.adapt(to, H.center),
              Adapt.adapt.(Ref(to), H.halos),
              H.halo_width;
              own_center=H.own_center)

function aliasing(A::HaloArray)
    return CombinedAliasing([aliasing(A.center), map(aliasing, A.halos)...])
end
memory_space(A::HaloArray) = memory_space(A.center)
# A HaloArray's chunk record must be labeled with the memory space of its backing
# storage. `value_memory_space` is what `tochunk` uses to label a task result, and
# GPU extensions only define it for device array types (CuArray, CLArray, ...); a
# GPU-backed HaloArray would otherwise fall back to the generic CPU space, so its
# chunk would be mislabeled `CPURAMMemorySpace` while wrapping GPU arrays. That
# mislabeling breaks datadeps aliasing/move tracking on every GPU backend (the
# "Aliasing mismatch!" assertion in the stencil suite). Delegate to the center.
value_memory_space(A::HaloArray) = value_memory_space(A.center)

# Header+children: transfer center and each halo, rebuild with halo_width
move_rewrap_parts(A::HaloArray) = ((A.center, A.halos...), A.halo_width)
function move_rewrap_build(::Type{<:HaloArray}, children, halo_width)
    return HaloArray(children[1], children[2:end], halo_width)
end
function move_rewrap_child_types(::Type{HA}) where {HA<:HaloArray}
    A = HA.parameters[3]
    H = HA.parameters[4]
    return (A, H.parameters...)
end
move_rewrap_header_mode(::Type{<:HaloArray}) = :broadcast
function move_rewrap_result_type(::Type{<:HaloArray}, child_cts, halo_width)
    A = child_cts[1]
    H = Tuple{child_cts[2:end]...}
    return HaloArray{eltype(A),ndims(A),A,H}
end

function Dagger.unsafe_free!(A::HaloArray)
    A.own_center && unsafe_free!(A.center)
    foreach(unsafe_free!, A.halos)
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
