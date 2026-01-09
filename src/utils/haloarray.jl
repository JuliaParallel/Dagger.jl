# Define the HaloArray type with minimized halo storage
struct HaloArray{T,N,E,C,A,EAT<:Tuple,CAT<:Tuple} <: AbstractArray{T,N}
    center::A
    edges::EAT
    corners::CAT
    halo_width::NTuple{N,Int}
end

# Helper function to create an empty HaloArray with minimized halo storage
function HaloArray{T,N}(center_size::NTuple{N,Int}, halo_width::NTuple{N,Int}) where {T,N}
    center = Array{T,N}(undef, center_size...)
    edges = ntuple(2N) do i
        prev_dims = center_size[1:(cld(i,2)-1)]
        next_dims = center_size[(cld(i,2)+1):end]
        return Array{T,N}(undef, prev_dims..., halo_width[cld(i,2)], next_dims...)
    end
    corners = ntuple(2^N) do i
        return Array{T,N}(undef, halo_width)
    end
    return HaloArray(center, edges, corners, halo_width)
end

HaloArray(center::AT, edges::EAT, corners::CAT, halo_width::NTuple{N, Int}) where {T,N,AT<:AbstractArray{T,N},CAT<:Tuple,EAT<:Tuple} =
    HaloArray{T,N,length(edges),length(corners),AT,EAT,CAT}(center, edges, corners, halo_width)

Base.size(tile::HaloArray) = size(tile.center) .+ 2 .* tile.halo_width
function Base.axes(tile::HaloArray{T,N,H}) where {T,N,H}
    ntuple(N) do i
        first_ind = 1 - tile.halo_width[i]
        last_ind = size(tile.center, i) + tile.halo_width[i]
        return first_ind:last_ind
    end
end
function Base.similar(tile::HaloArray{T,N,H}, ::Type{T}, dims::NTuple{N,Int}) where {T,N,H}
    center_size = dims
    halo_width = tile.halo_width
    return HaloArray{T,N,H}(center_size, halo_width)
end
function Base.copy(tile::HaloArray{T,N,H}) where {T,N,H}
    center = copy(tile.center)
    halo = ntuple(i->copy(tile.edges[i]), H)
    halo_width = tile.halo_width
    return HaloArray{T,N,H}(center, halo, halo_width)
end

# Define getindex for HaloArray
@inline function Base.getindex(tile::HaloArray{T,N}, I::Vararg{Int,N}) where {T,N}
    Base.@boundscheck checkbounds(tile, I...)
    if all(1 .<= I .<= size(tile.center))
        return tile.center[I...]
    elseif !any(1 .<= I .<= size(tile.center))
        # Corner
        # N.B. Corner indexes are in binary, e.g. 0b01, 0b10, 0b11
        corner_idx = sum(ntuple(i->(I[i] < 1 ? 0 : 1) * (2^(i-1)), N)) + 1
        corner_offset = CartesianIndex(I) + CartesianIndex(ntuple(i->(I[i] < 1 ? tile.halo_width[i] : -size(tile.center, i)), N))
        return tile.corners[corner_idx][corner_offset]
    else
        for d in 1:N
            if I[d] < 1
                halo_idx = ntuple(i->i == d ? I[i] + tile.halo_width[i] : I[i], N)
                return tile.edges[(2*(d-1))+1][halo_idx...]
            elseif I[d] > size(tile.center, d)
                halo_idx = ntuple(i->i == d ? I[i] - size(tile.center, d) : I[i], N)
                return tile.edges[(2*(d-1))+2][halo_idx...]
            end
        end
    end
    error("Index out of bounds")
end

# Define setindex! for HaloArray
@inline function Base.setindex!(tile::HaloArray{T,N}, value, I::Vararg{Int,N}) where {T,N}
    Base.@boundscheck checkbounds(tile, I...)
    if all(1 .<= I .<= size(tile.center))
        # Center
        return tile.center[I...] = value
    elseif !any(1 .<= I .<= size(tile.center))
        # Corner
        # N.B. Corner indexes are in binary, e.g. 0b01, 0b10, 0b11
        corner_idx = sum(ntuple(i->(I[i] < 1 ? 0 : 1) * (2^(i-1)), N)) + 1
        corner_offset = CartesianIndex(I) + CartesianIndex(ntuple(i->(I[i] < 1 ? tile.halo_width[i] : -size(tile.center, i)), N))
        return tile.corners[corner_idx][corner_offset] = value
    else
        # Edge
        for d in 1:N
            if I[d] < 1
                halo_idx = ntuple(i->i == d ? I[i] + tile.halo_width[i] : I[i], N)
                return tile.edges[(2*(d-1))+1][halo_idx...] = value
            elseif I[d] > size(tile.center, d)
                halo_idx = ntuple(i->i == d ? I[i] - size(tile.center, d) : I[i], N)
                return tile.edges[(2*(d-1))+2][halo_idx...] = value
            end
        end
    end
    error("Index out of bounds")
end

Adapt.adapt_structure(to, H::Dagger.HaloArray) =
    HaloArray(Adapt.adapt(to, H.center),
              Adapt.adapt.(Ref(to), H.edges),
              Adapt.adapt.(Ref(to), H.corners),
              H.halo_width)

function aliasing(A::HaloArray)
    return CombinedAliasing([aliasing(A.center), map(aliasing, A.edges)..., map(aliasing, A.corners)...])
end
memory_space(A::HaloArray) = memory_space(A.center)
function move_rewrap(cache::AliasedObjectCache, from_proc::Processor, to_proc::Processor, from_space::MemorySpace, to_space::MemorySpace, A::HaloArray)
    center_chunk = rewrap_aliased_object!(cache, from_proc, to_proc, from_space, to_space, A.center)
    edge_chunks = ntuple(i->rewrap_aliased_object!(cache, from_proc, to_proc, from_space, to_space, A.edges[i]), length(A.edges))
    corner_chunks = ntuple(i->rewrap_aliased_object!(cache, from_proc, to_proc, from_space, to_space, A.corners[i]), length(A.corners))
    halo_width = A.halo_width
    to_w = root_worker_id(to_proc)
    return remotecall_fetch(to_w, from_proc, to_proc, from_space, to_space, center_chunk, edge_chunks, corner_chunks, halo_width) do from_proc, to_proc, from_space, to_space, center_chunk, edge_chunks, corner_chunks, halo_width
        center_new = move(from_proc, to_proc, center_chunk)
        edges_new = ntuple(i->move(from_proc, to_proc, edge_chunks[i]), length(edge_chunks))
        corners_new = ntuple(i->move(from_proc, to_proc, corner_chunks[i]), length(corner_chunks))
        return tochunk(HaloArray(center_new, edges_new, corners_new, halo_width), to_proc)
    end
end
function find_object_holding_ptr(object::HaloArray, ptr::UInt64)
    for i in 1:length(object.edges)
        edge = object.edges[i]
        span = LocalMemorySpan(pointer(edge), length(edge)*sizeof(eltype(edge)))
        if span_start(span) <= ptr <= span_end(span)
            return edge
        end
    end
    for i in 1:length(object.corners)
        corner = object.corners[i]
        span = LocalMemorySpan(pointer(corner), length(corner)*sizeof(eltype(corner)))
        if span_start(span) <= ptr <= span_end(span)
            return corner
        end
    end
    center = object.center
    span = LocalMemorySpan(pointer(center), length(center)*sizeof(eltype(center)))
    @assert span_start(span) <= ptr <= span_end(span) "Pointer $ptr not found in HaloArray"
    return center
end