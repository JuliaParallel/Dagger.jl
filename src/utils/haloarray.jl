# Define the HaloArray type with minimized halo storage
struct HaloArray{T,N,E,C} <: AbstractArray{T,N}
    center::Array{T,N}
    edges::NTuple{E, Array{T,N}}
    corners::NTuple{C, Array{T,N}}
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
    return HaloArray{T,N,2N,2^N}(center, edges, corners, halo_width)
end

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
function Base.getindex(tile::HaloArray{T,N}, I::Vararg{Int,N}) where {T,N}
    checkbounds(tile, I...)
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
                halo_idx = (I[1:d-1]..., I[d] + tile.halo_width[d], I[d+1:end]...)
                return tile.edges[(2*(d-1))+1][halo_idx...]
            elseif I[d] > size(tile.center, d)
                halo_idx = (I[1:d-1]..., I[d] - size(tile.center, d), I[d+1:end]...)
                return tile.edges[(2*(d-1))+2][halo_idx...]
            end
        end
    end
    error("Index out of bounds")
end

# Define setindex! for HaloArray
function Base.setindex!(tile::HaloArray{T,N}, value, I::Vararg{Int,N}) where {T,N}
    checkbounds(tile, I...)
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
                halo_idx = (I[1:d-1]..., I[d] + tile.halo_width[d], I[d+1:end]...)
                return tile.edges[(2*(d-1))+1][halo_idx...] = value
            elseif I[d] > size(tile.center, d)
                halo_idx = (I[1:d-1]..., I[d] - size(tile.center, d), I[d+1:end]...)
                return tile.edges[(2*(d-1))+2][halo_idx...] = value
            end
        end
    end
    error("Index out of bounds")
end

#=
# Example usage
center_size = (3, 5)
halo_width = (1, 1)
tile = HaloArray{Float64, 2}(center_size, halo_width)

# Set values in the center and halo
tile[2, 2] = 1.0
tile[0, 2] = 2.0  # This should be in an edge
tile[0, 0] = 3.0  # This should be in a corner
tile[4, 6] = 4.0  # This should be in a corner

# Get values from the center and halo
println(tile[2, 2])  # 1.0
println(tile[0, 2])  # 2.0
println(tile[0, 0])  # 3.0
println(tile[4, 6])  # 4.0
=#
