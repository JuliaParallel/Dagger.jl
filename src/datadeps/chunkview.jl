struct ChunkView{N}
    chunk::Chunk
    slices::NTuple{N, Union{Int, AbstractRange{Int}, Colon}}
end

function Base.view(c::Chunk, slices...)
    if c.domain isa ArrayDomain
        nd, sz = ndims(c.domain), size(c.domain)
        nd == length(slices) || throw(DimensionMismatch("Expected $nd slices, got $(length(slices))"))

        for (i, s) in enumerate(slices)
            if s isa Int
                1 ≤ s ≤ sz[i] || throw(ArgumentError("Index $s out of bounds for dimension $i (size $(sz[i]))"))
            elseif s isa AbstractRange
                isempty(s) && continue
                1 ≤ first(s) ≤ last(s) ≤ sz[i] || throw(ArgumentError("Range $s out of bounds for dimension $i (size $(sz[i]))"))
            elseif s === Colon()
                continue
            else
                throw(ArgumentError("Invalid slice type $(typeof(s)) at dimension $i, Expected Type of Int, AbstractRange, or Colon"))
            end
        end
    end

    return ChunkView(c, slices)
end

Base.view(c::DTask, slices...) = view(fetch(c; move_value=false, unwrap=false), slices...)

aliasing(x::ChunkView) =
    throw(ConcurrencyViolationError("Cannot query aliasing of a ChunkView directly"))
memory_space(x::ChunkView) = memory_space(x.chunk)
isremotehandle(x::ChunkView) = true

# This definition is here because it's so similar to ChunkView
function move_rewrap(from_proc::Processor, to_proc::Processor, from_space::MemorySpace, to_space::MemorySpace, v::SubArray)
    p_chunk = aliased_object!(parent(v)) do p_chunk
        return remotecall_endpoint(identity, current_acceleration(), from_proc, to_proc, from_space, to_space, p_chunk)
    end
    inds = parentindices(v)
    return remotecall_endpoint(current_acceleration(), from_proc, to_proc, from_space, to_space, p_chunk) do p_new
        return view(p_new, inds...)
    end
end
function move_rewrap(from_proc::Processor, to_proc::Processor, from_space::MemorySpace, to_space::MemorySpace, slice::ChunkView)
    p_chunk = aliased_object!(slice.chunk) do p_chunk
        return remotecall_endpoint(identity, current_acceleration(), from_proc, to_proc, from_space, to_space, p_chunk)
    end
    inds = slice.slices
    return remotecall_endpoint(current_acceleration(), from_proc, to_proc, from_space, to_space, p_chunk) do p_new
        return view(p_new, inds...)
    end
end

Base.fetch(slice::ChunkView) = view(fetch(slice.chunk), slice.slices...)
