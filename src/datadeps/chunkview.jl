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

Base.view(c::DTask, slices...) = view(fetch(c; raw=true), slices...)

function aliasing(x::ChunkView{N}) where N
    return remotecall_fetch(root_worker_id(x.chunk.processor), x.chunk, x.slices) do x, slices
        x = unwrap(x)
        v = view(x, slices...)
        return aliasing(v)
    end
end
memory_space(x::ChunkView) = memory_space(x.chunk)
isremotehandle(x::ChunkView) = true

# This definition is here because it's so similar to ChunkView
function move_rewrap(from_proc::Processor, to_proc::Processor, from_space::MemorySpace, to_space::MemorySpace, v::SubArray)
    to_w = root_worker_id(to_proc)
    p_chunk = aliased_object!(parent(v)) do p
        return remotecall_fetch(to_w, from_proc, to_proc, from_space, to_space, p) do from_proc, to_proc, from_space, to_space, p
            return tochunk(move(from_proc, to_proc, p), to_proc)
        end
    end
    inds = parentindices(v)
    return remotecall_fetch(to_w, from_proc, to_proc, from_space, to_space, p_chunk, inds) do from_proc, to_proc, from_space, to_space, p_chunk, inds
        p_new = move(from_proc, to_proc, p_chunk)
        v_new = view(p_new, inds...)
        return tochunk(v_new, to_proc)
    end
end
function move_rewrap(from_proc::Processor, to_proc::Processor, from_space::MemorySpace, to_space::MemorySpace, slice::ChunkView)
    to_w = root_worker_id(to_proc)
    p_chunk = aliased_object!(slice.chunk) do p_chunk
        return remotecall_fetch(to_w, from_proc, to_proc, from_space, to_space, p_chunk) do from_proc, to_proc, from_space, to_space, p_chunk
            return tochunk(move(from_proc, to_proc, p_chunk), to_proc)
        end
    end
    return remotecall_fetch(to_w, from_proc, to_proc, from_space, to_space, p_chunk, slice.slices) do from_proc, to_proc, from_space, to_space, p_chunk, inds
        p_new = move(from_proc, to_proc, p_chunk)
        v_new = view(p_new, inds...)
        return tochunk(v_new, to_proc)
    end
end
function move(from_proc::Processor, to_proc::Processor, slice::ChunkView)
    to_w = root_worker_id(to_proc)
    return remotecall_fetch(to_w, from_proc, to_proc, slice.chunk, slice.slices) do from_proc, to_proc, chunk, slices
        chunk_new = move(from_proc, to_proc, chunk)
        v_new = view(chunk_new, slices...)
        return tochunk(v_new, to_proc)
    end
end

Base.fetch(slice::ChunkView) = view(fetch(slice.chunk), slice.slices...)