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
    remotecall_fetch(root_worker_id(x.chunk.processor), x.chunk, x.slices) do x, slices
        x = unwrap(x)
        v = view(x, slices...)
        return aliasing(v)
    end
end
memory_space(x::ChunkView) = memory_space(x.chunk)
isremotehandle(x::ChunkView) = true

#=
function move!(dep_mod, to_space::MemorySpace, from_space::MemorySpace, to::ChunkView, from::ChunkView)
    to_w = root_worker_id(to_space)
    @assert to_w == myid()
    to_raw = unwrap(to.chunk)
    from_w = root_worker_id(from_space)
    from_raw = to_w == from_w ? unwrap(from.chunk) : remotecall_fetch(f->copy(unwrap(f)), from_w, from.chunk)
    from_view = view(from_raw, from.slices...)
    to_view = view(to_raw, to.slices...)
    move!(dep_mod, to_space, from_space, to_view, from_view)
    return
end
=#

# This definition is here because it's so similar to ChunkView
function move_rewrap(from_proc::Processor, to_proc::Processor, v::SubArray)
    if from_proc == to_proc
        declare_aliased_object!(parent(v))
        return tochunk(v, to_proc)
    else
        to_w = root_worker_id(to_proc)
        p_chunk = aliased_object!(parent(v)) do p
            return remotecall_fetch(to_w, from_proc, to_proc, p) do from_proc, to_proc, p
                return tochunk(move(from_proc, to_proc, p), to_proc)
            end
        end
        inds = parentindices(v)
        return remotecall_fetch(to_w, from_proc, to_proc, p_chunk, inds) do from_proc, to_proc, p_chunk, inds
            p_new = move(from_proc, to_proc, p_chunk)
            v_new = view(p_new, inds...)
            return tochunk(v_new, to_proc)
        end
    end
end
function move_rewrap(from_proc::Processor, to_proc::Processor, slice::ChunkView)
    if from_proc == to_proc
        declare_aliased_object!(slice.chunk)
        v = view(unwrap(slice.chunk), slice.slices...)
        return tochunk(v, to_proc)
    else
        from_w = root_worker_id(from_proc)
        p_chunk = aliased_object!(slice.chunk) do p_chunk
            p = unwrap(p_chunk)
            return remotecall_fetch(from_w, from_proc, to_proc, p) do from_proc, to_proc, p
                return tochunk(move(from_proc, to_proc, p), to_proc)
            end
        end
        return remotecall_fetch(from_w, from_proc, to_proc, p_chunk, slice.slices) do from_proc, to_proc, p_chunk, inds
            p_new = move(from_proc, to_proc, p_chunk)
            v_new = view(p_new, inds...)
            return tochunk(v_new, to_proc)
        end
    end
end

Base.fetch(slice::ChunkView) = view(fetch(slice.chunk), slice.slices...)