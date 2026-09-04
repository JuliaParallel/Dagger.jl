struct ChunkView{N}
    chunk::Chunk
    slices::NTuple{N, Union{Int, AbstractRange{Int}, Colon}}
end

function _identity_hash(arg::ChunkView, h::UInt=UInt(0))
    return hash(arg.slices, _identity_hash(arg.chunk, h))
end

function Base.view(c::Chunk, slices...)
    # N.B. Placeholder chunk records (e.g. MPI non-owner views of remote data)
    # carry an empty domain; skip slice validation for them
    if c.domain isa ArrayDomain && !(ndims(c.domain) == 0 && length(slices) > 0)
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

# Compose nested ChunkView slices onto the underlying Chunk (flatten; do not
# nest ChunkView structs). Mirrors Base.reindex for Int / AbstractRange / Colon.
_compose_chunkview_slices(::Tuple{}, ::Tuple{}) = ()
_compose_chunkview_slices(::Tuple{}, ::Tuple{Any, Vararg}) =
    throw(DimensionMismatch("Too many indices for ChunkView"))
_compose_chunkview_slices(::Tuple{Colon, Vararg}, ::Tuple{}) =
    throw(DimensionMismatch("Too few indices for ChunkView"))
_compose_chunkview_slices(::Tuple{AbstractRange, Vararg}, ::Tuple{}) =
    throw(DimensionMismatch("Too few indices for ChunkView"))
# Parent Int: dimension already dropped; keep and do not consume a sub-slice
_compose_chunkview_slices(ps::Tuple{Int, Vararg}, ss::Tuple) =
    (ps[1], _compose_chunkview_slices(Base.tail(ps), ss)...)
# Parent Colon: pass the sub-slice through
_compose_chunkview_slices(ps::Tuple{Colon, Vararg}, ss::Tuple{Any, Vararg}) =
    (ss[1], _compose_chunkview_slices(Base.tail(ps), Base.tail(ss))...)
# Parent AbstractRange: re-index into it
function _compose_chunkview_slices(ps::Tuple{AbstractRange, Vararg}, ss::Tuple{Any, Vararg})
    composed = try
        ps[1][ss[1]]
    catch e
        e isa BoundsError || rethrow()
        throw(ArgumentError("Slice $(ss[1]) out of bounds for parent slice $(ps[1])"))
    end
    return (composed, _compose_chunkview_slices(Base.tail(ps), Base.tail(ss))...)
end

function Base.view(cv::ChunkView, slices...)
    composed = _compose_chunkview_slices(cv.slices, slices)
    return view(cv.chunk, composed...)
end

Base.view(c::DTask, slices...) = view(fetch(c; raw=true), slices...)

function aliasing(accel::Acceleration, x::ChunkView{N}, dep_mod) where N
    @assert dep_mod === identity "Dependency modifiers not yet supported for ChunkView: $dep_mod"
    return remotecall_fetch(root_worker_id(x.chunk.processor), x.chunk, x.slices) do x, slices
        x = unwrap(x)
        v = view(x, slices...)
        # A view of a whole-object container (e.g. `DSparseArray`) must alias the
        # entire container; `aliasing_unwrapped` resolves that (otherwise it just
        # aliases the view), and crucially does so here where `v` lives.
        return aliasing_unwrapped(v)
    end
end
aliasing(x::ChunkView) = aliasing(current_acceleration(), x, identity)
aliasing(x::ChunkView, dep_mod) = aliasing(current_acceleration(), x, dep_mod)
memory_space(x::ChunkView) = memory_space(x.chunk)
isremotehandle(x::ChunkView) = true

# Under uniform (SPMD) execution, a ChunkView is replicated metadata on every
# rank (inner chunk record + slices); wrapping it in a rank-0-owned Chunk
# would strand the other ranks' inner chunk records, so keep it raw
datadeps_arg_wrap(arg::ChunkView) = uniform_execution() ? arg : tochunk(arg)

# ChunkView is a remote handle: its move_rewrap accesses the underlying chunk
# remotely itself, so dispatch directly from the caller
function remotecall_endpoint_toplevel(f, accel::DistributedAcceleration, cache::AliasedObjectCache, from_proc, to_proc, from_space, to_space, data::ChunkView)
    return f(accel, cache, from_proc, to_proc, from_space, to_space, data)::Chunk
end

# Header+children registration: share the backing chunk, rebuild as a view.
# N.B. We use move_rewrap (not rewrap_aliased_object!) so that if the inner
# chunk is a SubArray, it goes through the SubArray-aware path which shares
# the parent array via the aliased object cache.
move_rewrap_parts(slice::ChunkView) = ((slice.chunk,), slice.slices)
move_rewrap_build(::Type{<:ChunkView}, (p,), inds) = view(p, inds...)
move_rewrap_header_mode(::Type{<:ChunkView}) = :replicated
move_rewrap_result_type(::Type{<:ChunkView}, (PT,), inds) =
    Base.promote_op(view, PT, typeof.(inds)...)

function move(from_proc::Processor, to_proc::Processor, slice::ChunkView)
    to_w = root_worker_id(to_proc)
    return remotecall_fetch(to_w, from_proc, to_proc, slice.chunk, slice.slices) do from_proc, to_proc, chunk, slices
        chunk_new = move(from_proc, to_proc, chunk)
        v_new = view(chunk_new, slices...)
        return tochunk(v_new, to_proc)
    end
end

Base.fetch(slice::ChunkView) = view(fetch(slice.chunk), slice.slices...)
