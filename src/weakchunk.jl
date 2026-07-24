struct WeakChunk
    wid::Int
    id::Int
    x::WeakRef
    # Cache the referent's chunktype: `chunktype(::WeakChunk)` is used by the
    # scheduler's post-completion cost bookkeeping (`signature`), which can run
    # after datadeps has already freed the underlying chunk (so the `WeakRef`
    # would be expired). The type is fixed for the chunk's lifetime, so caching
    # it keeps that path working without pinning the chunk alive.
    chunktype::Type
end

function WeakChunk(c::Chunk)
    return WeakChunk(c.handle.owner, c.handle.id, WeakRef(c), chunktype(c))
end

unwrap_weak(c::WeakChunk) = c.x.value
function unwrap_weak_checked(c::WeakChunk)
    cw = unwrap_weak(c)
    # A `Nothing` chunktype marks a non-resident placeholder (e.g. an MPIRef
    # chunk for data owned by another rank): its wrapper legitimately may have
    # been GC'd, so only assert liveness for chunks that carry real data.
    @assert chunktype(c) === Nothing || cw !== nothing "WeakChunk expired: ($(c.wid), $(c.id))"
    return cw
end
wrap_weak(c::Chunk) = WeakChunk(c)
isweak(c::WeakChunk) = true
isweak(c::Chunk) = false
is_task_or_chunk(c::WeakChunk) = true
Serialization.serialize(io::AbstractSerializer, wc::WeakChunk) =
    error("Cannot serialize a WeakChunk")
chunktype(c::WeakChunk) = c.chunktype
