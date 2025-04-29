struct WeakChunk
    wid::Int
    id::Int
    x::WeakRef
end

function WeakChunk(c::Chunk)
    return WeakChunk(c.handle.owner, c.handle.id, WeakRef(c))
end

unwrap_weak(c::WeakChunk) = c.x.value
function unwrap_weak_checked(c::WeakChunk)
    cw = unwrap_weak(c)
    @assert cw !== nothing "WeakChunk expired: ($(c.wid), $(c.id))"
    return cw
end
wrap_weak(c::Chunk) = WeakChunk(c)
isweak(c::WeakChunk) = true
isweak(c::Chunk) = false
is_task_or_chunk(c::WeakChunk) = true
Serialization.serialize(io::AbstractSerializer, wc::WeakChunk) =
    error("Cannot serialize a WeakChunk")
chunktype(c::WeakChunk) = chunktype(unwrap_weak_checked(c))