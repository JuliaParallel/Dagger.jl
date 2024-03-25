export domain, UnitDomain, project, alignfirst, ArrayDomain

import Base: isempty, getindex, intersect, ==, size, length, ndims

"""
    domain(x::T)

Returns metadata about `x`. This metadata will be in the `domain`
field of a Chunk object when an object of type `T` is created as
the result of evaluating a Thunk.
"""
function domain end

"""
    UnitDomain

Default domain -- has no information about the value
"""
struct UnitDomain end

"""
If no `domain` method is defined on an object, then
we use the `UnitDomain` on it. A `UnitDomain` is indivisible.
"""
domain(x::Any) = UnitDomain()

###### Chunk ######

"""
    Chunk

A reference to a piece of data located on a remote worker. `Chunk`s are
typically created with `Dagger.tochunk(data)`, and the data can then be
accessed from any worker with `collect(::Chunk)`. `Chunk`s are
serialization-safe, and use distributed refcounting (provided by
`MemPool.DRef`) to ensure that the data referenced by a `Chunk` won't be GC'd,
as long as a reference exists on some worker.

Each `Chunk` is associated with a given `Dagger.Processor`, which is (in a
sense) the processor that "owns" or contains the data. Calling
`collect(::Chunk)` will perform data movement and conversions defined by that
processor to safely serialize the data to the calling worker.

## Constructors
See [`tochunk`](@ref).
"""
mutable struct Chunk{T, H, P<:Processor, S<:AbstractScope}
    chunktype::Type{T}
    domain
    handle::H
    processor::P
    scope::S
end

domain(c::Chunk) = c.domain
chunktype(c::Chunk) = c.chunktype
processor(c::Chunk) = c.processor
affinity(c::Chunk) = affinity(c.handle)

is_task_or_chunk(c::Chunk) = true

Base.:(==)(c1::Chunk, c2::Chunk) = c1.handle == c2.handle
Base.hash(c::Chunk, x::UInt64) = hash(c.handle, hash(Chunk, x))

collect_remote(chunk::Chunk) =
    move(chunk.processor, OSProc(), poolget(chunk.handle))

function collect(ctx::Context, chunk::Chunk; options=nothing)
    # delegate fetching to handle by default.
    if chunk.handle isa DRef && !(chunk.processor isa OSProc)
        return remotecall_fetch(collect_remote, chunk.handle.owner, chunk)
    elseif chunk.handle isa FileRef
        return poolget(chunk.handle)
    else
        return move(chunk.processor, OSProc(), chunk.handle)
    end
end
collect(ctx::Context, ref::DRef; options=nothing) =
    move(OSProc(ref.owner), OSProc(), ref)
collect(ctx::Context, ref::FileRef; options=nothing) =
    poolget(ref) # FIXME: Do move call
function Base.fetch(chunk::Chunk; raw=false)
    if raw
        poolget(chunk.handle)
    else
        collect(chunk)
    end
end

# Unwrap Chunk, DRef, and FileRef by default
move(from_proc::Processor, to_proc::Processor, x::Chunk) =
    move(from_proc, to_proc, x.handle)
move(from_proc::Processor, to_proc::Processor, x::Union{DRef,FileRef}) =
    move(from_proc, to_proc, poolget(x))

# Determine from_proc when unspecified
move(to_proc::Processor, chunk::Chunk) =
    move(chunk.processor, to_proc, chunk)
move(to_proc::Processor, d::DRef) =
    move(OSProc(d.owner), to_proc, d)
move(to_proc::Processor, x) =
    move(OSProc(), to_proc, x)

### ChunkIO
affinity(r::DRef) = OSProc(r.owner)=>r.size
# this previously returned a vector with all machines that had the file cached
# but now only returns the owner and size, for consistency with affinity(::DRef),
# see #295
affinity(r::FileRef) = OSProc(1)=>r.size

struct WeakChunk
    wid::Int
    id::Int
    x::WeakRef
    function WeakChunk(c::Chunk)
        return new(c.handle.owner, c.handle.id, WeakRef(c))
    end
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
