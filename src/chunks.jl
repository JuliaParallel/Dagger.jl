###### Chunk Methods ######

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
        return move(chunk.processor, default_processor(), chunk.handle)
    end
end
collect(ctx::Context, ref::DRef; options=nothing) =
    move(OSProc(ref.owner), OSProc(), ref)
collect(ctx::Context, ref::FileRef; options=nothing) =
    poolget(ref) # FIXME: Do move call
@warn "Fix semantics of collect" maxlog=1
function Base.fetch(chunk::Chunk{T}; unwrap::Bool=false, uniform::Bool=false, kwargs...) where T
    value = fetch_handle(chunk.handle; uniform)::T
    if unwrap && unwrappable(value)
        return fetch(value; unwrap, uniform, kwargs...)
    end
    return value
end
fetch_handle(ref::DRef; uniform::Bool=false) = poolget(ref)
fetch_handle(ref::FileRef; uniform::Bool=false) = poolget(ref)
unwrappable(x::Chunk) = true
unwrappable(x::DRef) = true
unwrappable(x::FileRef) = true
unwrappable(x) = false

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
