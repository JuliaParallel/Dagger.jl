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

###### Chunk Methods ######

domain(c::Chunk) = c.domain
chunktype(c::Chunk) = c.chunktype
processor(c::Chunk) = c.processor

"""
    datasize(x)

Returns the estimated memory size of `x`'s data, used for transfer-cost estimation.
"""
datasize(c::Chunk) = datasize(c.handle)
datasize(r::DRef) = r.size
datasize(r::FileRef) = r.size

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
function Base.fetch(chunk::Chunk{T}; unwrap::Bool=false, uniform::Bool=uniform_execution(),
                   local_only::Bool=false, kwargs...) where T
    local_only && return fetch_local(chunk)
    # N.B. Do not assert `::T`: the chunktype is not always the restored value
    # type. File-backed chunks (`tochunk(FileRef(path); device=...)`) carry
    # chunktype `FileRef` but restore to the file's deserialized contents.
    value = fetch_handle(chunk.handle; uniform)
    if unwrap && unwrappable(value)
        return fetch(value; unwrap, uniform, kwargs...)
    end
    return value
end
fetch_handle(ref::DRef; uniform::Bool) = poolget(ref)
fetch_handle(ref::FileRef; uniform::Bool) = poolget(ref)

"""
    fetch_local(x)

The payload of `x` as it sits in the calling process's memory, or `nothing` when another
process owns it. Unlike [`fetch`](@ref) this never communicates, which is what makes it
callable on only some of the processes: under a uniform-execution acceleration (MPI) a
`fetch` is collective and every rank has to join in, whereas `fetch_local` lets each rank
read what it happens to hold and ignore the rest. Use it for data that is meant to stay
where it was computed.
"""
fetch_local(x) = x
fetch_local(chunk::Chunk) = fetch_local(chunk.handle)
fetch_local(ref::DRef) = root_worker_id(ref) == myid() ? poolget(ref) : nothing
fetch_local(ref::FileRef) = poolget(ref)
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
