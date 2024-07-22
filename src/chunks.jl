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
    persist::Bool
end

domain(c::Chunk) = c.domain
chunktype(c::Chunk) = c.chunktype
persist!(t::Chunk) = (t.persist=true; t)
shouldpersist(p::Chunk) = t.persist
processor(c::Chunk) = c.processor
affinity(c::Chunk) = affinity(c.handle)

is_task_or_chunk(c::Chunk) = true

Base.:(==)(c1::Chunk, c2::Chunk) = c1.handle == c2.handle
Base.hash(c::Chunk, x::UInt64) = hash(c.handle, hash(Chunk, x))

Adapt.adapt_storage(::FetchAdaptor, x::Chunk) = fetch(x)

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

### Mutation

function _mutable_inner(@nospecialize(f), proc, scope)
    result = f()
    return Ref(Dagger.tochunk(result, proc, scope))
end

"""
    mutable(f::Base.Callable; worker, processor, scope) -> Chunk

Calls `f()` on the specified worker or processor, returning a `Chunk`
referencing the result with the specified scope `scope`.
"""
function mutable(@nospecialize(f); worker=nothing, processor=nothing, scope=nothing)
    if processor === nothing
        if worker === nothing
            processor = OSProc()
        else
            processor = OSProc(worker)
        end
    else
        @assert worker === nothing "mutable: Can't mix worker and processor"
    end
    if scope === nothing
        scope = processor isa OSProc ? ProcessScope(processor) : ExactScope(processor)
    end
    return fetch(Dagger.@spawn scope=scope _mutable_inner(f, processor, scope))[]
end

"""
    @mutable [worker=1] [processor=OSProc()] [scope=ProcessorScope()] f()

Helper macro for [`mutable()`](@ref).
"""
macro mutable(exs...)
    opts = esc.(exs[1:end-1])
    ex = exs[end]
    quote
        let f = @noinline ()->$(esc(ex))
            $mutable(f; $(opts...))
        end
    end
end

"""
Maps a value to one of multiple distributed "mirror" values automatically when
used as a thunk argument. Construct using `@shard` or `shard`.
"""
struct Shard
    chunks::Dict{Processor,Chunk}
end

"""
    shard(f; kwargs...) -> Chunk{Shard}

Executes `f` on all workers in `workers`, wrapping the result in a
process-scoped `Chunk`, and constructs a `Chunk{Shard}` containing all of these
`Chunk`s on the current worker.

Keyword arguments:
- `procs` -- The list of processors to create pieces on. May be any iterable container of `Processor`s.
- `workers` -- The list of workers to create pieces on. May be any iterable container of `Integer`s.
- `per_thread::Bool=false` -- If `true`, creates a piece per each thread, rather than a piece per each worker.
"""
function shard(@nospecialize(f); procs=nothing, workers=nothing, per_thread=false)
    if procs === nothing
        if workers !== nothing
            procs = [OSProc(w) for w in workers]
        else
            procs = lock(Sch.eager_context()) do
                copy(Sch.eager_context().procs)
            end
        end
        if per_thread
            _procs = ThreadProc[]
            for p in procs
                append!(_procs, filter(p->p isa ThreadProc, get_processors(p)))
            end
            procs = _procs
        end
    else
        if workers !== nothing
            throw(ArgumentError("Cannot combine `procs` and `workers`"))
        elseif per_thread
            throw(ArgumentError("Cannot combine `procs` and `per_thread=true`"))
        end
    end
    isempty(procs) && throw(ArgumentError("Cannot create empty Shard"))
    shard_running_dict = Dict{Processor,DTask}()
    for proc in procs
        scope = proc isa OSProc ? ProcessScope(proc) : ExactScope(proc)
        thunk = Dagger.@spawn scope=scope _mutable_inner(f, proc, scope)
        shard_running_dict[proc] = thunk
    end
    shard_dict = Dict{Processor,Chunk}()
    for proc in procs
        shard_dict[proc] = fetch(shard_running_dict[proc])[]
    end
    return Shard(shard_dict)
end

"Creates a `Shard`. See [`Dagger.shard`](@ref) for details."
macro shard(exs...)
    opts = esc.(exs[1:end-1])
    ex = exs[end]
    quote
        let f = @noinline ()->$(esc(ex))
            $shard(f; $(opts...))
        end
    end
end

function move(from_proc::Processor, to_proc::Processor, shard::Shard)
    # Match either this proc or some ancestor
    # N.B. This behavior may bypass the piece's scope restriction
    proc = to_proc
    if haskey(shard.chunks, proc)
        return move(from_proc, to_proc, shard.chunks[proc])
    end
    parent = Dagger.get_parent(proc)
    while parent != proc
        proc = parent
        parent = Dagger.get_parent(proc)
        if haskey(shard.chunks, proc)
            return move(from_proc, to_proc, shard.chunks[proc])
        end
    end

    throw(KeyError(to_proc))
end
Base.iterate(s::Shard) = iterate(values(s.chunks))
Base.iterate(s::Shard, state) = iterate(values(s.chunks), state)
Base.length(s::Shard) = length(s.chunks)

### Core Stuff

"""
    tochunk(x, proc::Processor, scope::AbstractScope; device=nothing, kwargs...) -> Chunk

Create a chunk from data `x` which resides on `proc` and which has scope
`scope`.

`device` specifies a `MemPool.StorageDevice` (which is itself wrapped in a
`Chunk`) which will be used to manage the reference contained in the `Chunk`
generated by this function. If `device` is `nothing` (the default), the data
will be inspected to determine if it's safe to serialize; if so, the default
MemPool storage device will be used; if not, then a `MemPool.CPURAMDevice` will
be used.

All other kwargs are passed directly to `MemPool.poolset`.
"""
function tochunk(x::X, proc::P=OSProc(), scope::S=AnyScope(); persist=false, cache=false, device=nothing, kwargs...) where {X,P,S}
    if device === nothing
        device = if Sch.walk_storage_safe(x)
            MemPool.GLOBAL_DEVICE[]
        else
            MemPool.CPURAMDevice()
        end
    end
    ref = poolset(x; device, kwargs...)
    Chunk{X,typeof(ref),P,S}(X, domain(x), ref, proc, scope, persist)
end
tochunk(x::Union{Chunk, Thunk}, proc=nothing, scope=nothing; kwargs...) = x

function savechunk(data, dir, f)
    sz = open(joinpath(dir, f), "w") do io
        serialize(io, MemPool.MMWrap(data))
        return position(io)
    end
    fr = FileRef(f, sz)
    proc = OSProc()
    scope = AnyScope() # FIXME: Scoped to this node
    Chunk{typeof(data),typeof(fr),typeof(proc),typeof(scope)}(typeof(data), domain(data), fr, proc, scope, true)
end

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
is_task_or_chunk(c::WeakChunk) = true
Serialization.serialize(io::AbstractSerializer, wc::WeakChunk) =
    error("Cannot serialize a WeakChunk")

Base.@deprecate_binding AbstractPart Union{Chunk, Thunk}
Base.@deprecate_binding Part Chunk
Base.@deprecate parts(args...) chunks(args...)
Base.@deprecate part(args...) tochunk(args...)
Base.@deprecate parttype(args...) chunktype(args...)
