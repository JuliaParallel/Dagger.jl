using Serialization

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

Base.:(==)(c1::Chunk, c2::Chunk) = c1.handle == c2.handle
Base.hash(c::Chunk, x::UInt64) = hash(c.handle, x)

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

"Wraps `x` in a `Chunk` on `proc`, scoped to `scope`, which allows `x` to be mutated by tasks that use it."
macro mutable(proc, scope, x)
    :(Dagger.tochunk($(esc(x)), $(esc(proc)), $(esc(scope))))
end
"Creates a mutable `Chunk` on `proc`, scoped to exactly `proc`."
macro mutable(proc, x)
    quote
        let proc = $(esc(proc))
            let scope = proc isa OSProc ? Dagger.ProcessScope(proc.pid) : Dagger.ExactScope(proc)
                Dagger.@mutable proc scope $(esc(x))
            end
        end
    end
end
"Creates a mutable `Chunk` on the current worker."
macro mutable(x)
    :(Dagger.@mutable OSProc() Dagger.ProcessScope() $(esc(x)))
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
function shard(f; procs=nothing, workers=nothing, per_thread=false)
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
    scopes = [p isa OSProc ? ProcessScope(p) : ExactScope(p) for p in procs]
    thunks = [proc=>Dagger.@spawn single=Dagger.get_parent(proc).pid _shard_inner(f, proc, scope) for (proc,scope) in zip(procs,scopes)]
    shard = Shard(Dict{Processor,Chunk}(thunk[1]=>fetch(thunk[2])[] for thunk in thunks))
    scope = UnionScope(scopes)
    Dagger.tochunk(shard, OSProc(), scope)
end
function _shard_inner(f, proc, scope)
    Ref(Dagger.@mutable proc scope f())
end

"Creates a `Shard`. See [`Dagger.shard`](@ref) for details."
macro shard(exs...)
    opts = esc.(exs[1:end-1])
    ex = exs[end]
    quote
        let f = ()->$(esc(ex))
            $shard(f; $(opts...))
        end
    end
end

function move(from_proc::Processor, to_proc::Processor, shard::Shard)
    # Match either this proc or some ancestor
    # N.B. This behavior may bypass the piece's scope restriction
    proc = to_proc
    if haskey(shard.chunks, proc)
        return shard.chunks[proc]
    end
    parent = Dagger.get_parent(proc)
    while parent != proc
        proc = parent
        parent = Dagger.get_parent(proc)
        if haskey(shard.chunks, proc)
            return shard.chunks[proc]
        end
    end

    throw(KeyError(to_proc))
end
function move(from_proc::Processor, to_proc::Processor, x::Chunk{Shard})
    piece = remotecall_fetch(x.handle.owner, x.handle, from_proc, to_proc) do ref, from_proc, to_proc
        shard = MemPool.poolget(ref)
        move(from_proc, to_proc, shard)
    end::Chunk
    move(from_proc, to_proc, piece)
end
Base.map(f, cs::Chunk{Shard}) = map(f, fetch(cs; raw=true))
Base.map(f, s::Shard) = [Dagger.spawn(f, c) for c in values(s.chunks)]

### Core Stuff

"""
    tochunk(x, proc; persist=false, cache=false) -> Chunk

Create a chunk from sequential object `x` which resides on `proc`.
"""
function tochunk(x::X, proc::P=OSProc(), scope::S=AnyScope(); persist=false, cache=false) where {X,P,S}
    ref = poolset(x)
    Chunk{X,typeof(ref),P,S}(X, domain(x), ref, proc, scope, persist)
end
tochunk(x::Union{Chunk, Thunk}, proc=nothing, scope=nothing) = x

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


Base.@deprecate_binding AbstractPart Union{Chunk, Thunk}
Base.@deprecate_binding Part Chunk
Base.@deprecate parts(args...) chunks(args...)
Base.@deprecate part(args...) tochunk(args...)
Base.@deprecate parttype(args...) chunktype(args...)
