import Graphs: SimpleDiGraph, add_edge!, add_vertex!, inneighbors, outneighbors, nv,
               weakly_connected_components, topological_sort_by_dfs, vertices

export In, Out, InOut, Deps, spawn_datadeps

#=
==============================================================================
                    DATADEPS ALIASING AND DATA MOVEMENT SYSTEM
==============================================================================

This file implements the data dependencies system for Dagger tasks, which allows
tasks to access their arguments in a controlled manner. The system maintains
data coherency across distributed workers by tracking aliasing relationships
and orchestrating data movement operations.

OVERVIEW:
---------
The datadeps system enables parallel execution of tasks that modify shared data
by analyzing memory aliasing relationships and scheduling appropriate data 
transfers. The core challenge is maintaining coherency when aliased data (e.g., 
an array and its views) needs to be accessed by tasks running on different workers.

KEY CONCEPTS:
-------------

1. ALIASING ANALYSIS:
   - Every mutable argument is analyzed for its memory access pattern
   - Memory spans are computed to determine which bytes in memory are accessed
   - Arguments that access overlapping memory spans are considered "aliasing"
   - Examples: An array A and view(A, 2:3, 2:3) alias each other

2. DATA LOCALITY TRACKING:
   - The system tracks where the "source of truth" for each piece of data lives
   - As tasks execute and modify data, the source of truth may move between workers
   - Each argument can have its own independent source of truth location

3. ALIASED OBJECT MANAGEMENT:
   - When copying arguments between workers, the system tracks "aliased objects"
   - This ensures that if both an array and its view need to be copied to a worker,
     only one copy of the underlying array is made, with the view pointing to it
   - The aliased_object!() and move_rewrap() functions manage this sharing

ALIASING INFO:
--------------

The system uses different types of aliasing info to represent different types of
aliasing relationships:

- ContiguousAliasing: Single contiguous memory region (e.g., full array)
- StridedAliasing: Multiple non-contiguous regions (e.g., SubArray)
- DiagonalAliasing: Diagonal elements only (e.g., Diagonal(A))
- TriangularAliasing: Triangular regions (e.g., UpperTriangular(A))

Any two aliasing objects can be compared using the will_alias function to
determine if they overlap. Additionally, any aliasing object can be converted to
a vector of memory spans, which represents the contiguous regions of memory that
the aliasing object covers.

DATA MOVEMENT FUNCTIONS:
------------------------

move!(dep_mod, to_space, from_space, to, from):
- The core in-place data movement function
- dep_mod specifies which part of the data to copy (identity, UpperTriangular, etc.)
- Supports partial copies via RemainderAliasing dependency modifiers

move_rewrap(...):
- Handles copying of wrapped objects (SubArrays, ChunkViews)
- Ensures aliased objects are reused on destination worker

read/write_remainder!(...):
- Read/write a span of memory from an object to/from a buffer
- Used by move! to copy the remainder of an aliased object

THE DISTRIBUTED ALIASING PROBLEM:
---------------------------------

In a multithreaded environment, aliasing "just works" because all tasks operate
on the user-provided memory. However, in a distributed environment, arguments
must be copied between workers, which breaks aliasing relationships if care is
not taken.

Consider this scenario:
```julia
A = rand(4, 4)
vA = view(A, 2:3, 2:3)

Dagger.spawn_datadeps() do
    Dagger.@spawn inc!(InOut(A), 1)    # Task 1: increment all of A
    Dagger.@spawn inc!(InOut(vA), 2)   # Task 2: increment view of A
end
```

MULTITHREADED BEHAVIOR (WORKS):
- Both tasks run on the same worker
- They operate on the same memory, with proper dependency tracking
- Task dependencies ensure correct ordering (e.g., Task 1 then Task 2)

DISTRIBUTED BEHAVIOR (THE PROBLEM):
- Each argument must be copied to the destination worker
- Without special handling, we would copy A and vA independently to another worker
- This creates two separate arrays, breaking the aliasing relationship between A and vA

THE SOLUTION - PARTIAL DATA MOVEMENT:
-------------------------------------

The datadeps system solves this by:

1. UNIFIED ALLOCATION:
   - When copying aliased objects, ensure only one underlying array exists per worker
   - Use aliased_object!() to detect and reuse existing allocations
   - Views on the destination worker point to the shared underlying array

2. PARTIAL DATA TRANSFER:
   - Instead of copying entire objects, only transfer the "dirty" regions
   - This prevents overwrites of data that has already been updated by another task
   - This also minimizes network traffic and overall copy time
   - Uses the move!(dep_mod, ...) function with RemainderAliasing dependency modifiers

3. REMAINDER TRACKING:
   - When a task needs the full object, copy partial regions as needed
   - When a partial region is updated, track what parts still need updating
   - This preserves all updates while avoiding overwrites

EXAMPLE EXECUTION FLOW:
-----------------------

Given: A = 4x4 array, vA = view(A, 2:3, 2:3)
Tasks: T1 modifies InOut(A), T2 modifies InOut(vA)

1. INITIAL STATE:
   - A and vA both exist on worker0 (main worker)
   - A's data_locality = worker0, vA's data_locality = worker0

2. T1 SCHEDULED ON WORKER1:
   - Copy A from worker0 to worker1
   - T1 executes, modifying all of A on worker1
   - Update: A's data_locality = worker1, A is now "dirty" on worker1

3. T2 SCHEDULED ON WORKER2:
   - T2 needs vA, but vA aliases with A (which was modified by T1)
   - Copy vA-region of A from worker1 to worker2
   - This is a PARTIAL copy - only the 2:3, 2:3 region
   - Create vA on worker2 pointing to the appropriate region of A
   - T2 executes, modifying vA region on worker2
   - Update: vA's data_locality = worker2

4. FINAL SYNCHRONIZATION:
   - Need to copy-back A and vA to worker0
   - A needs to be assembled from: worker1 (non-vA regions of A) + worker2 (vA region of A)
   - REMAINDER COPY: Copy non-vA regions from worker1 to worker0
   - REMAINDER COPY: Copy vA region from worker2 to worker0

REMAINDER COMPUTATION:
----------------------

Remainder computation involves:
1. Computing memory spans for all overlapping aliasing objects
2. Finding the set difference: full_object_spans - updated_spans
3. Creating a RemainderAliasing object representing the difference between spans
4. Performing one or more move! calls with this RemainderAliasing object to copy only needed data
=#

"Specifies a read-only dependency."
struct In{T}
    x::T
end
"Specifies a write-only dependency."
struct Out{T}
    x::T
end
"Specifies a read-write dependency."
struct InOut{T}
    x::T
end
"Specifies one or more dependencies."
struct Deps{T,DT<:Tuple}
    x::T
    deps::DT
end
Deps(x, deps...) = Deps(x, deps)

chunktype(::In{T}) where T = T
chunktype(::Out{T}) where T = T
chunktype(::InOut{T}) where T = T
chunktype(::Deps{T,DT}) where {T,DT} = T

"""
    unwrap_inout(arg) -> (arg, deps)

Strips any `In`/`Out`/`InOut`/`Deps` wrapper from `arg`, returning the bare
value together with its dependencies as a *tuple* of `(dep_mod, readdep,
writedep)` triples. A bare (unwrapped) value is a read dependency with
`identity` as its `dep_mod`.

N.B. The dependencies are a tuple, not a `Vector{Tuple}`: `Deps` stores its
sub-dependencies as a `Tuple` already, and every consumer only iterates the
result, so keeping it a tuple makes the whole thing inferrable and keeps the
common single-dependency case off the heap.
"""
function unwrap_inout(arg)
    if arg isa In
        return arg.x, ((identity, true, false),)
    elseif arg isa Out
        return arg.x, ((identity, false, true),)
    elseif arg isa InOut
        return arg.x, ((identity, true, true),)
    elseif arg isa Deps
        return arg.x, _unwrap_deps(arg.deps)
    else
        return arg, ((identity, true, false),)
    end
end
# Flatten `Deps`' sub-dependencies: each sub-dependency contributes its own
# unwrapped value as the `dep_mod` for every triple it produces.
_unwrap_deps(::Tuple{}) = ()
@inline function _unwrap_deps(deps::Tuple)
    dep_mod, inner_deps = unwrap_inout(first(deps))
    head = map(d->(dep_mod, d[2], d[3]), inner_deps)
    return (head..., _unwrap_deps(Base.tail(deps))...)
end

_identity_hash(arg, h::UInt=UInt(0)) = ismutable(arg) ? objectid(arg) : hash(arg, h)
_identity_hash(arg::Chunk, h::UInt=UInt(0)) = hash(arg.handle, hash(Chunk, h))
_identity_hash(arg::SubArray, h::UInt=UInt(0)) = hash(arg.indices, hash(arg.offset1, hash(arg.stride1, _identity_hash(arg.parent, h))))
_identity_hash(arg::CartesianIndices, h::UInt=UInt(0)) = hash(arg.indices, hash(typeof(arg), h))

"""
    ChunkAinfoMemo

Per-region memo for the aliasing info of a `Chunk` / `ChunkView`.

A chunk's aliasing info cannot be computed locally: under Distributed it takes a
`remotecall_fetch` to the owner, and under MPI a broadcast from the owner that
every rank has to join. Datadeps asks the same question repeatedly while planning
one region -- once per unique argument to build the dependency DAG, again for
every slot (`aliased_object!`, `aliasing!`), and again for the write-back
epilogue -- so left unmemoized a single region spends hundreds of round-trips
re-deriving a handful of distinct answers. Under MPI each of those is a
*global synchronization point*, which is what makes replicated planning scale
poorly with rank count.

Memoization is per-region because aliasing info describes where a value's memory
currently is: stable while one region plans (datadeps copies into existing
buffers, it never relocates a live argument), but a chunk allocated by a later
region may well reuse a freed address.

Keys (`ainfo_memo_key`) combine the argument's identity, the dependency modifier
and the acceleration, and are identical on every rank (chunk handles hash by rank
+ id). Uniform keys are what makes the memo safe under SPMD replay: every rank
hits and misses on exactly the same calls, so the broadcasts that remain are
still reached collectively by all ranks.
"""
struct ChunkAinfoMemo
    entries::Dict{UInt,AbstractAliasing}
    lock::ReentrantLock
end
ChunkAinfoMemo() = ChunkAinfoMemo(Dict{UInt,AbstractAliasing}(), ReentrantLock())

const CHUNK_AINFO_MEMO = ScopedValue{Union{ChunkAinfoMemo,Nothing}}(nothing)

"The memo key for `arg`'s aliasing info under `dep_mod`; see `ChunkAinfoMemo`."
ainfo_memo_key(arg, dep_mod) = ainfo_memo_key(_identity_hash(arg), dep_mod)

"""
    ainfo_memo_key(idhash::UInt, dep_mod) -> UInt

The memo key for an argument whose identity hash is already known (an indirect
handle, e.g. an MPI wire value, keys on the chunk it stands in for).

The current acceleration is part of the key because the memo answers "what does
*this* acceleration say the aliasing is". Under MPI the acceleration-level answer
is the owner's local answer rank-stamped and broadcast, and it is derived by
re-asking the *default* acceleration for the raw local one -- a nested query with
the same argument. Keying both alike lets the outer, uniform answer be displaced
by the inner, rank-local one: the owner then returns an unstamped span while
every other rank holds the stamped one, and the region fails its uniformity check
(or, with checks off, mistakes two ranks' buffers for the same memory).
"""
ainfo_memo_key(idhash::UInt, dep_mod) =
    hash(accel_kind(current_acceleration()), hash(dep_mod, idhash))

"Run `f` (a chunk-aliasing computation) at most once per region per `(arg, dep_mod)`."
memoized_chunk_aliasing(f, arg, dep_mod) =
    memoized_ainfo(f, ainfo_memo_key(arg, dep_mod))

"""
    memoized_ainfo(f, key::UInt)

`memoized_chunk_aliasing` for callers that hold a key rather than the argument
itself, which is how an indirect handle (e.g. an MPI wire value standing in for a
chunk) reuses the entry already computed for the chunk it came from.
"""
function memoized_ainfo(f, key::UInt)
    memo = CHUNK_AINFO_MEMO[]
    memo === nothing && return f()
    @lock memo.lock begin
        cached = get(memo.entries, key, nothing)
        cached === nothing || return cached
    end
    # Computed outside the lock: it blocks on the owner, and under parallel
    # partition planning holding the lock across that would serialize planners.
    # Two planners racing on one key both compute; the loser's result is dropped.
    ainfo = f()
    @lock memo.lock begin
        return get!(memo.entries, key, ainfo)
    end
end

"""
    memoize_ainfo!(key::UInt, ainfo) -> AbstractAliasing

Record `ainfo` as the region's answer for `key`, for a caller that obtained it
some other way than by asking `memoized_ainfo` -- a batch exchange
(`batch_aliasing`) resolves many arguments at once, and seeding its results here
is what keeps the rest of planning from re-deriving them one collective at a
time. Returns the entry in force, which is the existing one if any (a batch never
contradicts what has already been computed).
"""
function memoize_ainfo!(key::UInt, ainfo::AbstractAliasing)
    memo = CHUNK_AINFO_MEMO[]
    memo === nothing && return ainfo
    @lock memo.lock begin
        return get!(memo.entries, key, ainfo)
    end
end

struct ArgumentWrapper
    arg
    dep_mod
    hash::UInt

    function ArgumentWrapper(arg, dep_mod)
        h = hash(dep_mod)
        h = _identity_hash(arg, h)
        @check_uniform(h, arg)
        return new(arg, dep_mod, h)
    end
end
Base.hash(aw::ArgumentWrapper, h::UInt) = hash(aw.hash, hash(ArgumentWrapper, h))
Base.:(==)(aw1::ArgumentWrapper, aw2::ArgumentWrapper) =
    aw1.hash == aw2.hash
Base.isequal(aw1::ArgumentWrapper, aw2::ArgumentWrapper) =
    aw1.hash == aw2.hash

struct DataDepsTaskDependency
    arg_w::ArgumentWrapper
    readdep::Bool
    writedep::Bool
end
DataDepsTaskDependency(arg, dep) =
    DataDepsTaskDependency(ArgumentWrapper(arg, dep[1]), dep[2], dep[3])

"""
    TaskArgInfo

Flat, concrete per-argument record produced by `populate_task_info!` into
`DataDepsState`'s per-task scratch vectors. The argument's dependencies live in
`state.scratch_deps[dep_start:dep_stop]`. Replaces the former heterogeneous
tuple of `TypedDataDepsTaskArgument{T,N}`s, which forced every planning
traversal to re-box tuple elements and copy the whole tuple across each
dynamic closure boundary.
"""
struct TaskArgInfo
    arg::Any        # unwrapped argument, or its tracking Chunk when aliasing
    pos::ArgPosition
    may_alias::Bool
    inplace_move::Bool
    dep_start::Int
    dep_stop::Int
end
arg_deps_range(info::TaskArgInfo) = info.dep_start:info.dep_stop

# Self-contained per-argument record for logging payloads: the scratch
# `TaskArgInfo`s reference `state.scratch_deps` by index and are recycled per
# task, so log consumers get a snapshot with the deps materialized inline.
struct LoggedTaskArg
    arg::Any
    pos::ArgPosition
    may_alias::Bool
    inplace_move::Bool
    deps::Vector{DataDepsTaskDependency}
end
logged_task_args(deps_vec::Vector{DataDepsTaskDependency}, infos::Vector{TaskArgInfo}) =
    [LoggedTaskArg(i.arg, i.pos, i.may_alias, i.inplace_move,
                   deps_vec[arg_deps_range(i)]) for i in infos]

struct HistoryEntry
    ainfo::AliasingWrapper
    space::MemorySpace
    write_num::Int
    # Producer of this write/copy. Remainder syncdeps wait on this task
    # directly instead of re-resolving through live `ainfos_owner`, which may
    # later name a different task for the same ainfo or miss the producer when
    # only an overlapping ainfo is consulted.
    task::DTask
end

struct AliasedObjectCacheStore
    accel::Acceleration
    keys::Vector{AbstractAliasing}
    derived::Dict{AbstractAliasing,AbstractAliasing}
    stored::Dict{MemorySpace,Set{AbstractAliasing}}
    values::Dict{MemorySpace,Dict{AbstractAliasing,Chunk}}
    # The `(space, key)` pairs identifying the user's original data, as opposed
    # to Datadeps-allocated copies. A `key` ainfo is recorded here at the space
    # where it is first registered (its source space, see `set_key_stored!`),
    # which is exactly where its value is the user's own object. Every *other*
    # space holding that key is a copy we allocated and may free.
    originals::Set{Tuple{MemorySpace,AbstractAliasing}}
    # Copies whose own (destination-side) ainfo has not been computed yet, as
    # `key => copy`. See `resolve_pending!`.
    pending::Vector{Pair{AbstractAliasing,Chunk}}
end
AliasedObjectCacheStore(accel::Acceleration) =
    AliasedObjectCacheStore(accel,
                            Vector{AbstractAliasing}(),
                            Dict{AbstractAliasing,AbstractAliasing}(),
                            Dict{MemorySpace,Set{AbstractAliasing}}(),
                            Dict{MemorySpace,Dict{AbstractAliasing,Chunk}}(),
                            Set{Tuple{MemorySpace,AbstractAliasing}}(),
                            Vector{Pair{AbstractAliasing,Chunk}}())

"""
    resolve_pending!(cache) -> Bool

Give every copy recorded by `set_stored!` its `derived` entry, returning whether
there was anything to do.

`set_stored!` defers this because obtaining a copy's own ainfo is expensive
exactly where it is least likely to be needed. The ainfo describes pointer spans
in the destination space, so under MPI only the destination rank can compute it
and it has to be broadcast to every other rank -- a second rendezvous per slot,
on top of the transfer, which measured as about a quarter of `distribute_task!`
for a 2-rank stencil sweep. What it buys is the ability to recognize a copy when
that copy is itself the *source* of a later move, so that both hops share one
cache key. Regions that never take a second hop -- the common case -- never need
it at all.

Deferring is safe under SPMD because the trigger is uniform: `derived` and the
ainfo being looked up are rank-uniform, so every rank misses, and resolves, at
the same point and in the same order.
"""
function resolve_pending!(cache::AliasedObjectCacheStore)
    isempty(cache.pending) && return false
    entries = copy(cache.pending)
    empty!(cache.pending)
    @opcounter :aliasing_resolve_pending
    @opcounter :aliasing_resolve_pending_entries length(entries)
    # Resolved as a batch: the whole point of deferring was to not pay a
    # rendezvous per copy, which asking one at a time here would reintroduce.
    values = Chunk[value for (_, value) in entries]
    dep_mods = Any[identity for _ in entries]
    for (i, ainfo) in enumerate(batch_ainfos(cache.accel, values, dep_mods))
        cache.derived[ainfo] = first(entries[i])
    end
    return true
end

"`cache.derived[ainfo]`, resolving deferred copies first, or `nothing` if absent."
function derived_key(cache::AliasedObjectCacheStore, ainfo::AbstractAliasing)
    key = get(cache.derived, ainfo, nothing)
    key === nothing || return key
    resolve_pending!(cache) || return nothing
    return get(cache.derived, ainfo, nothing)
end

"""
    is_original(cache, space, ainfo) -> Bool

Whether `cache.values[space][ainfo]` holds the user's original data (which must
never be freed) rather than a Datadeps-allocated copy. True only at the `key`
ainfo's source space (recorded by `set_key_stored!`).
"""
is_original(cache::AliasedObjectCacheStore, space::MemorySpace, ainfo::AbstractAliasing) =
    (space, ainfo) in cache.originals

function is_stored(cache::AliasedObjectCacheStore, space::MemorySpace, ainfo::AbstractAliasing)
    if !haskey(cache.stored, space)
        return false
    end
    key = derived_key(cache, ainfo)
    key === nothing && return false
    return key in cache.stored[space]
end
function is_key_present(cache::AliasedObjectCacheStore, space::MemorySpace, ainfo::AbstractAliasing)
    return derived_key(cache, ainfo) !== nothing
end
function get_stored(cache::AliasedObjectCacheStore, space::MemorySpace, ainfo::AbstractAliasing)
    key = derived_key(cache, ainfo)
    @assert key !== nothing "Cache does not have derived ainfo $ainfo"
    return cache.values[space][key]
end
function set_stored!(cache::AliasedObjectCacheStore, dest_space::MemorySpace, value::Chunk, ainfo::AbstractAliasing)
    @assert !is_stored(cache, dest_space, ainfo) "Cache already has derived ainfo $ainfo"
    @check_uniform(value)
    key = cache.derived[ainfo]
    push!(cache.pending, key => value)
    push!(get!(Set{AbstractAliasing}, cache.stored, dest_space), key)
    values_dict = get!(Dict{AbstractAliasing,Chunk}, cache.values, dest_space)
    values_dict[key] = value
    return
end
function set_key_stored!(cache::AliasedObjectCacheStore, space::MemorySpace, ainfo::AbstractAliasing, value::Chunk)
    @check_uniform(value)
    push!(cache.keys, ainfo)
    cache.derived[ainfo] = ainfo
    # A key is first registered at the space where the original object lives, so
    # `value` here is the user's own data; record it so it's never freed.
    push!(cache.originals, (space, ainfo))
    push!(get!(Set{AbstractAliasing}, cache.stored, space), ainfo)
    values_dict = get!(Dict{AbstractAliasing,Chunk}, cache.values, space)
    values_dict[ainfo] = value
    return
end

struct AliasedObjectCache
    accel::Acceleration
    space::MemorySpace
    chunk::Chunk
end
function is_stored(cache::AliasedObjectCache, ainfo::AbstractAliasing)
    wid = root_worker_id(cache.chunk)
    if wid != myid()
        return remotecall_fetch(is_stored, wid, cache, ainfo)
    end
    cache_raw = unwrap(cache.chunk)::AliasedObjectCacheStore
    return is_stored(cache_raw, cache.space, ainfo)
end
function is_key_present(cache::AliasedObjectCache, space::MemorySpace, ainfo::AbstractAliasing)
    wid = root_worker_id(cache.chunk)
    if wid != myid()
        return remotecall_fetch(is_key_present, wid, cache, space, ainfo)
    end
    cache_raw = unwrap(cache.chunk)::AliasedObjectCacheStore
    return is_key_present(cache_raw, space, ainfo)
end
function get_stored(cache::AliasedObjectCache, ainfo::AbstractAliasing)
    wid = root_worker_id(cache.chunk)
    if wid != myid()
        return remotecall_fetch(get_stored, wid, cache, ainfo)
    end
    cache_raw = unwrap(cache.chunk)::AliasedObjectCacheStore
    return get_stored(cache_raw, cache.space, ainfo)
end

function set_stored!(accel::DistributedAcceleration, cache::AliasedObjectCache, value::Chunk, ainfo::AbstractAliasing)
    wid = root_worker_id(cache.chunk)
    if wid != myid()
        return remotecall_fetch(set_stored!, wid, accel, cache, value, ainfo)
    end
    cache_raw = unwrap(cache.chunk)::AliasedObjectCacheStore
    set_stored!(cache_raw, cache.space, value, ainfo)
    return
end

function set_key_stored!(accel::DistributedAcceleration, cache::AliasedObjectCache, space::MemorySpace, ainfo::AbstractAliasing, value::Chunk)
    wid = root_worker_id(cache.chunk)
    if wid != myid()
        return remotecall_fetch(set_key_stored!, wid, accel, cache, space, ainfo, value)
    end
    cache_raw = unwrap(cache.chunk)::AliasedObjectCacheStore
    set_key_stored!(cache_raw, space, ainfo, value)
end

function aliased_object!(f, cache::AliasedObjectCache, x; ainfo=aliasing(cache.accel, x, identity))
    x_space = memory_space(x)
    if !is_key_present(cache, x_space, ainfo)
        # Preserve the object's memory-space/processor pairing when inserting
        # the source key. Using bare `tochunk(x)` defaults to OSProc, which can
        # incorrectly wrap GPU-backed objects as CPU chunks.
        x_chunk = x isa Chunk ? x : tochunk(x, first(processors(x_space)))
        set_key_stored!(cache.accel, cache, x_space, ainfo, x_chunk)
    end
    if is_stored(cache, ainfo)
        return get_stored(cache, ainfo)
    else
        y = f(x)
        @assert y isa Chunk "Didn't get a Chunk from functor"
        # N.B. Deliberately not also checking that `y`'s ainfo differs from `x`'s
        # when the spaces differ: distinct memory spaces hold distinct memory, so
        # the space assertion above already implies it, and asking for `y`'s ainfo
        # here would cost a collective per slot (see `resolve_pending!`).
        @assert memory_space(y) == cache.space "Space mismatch! $(memory_space(y)) != $(cache.space)"
        set_stored!(cache.accel, cache, y, ainfo)
        return y
    end
end

# N.B. Declared `mutable` (despite no field ever being rebound) purely for
# identity: an immutable struct this large is re-boxed on every dynamic call
# that passes it, and planning threads it through hundreds of such calls per
# task. A mutable struct has a stable heap identity and is passed by pointer
# instead, so those boxes disappear. It is never used as a `Dict` key, so the
# resulting identity-based `hash`/`==` are not observed.
mutable struct DataDepsState
    # The mapping of original raw argument to its Chunk
    # N.B. Values are Chunks, or raw remote handles (e.g. ChunkView under MPI)
    raw_arg_to_chunk::IdDict{Any,Any}

    # The origin memory space of each argument
    # Used to track the original location of an argument, for final copy-from
    arg_origin::IdDict{Any,MemorySpace}

    # The mapping of memory space to argument to remote argument copies
    # Used to replace an argument with its remote copy
    # N.B. Values are Chunks, or raw remote handles (e.g. ChunkView under MPI),
    # matching `raw_arg_to_chunk` above -- hence `IdDict{Any,Any}`. Every
    # construction site (`generate_slot!`, `get_or_generate_slot!`,
    # hierarchical's ownership sync) builds `IdDict{Any,Any}`; declaring a
    # narrower value type here only forced a convert+copy on each of them.
    remote_args::Dict{MemorySpace,IdDict{Any,Any}}

    # The mapping of remote argument to original argument
    remote_arg_to_original::IdDict{Any,Any}

    # The mapping of original argument wrapper to remote argument wrapper
    remote_arg_w::Dict{ArgumentWrapper,Dict{MemorySpace,ArgumentWrapper}}

    # The mapping of ainfo to argument and dep_mod
    # Used to lookup which argument and dep_mod a given ainfo is generated from
    # N.B. This is a mapping for remote argument copies
    ainfo_arg::Dict{AliasingWrapper,Set{ArgumentWrapper}}

    # The history of writes (direct or indirect) to each argument and dep_mod, in terms of ainfos directly written to, and the memory space they were written to
    # Updated when a new write happens on an overlapping ainfo
    # Used by remainder copies to track which portions of an argument and dep_mod were written to elsewhere, through another argument
    arg_history::Dict{ArgumentWrapper,Vector{HistoryEntry}}

    # The mapping of memory space and argument to the memory space of the last direct write
    # Used by remainder copies to lookup the "backstop" if any portion of the target ainfo is not updated by the remainder
    arg_owner::Dict{ArgumentWrapper,MemorySpace}

    # The set of memory spaces holding a fully-current replica of each argument
    # and dep_mod: a task write resets it to the written space, while a copy
    # extends it (the copy's source remains current)
    # Used to elide copies, notably the final copy-from of read-only arguments
    arg_current::Dict{ArgumentWrapper,Set{MemorySpace}}

    # The overlap of each argument with every other argument, based on the ainfo overlaps
    # Incrementally updated as new ainfos are created
    # Used for fast history updates
    arg_overlaps::Dict{ArgumentWrapper,Set{ArgumentWrapper}}

    # The mapping of, for a given memory space, the backing Chunks that an ainfo references
    # Used by slot generation to replace the backing Chunks during move
    ainfo_backing_chunk::Chunk{AliasedObjectCacheStore}

    # Cache of argument's supports_inplace_move query result
    supports_inplace_cache::IdDict{Any,Bool}

    # Cache of argument and dep_mod to ainfo
    # N.B. This is a mapping for remote argument copies
    ainfo_cache::Dict{ArgumentWrapper,AliasingWrapper}

    # The oracle for aliasing lookups
    # Used to populate ainfos_overlaps efficiently
    ainfos_lookup::AliasingLookup

    # The overlapping ainfos for each ainfo
    # Incrementally updated as new ainfos are created
    # Used for fast will_alias lookups
    ainfos_overlaps::Dict{AliasingWrapper,Set{AliasingWrapper}}

    # Track writers ("owners") and readers
    # Updated as new writer and reader tasks are launched
    # Used by task dependency tracking to calculate syncdeps and ensure correct launch ordering
    ainfos_owner::Dict{AliasingWrapper,Union{Pair{DTask,Int},Nothing}}
    ainfos_readers::Dict{AliasingWrapper,Vector{Pair{DTask,Int}}}

    # Per-task scratch buffers filled by `populate_task_info!` and consumed by
    # `distribute_task!` (which plans one task at a time per state; parallel
    # hierarchical partitions each own a private state). Reused across tasks.
    scratch_args::Vector{TaskArgInfo}
    scratch_deps::Vector{DataDepsTaskDependency}
    scratch_remote::Vector{Any}

    function DataDepsState()
        arg_to_chunk = IdDict{Any,Any}()
        arg_origin = IdDict{Any,MemorySpace}()
        remote_args = Dict{MemorySpace,IdDict{Any,Any}}()
        remote_arg_to_original = IdDict{Any,Any}()
        remote_arg_w = Dict{ArgumentWrapper,Dict{MemorySpace,ArgumentWrapper}}()
        ainfo_arg = Dict{AliasingWrapper,Set{ArgumentWrapper}}()
        arg_history = Dict{ArgumentWrapper,Vector{HistoryEntry}}()
        arg_owner = Dict{ArgumentWrapper,MemorySpace}()
        arg_current = Dict{ArgumentWrapper,Set{MemorySpace}}()
        arg_overlaps = Dict{ArgumentWrapper,Set{ArgumentWrapper}}()
        accel = current_acceleration()
        ainfo_backing_chunk = _with_default_acceleration() do
            tochunk(AliasedObjectCacheStore(accel))
        end

        supports_inplace_cache = IdDict{Any,Bool}()
        ainfo_cache = Dict{ArgumentWrapper,AliasingWrapper}()

        ainfos_lookup = AliasingLookup()
        ainfos_overlaps = Dict{AliasingWrapper,Set{AliasingWrapper}}()

        ainfos_owner = Dict{AliasingWrapper,Union{Pair{DTask,Int},Nothing}}()
        ainfos_readers = Dict{AliasingWrapper,Vector{Pair{DTask,Int}}}()

        return new(arg_to_chunk, arg_origin, remote_args, remote_arg_to_original, remote_arg_w, ainfo_arg, arg_history, arg_owner, arg_current, arg_overlaps, ainfo_backing_chunk,
                   supports_inplace_cache, ainfo_cache, ainfos_lookup, ainfos_overlaps, ainfos_owner, ainfos_readers,
                   TaskArgInfo[], DataDepsTaskDependency[], Any[])
    end
end

function supports_inplace_move(state::DataDepsState, arg)
    return get!(state.supports_inplace_cache, arg) do
        return supports_inplace_move(arg)
    end
end

# Determine which arguments could be written to, and thus need tracking
"Whether `arg` is written to by `task`."
function is_writedep(arg, deps, task::DTask)
    return any(dep->dep[3], deps)
end

# Wrap a non-Chunk argument for datadeps tracking. Most values become local
# Chunks; remote handles that are already rank-replicated metadata under SPMD
# (e.g. ChunkView under MPI) override this to stay raw.
datadeps_arg_wrap(arg) = tochunk(arg)

"""
    get_or_make_arg_chunk!(state, arg, task) -> chunk

Returns the `Chunk` (or raw remote handle) tracking `arg`, generating and
recording it on first sight.

N.B. Split out of `populate_task_info!`'s per-argument closure deliberately: as
an inline `if`/`else` the result was assigned in three branches and then
captured by the closures below it, which forced Julia to box it. A single
assignment from a call keeps it unboxed.
"""
function get_or_make_arg_chunk!(state::DataDepsState, arg, task::DTask)
    existing = get(state.raw_arg_to_chunk, arg, nothing)
    existing === nothing || return existing
    if arg isa Chunk
        state.raw_arg_to_chunk[arg] = arg
        return arg
    end
    arg_chunk = with(DATADEPS_THUNK_ID=>task.uid) do
        datadeps_arg_wrap(arg)
    end
    state.raw_arg_to_chunk[arg] = arg_chunk
    return arg_chunk
end

# Aliasing state setup
#
# Fills `state.scratch_args`/`state.scratch_deps` with one flat, concrete
# `TaskArgInfo` per task argument (position 1 is `f_arg`, the already-moved
# function argument), and returns `state.scratch_args`. The buffers are valid
# until the next `populate_task_info!` call on this state.
function populate_task_info!(state::DataDepsState, f_arg, fargs, spec::DTaskSpec, task::DTask)
    infos = state.scratch_args
    deps_vec = state.scratch_deps
    empty!(infos)
    empty!(deps_vec)
    for idx in 1:length(fargs)
        _arg = idx == 1 ? f_arg : fargs[idx]
        # N.B. Function barrier: `_arg` is abstract here (heterogeneous tuple /
        # Vector{Argument} element), so processing it inline boxes every field
        # read and destructure; one dynamic dispatch specializes the body on
        # the concrete argument type instead.
        _populate_one_arg!(state, infos, deps_vec, _arg, task)
    end
    return infos
end
function _populate_one_arg!(state::DataDepsState, infos::Vector{TaskArgInfo},
                            deps_vec::Vector{DataDepsTaskDependency}, _arg, task::DTask)
    # Unwrap the argument
    _arg_with_deps = value(_arg)
    pos = _arg.pos

    # Unwrap In/InOut/Out wrappers and record dependencies
    arg_pre_unwrap, raw_deps = unwrap_inout(_arg_with_deps)

    # Unwrap the Chunk underlying any DTask arguments
    arg = arg_pre_unwrap isa DTask ? fetch(arg_pre_unwrap; raw=true) : arg_pre_unwrap

    # Skip non-aliasing arguments or arguments that don't support in-place move
    may_alias = type_may_alias(typeof(arg))
    inplace_move = may_alias && supports_inplace_move(state, arg)
    dep_start = length(deps_vec) + 1
    if !may_alias || !inplace_move
        arg_w = ArgumentWrapper(arg, identity)
        push!(deps_vec, DataDepsTaskDependency(arg_w, false, false))
        push!(infos, TaskArgInfo(arg, pos, may_alias, inplace_move, dep_start, length(deps_vec)))
        return
    end

    # Generate a Chunk for the argument if necessary
    arg_chunk = get_or_make_arg_chunk!(state, arg, task)

    # Track the origin space of the argument
    origin_space = memory_space(arg_chunk)
    @check_uniform(origin_space)
    state.arg_origin[arg_chunk] = origin_space
    state.remote_arg_to_original[arg_chunk] = arg_chunk

    # Record and populate argument info for all aliasing dependencies
    for dep in raw_deps
        dep_full = DataDepsTaskDependency(arg_chunk, dep)
        push!(deps_vec, dep_full)
        populate_argument_info!(state, dep_full.arg_w, origin_space)
    end
    push!(infos, TaskArgInfo(arg_chunk, pos, may_alias, inplace_move, dep_start, length(deps_vec)))
    return
end
function populate_argument_info!(state::DataDepsState, arg_w::ArgumentWrapper, origin_space::MemorySpace)
    # Initialize ownership and history
    if !haskey(state.arg_owner, arg_w)
        # N.B. This is valid (even if the backing data is up-to-date elsewhere),
        # because we only use this to track the "backstop" if any portion of the
        # target ainfo is not updated by the remainder (at which point, this
        # is thus the correct owner).
        state.arg_owner[arg_w] = origin_space

        # Initialize the overlap set
        state.arg_overlaps[arg_w] = Set{ArgumentWrapper}()
    end
    if !haskey(state.arg_history, arg_w)
        state.arg_history[arg_w] = Vector{HistoryEntry}()
    end

    # Calculate the ainfo (which will populate ainfo structures and merge history)
    aliasing!(state, origin_space, arg_w)
end
# N.B. arg_w must be the original argument wrapper, not a remote copy
function aliasing!(state::DataDepsState, target_space::MemorySpace, arg_w::ArgumentWrapper)
    if haskey(state.remote_arg_w, arg_w) && haskey(state.remote_arg_w[arg_w], target_space)
        remote_arg_w = @inbounds state.remote_arg_w[arg_w][target_space]
        remote_arg = remote_arg_w.arg
    else
        # Grab the remote copy of the argument, and calculate the ainfo
        remote_arg = get_or_generate_slot!(state, target_space, arg_w.arg)
        remote_arg_w = ArgumentWrapper(remote_arg, arg_w.dep_mod)
        get!(Dict{MemorySpace,ArgumentWrapper}, state.remote_arg_w, arg_w)[target_space] = remote_arg_w
    end

    # Check if we already have the result cached
    if haskey(state.ainfo_cache, remote_arg_w)
        return state.ainfo_cache[remote_arg_w]
    end

    # Calculate the ainfo
    ainfo = AliasingWrapper(aliasing(current_acceleration(), remote_arg, arg_w.dep_mod))

    # Cache the result
    state.ainfo_cache[remote_arg_w] = ainfo

    # Update the mapping of ainfo to argument and dep_mod
    if !haskey(state.ainfo_arg, ainfo)
        state.ainfo_arg[ainfo] = Set{ArgumentWrapper}([remote_arg_w])
    end
    push!(state.ainfo_arg[ainfo], remote_arg_w)

    # Populate info for the new ainfo
    populate_ainfo!(state, arg_w, ainfo, target_space)

    return ainfo
end
function populate_ainfo!(state::DataDepsState, original_arg_w::ArgumentWrapper, target_ainfo::AliasingWrapper, target_space::MemorySpace)
    if !haskey(state.ainfos_owner, target_ainfo)
        # Add ourselves to the lookup oracle
        ainfo_idx = push!(state.ainfos_lookup, target_ainfo)

        # Find overlapping ainfos
        overlaps = Set{AliasingWrapper}()
        push!(overlaps, target_ainfo)
        for other_ainfo in intersect(state.ainfos_lookup, target_ainfo; ainfo_idx)
            target_ainfo == other_ainfo && continue
            # Mark us and them as overlapping
            push!(overlaps, other_ainfo)
            push!(state.ainfos_overlaps[other_ainfo], target_ainfo)

            # Add overlapping history to our own
            for other_remote_arg_w in state.ainfo_arg[other_ainfo]
                other_arg = state.remote_arg_to_original[other_remote_arg_w.arg]
                other_arg_w = ArgumentWrapper(other_arg, other_remote_arg_w.dep_mod)
                push!(state.arg_overlaps[original_arg_w], other_arg_w)
                push!(state.arg_overlaps[other_arg_w], original_arg_w)
                merge_history!(state, original_arg_w, other_arg_w)
            end
        end
        state.ainfos_overlaps[target_ainfo] = overlaps

        # Initialize owner and readers
        state.ainfos_owner[target_ainfo] = nothing
        state.ainfos_readers[target_ainfo] = Pair{DTask,Int}[]
    end
end
function merge_history!(state::DataDepsState, arg_w::ArgumentWrapper, other_arg_w::ArgumentWrapper)
    history = state.arg_history[arg_w]
    @opcounter :merge_history
    @opcounter :merge_history_complexity length(history)
    for other_entry in state.arg_history[other_arg_w]
        range = searchsorted(history, other_entry; by=x->x.write_num)
        if !isempty(range)
            # Find and skip duplicates
            match = false
            for source_idx in range
                source_entry = history[source_idx]
                if source_entry.ainfo == other_entry.ainfo &&
                    source_entry.space == other_entry.space &&
                    source_entry.write_num == other_entry.write_num &&
                    source_entry.task == other_entry.task
                    match = true
                    break
                end
            end
            match && continue

            # Insert at the first position
            idx = first(range)
        else
            # Insert at the last position
            idx = length(history) + 1
        end
        insert!(history, idx, other_entry)
    end
end
function truncate_history!(state::DataDepsState, arg_w::ArgumentWrapper)
    # FIXME: Do this continuously if possible
    if haskey(state.arg_history, arg_w) && length(state.arg_history[arg_w]) > 100000
        origin_space = state.arg_origin[arg_w.arg]
        @opcounter :truncate_history
        _, last_idx = compute_remainder_for_arg!(state, origin_space, arg_w, 0; compute_syncdeps=false)
        if last_idx > 0
            @opcounter :truncate_history_removed last_idx
            deleteat!(state.arg_history[arg_w], 1:last_idx)
        end
    end
end

"""
    supports_inplace_move(x) -> Bool

Returns `false` if `x` doesn't support being copied into from another object
like `x`, via `move!`. This is used in `spawn_datadeps` to prevent attempting
to copy between values which don't support mutation or otherwise don't have an
implemented `move!` and want to skip in-place copies. When this returns
`false`, datadeps will instead perform out-of-place copies for each non-local
use of `x`, and the data in `x` will not be updated when the `spawn_datadeps`
region returns.

This is deliberately a *type-level* query: it excludes values that
`type_may_alias` reports as aliasing but which cannot actually be mutated in
place (a `Function` closure that captures arrays is the canonical example —
it aliases its captures, but there is no `move!` that writes into a closure).
Deciding from the type alone (rather than inspecting the value) is what makes
this usable under SPMD/MPI execution, where a `Chunk`'s payload is not
materialized on every rank but its `chunktype` is known everywhere.

Note this is distinct from whether a backend can perform a *zero-copy*
in-place transfer of the payload (e.g. MPIExt's `supports_inplace_mpi`, which
additionally requires a bits-eltype `DenseArray`); that is a transport-layer
optimization applied only once an in-place move has been scheduled.
"""
supports_inplace_move(x) = supports_inplace_move(typeof(x))
supports_inplace_move(t::DTask) = supports_inplace_move(chunktype(t))
supports_inplace_move(c::Chunk) = supports_inplace_move(chunktype(c))
supports_inplace_move(::Type) = true
supports_inplace_move(::Type{<:Function}) = false

# Read/write dependency management
function get_write_deps!(state::DataDepsState, dest_space::MemorySpace, ainfo::AbstractAliasing, write_num, syncdeps)
    # We need to sync with both writers and readers
    _get_write_deps!(state, dest_space, ainfo, write_num, syncdeps)
    _get_read_deps!(state, dest_space, ainfo, write_num, syncdeps)
end
function get_read_deps!(state::DataDepsState, dest_space::MemorySpace, ainfo::AbstractAliasing, write_num, syncdeps)
    # We only need to sync with writers, not readers
    _get_write_deps!(state, dest_space, ainfo, write_num, syncdeps)
end

function _get_write_deps!(state::DataDepsState, dest_space::MemorySpace, ainfo::AbstractAliasing, write_num, syncdeps)
    ainfo.inner isa NoAliasing && return
    for other_ainfo in state.ainfos_overlaps[ainfo]
        other_task_write_num = state.ainfos_owner[other_ainfo]
        @dagdebug nothing :spawn_datadeps_sync "Considering sync with writer via $ainfo -> $other_ainfo"
        other_task_write_num === nothing && continue
        other_task, other_write_num = other_task_write_num
        write_num == other_write_num && continue
        @dagdebug nothing :spawn_datadeps_sync "Sync with writer via $ainfo -> $other_ainfo"
        push!(syncdeps, ThunkSyncdep(other_task))
    end
end
function _get_read_deps!(state::DataDepsState, dest_space::MemorySpace, ainfo::AbstractAliasing, write_num, syncdeps)
    ainfo.inner isa NoAliasing && return
    for other_ainfo in state.ainfos_overlaps[ainfo]
        @dagdebug nothing :spawn_datadeps_sync "Considering sync with reader via $ainfo -> $other_ainfo"
        other_tasks = state.ainfos_readers[other_ainfo]
        for (other_task, other_write_num) in other_tasks
            write_num == other_write_num && continue
            @dagdebug nothing :spawn_datadeps_sync "Sync with reader via $ainfo -> $other_ainfo"
            push!(syncdeps, ThunkSyncdep(other_task))
        end
    end
end
# Whether the raw data backing `x` can be inspected on the current process to
# compute its aliasing. Distributed `Chunk`s are always inspectable (locally or
# via `remotecall`); the MPI extension overrides this for refs owned by a
# different rank, where the data is not present on this rank.
aliasing_available(@nospecialize(x)) = true

# Whether `aliasing(x)` may be called here. Under uniform (SPMD) execution
# `aliasing` is a collective -- the owner computes and broadcasts, every other
# rank receives -- so availability must be answered identically on every rank:
# branching on the rank-local `aliasing_available` sends the owner alone into
# the broadcast and hangs the region (or mismatches tags with whatever
# collective the other ranks reached instead).
aliasing_obtainable(@nospecialize(x)) = uniform_execution() || aliasing_available(x)

"""
    gather_overlap_syncdeps!(state, ainfo, write_num, syncdeps)

Add to `syncdeps` the owner and readers of every tracked ainfo in
`state.ainfos_lookup` overlapping `ainfo`. Used by `gather_free_syncdeps!` for
buffers that are not themselves a directly-tracked task dependency, so their
overlaps are not already precomputed in `state.ainfos_overlaps`.

We reuse the lookup's interval-tree overlap search (which prunes most
`will_alias` comparisons via bounding spans) rather than scanning every tracked
ainfo, via `intersect_ad_hoc` -- a variant of `intersect` for a query `ainfo`
that is not registered in the lookup and never will be.

# N.B. Why not just `push!` and `intersect`, like a normal query

An earlier version of this function `push!`ed `ainfo` into `state.ainfos_lookup`
to satisfy `intersect`'s requirement that its query be a registered entry,
reasoning that this was safe because the free loop was the final step of
`distribute_tasks!`, after which the lookup was never consulted again -- the
whole `state` was dropped at the end of the region.

`DataDepsContext` (`context.jl`) now exists to eventually give `state` a
longer lifetime than one region; as of this phase `distribute_tasks!` still
rebuilds `state` fresh every region (see the N.B. there), so that assumption
happens to still hold today. But it is exactly the assumption a later phase
removes on purpose, and it was already shakier than it looked even within one
region: the free loop calls this function once per freed buffer, all against
the *same* `state`, so every earlier buffer's throwaway push was still sitting
in the lookup -- and being walked as a candidate, then rejected -- for every
later buffer's query in that same loop. Once `state` does outlive a region,
"walked and rejected" becomes "unbounded growth of the interval tree, and
query results that have to filter out entries naming no tracked argument", for
the next region's planning and (Phase 6) `AliasingLookupFinder` interop queries
alike. Fixing it now, alongside the container that will eventually make it a
real bug rather than a wasted comparison, means it ships as a pure improvement
today instead of a fire drill later. `intersect_ad_hoc` computes the query's
bounding spans on the fly instead of registering them, so there is nothing to
walk past and nothing to undo.
"""
function gather_overlap_syncdeps!(state::DataDepsState, ainfo::AliasingWrapper, write_num::Int, syncdeps)
    ainfo.inner isa NoAliasing && return
    for other_ainfo in intersect_ad_hoc(state.ainfos_lookup, ainfo)
        owner = get(state.ainfos_owner, other_ainfo, nothing)
        if owner !== nothing
            owner_task, owner_write_num = owner
            owner_write_num != write_num && push!(syncdeps, ThunkSyncdep(owner_task))
        end
        for (reader_task, reader_write_num) in get(state.ainfos_readers, other_ainfo, ())
            reader_write_num != write_num && push!(syncdeps, ThunkSyncdep(reader_task))
        end
    end
end

# Debug-only invariant: freeing a buffer must never produce an empty syncdep
# set while the state still records a writer or readers overlapping it. Gated
# behind this `Ref` (following the same pattern as `CHECK_UNIFORMITY`,
# acceleration.jl:75) so it costs nothing -- not even a function call, since
# `assert_free_syncdeps!` itself is only ever invoked from within
# `gather_free_syncdeps!`, which is already off the hot path -- in normal
# operation. Flip it on in tests that want this checked.
#
# Deliberately re-derives the answer independently of `gather_free_syncdeps!`
# / `gather_overlap_syncdeps!` (a linear scan over every tracked ainfo plus
# `will_alias`, instead of the interval-tree search they use) so that a bug in
# the fast path can't also hide from the assertion meant to catch it.
const DATADEPS_ASSERT_FREE_SYNCDEPS = Ref(false)
function assert_free_syncdeps!(state::DataDepsState, ainfo::AliasingWrapper, write_num::Int, syncdeps)
    DATADEPS_ASSERT_FREE_SYNCDEPS[] || return
    ainfo.inner isa NoAliasing && return
    for (other_ainfo, owner) in state.ainfos_owner
        owner === nothing && continue
        owner_task, owner_write_num = owner
        owner_write_num == write_num && continue
        will_alias(ainfo, other_ainfo) || continue
        @assert ThunkSyncdep(owner_task) in syncdeps "gather_free_syncdeps! omitted a live writer $owner_task ($other_ainfo) for buffer overlapping $ainfo"
    end
    for (other_ainfo, readers) in state.ainfos_readers
        for (reader_task, reader_write_num) in readers
            reader_write_num == write_num && continue
            will_alias(ainfo, other_ainfo) || continue
            @assert ThunkSyncdep(reader_task) in syncdeps "gather_free_syncdeps! omitted a live reader $reader_task ($other_ainfo) for buffer overlapping $ainfo"
        end
    end
end

"""
    gather_free_syncdeps!(state, space, key_ainfo, remote_arg, write_num, chunk_to_ainfos, syncdeps)

Collect into `syncdeps` every task that must complete before the backing buffer
`remote_arg` (a Datadeps-allocated copy in `space`) can be freed.

If `remote_arg` is itself a tracked slot (the common case -- whole-object
arguments), its ainfos are in `chunk_to_ainfos` and we reuse their precomputed
overlap sets. Otherwise the buffer only underlies wrapper arguments (e.g. it is
the parent array shared by several `view`s, whose tracked slots are the views
rather than this buffer); in that case we compute the buffer's own aliasing and
sync with every tracked ainfo that overlaps its memory. When the buffer's
aliasing cannot be obtained here (`aliasing_obtainable` is `false`), we fall back
to the rank-uniform cache key ainfo `key_ainfo`, run through the same overlap
search (`gather_overlap_syncdeps!`) as the wrapper-argument case.
"""
function gather_free_syncdeps!(state::DataDepsState, space::MemorySpace, key_ainfo, remote_arg, write_num::Int, chunk_to_ainfos, syncdeps)
    ainfos = get(chunk_to_ainfos, remote_arg, nothing)
    if ainfos !== nothing
        for ainfo in ainfos
            get_write_deps!(state, space, ainfo, write_num, syncdeps)
            assert_free_syncdeps!(state, ainfo, write_num, syncdeps)
        end
        return
    end

    # If the buffer's aliasing cannot be obtained here, fall back to the cache
    # key ainfo, which is metadata available identically on every rank (it was
    # already computed, as a concrete value, when the buffer was first inserted
    # into the object cache -- no remote inspection is needed to use it here).
    # The cache stores raw ainfos, so wrap it to match the `AliasingWrapper`
    # keys used by the overlap tracking.
    #
    # N.B. `key_ainfo` is frequently *not* itself a key of `ainfos_overlaps`:
    # that dict is only populated by `populate_ainfo!`, for ainfos that were
    # directly tracked as a task dependency via `aliasing!`. The buffers this
    # branch exists for -- their own aliasing unavailable locally, which today
    # only happens for an MPI `Chunk{<:Any,<:MPIRef}` owned by a different rank
    # (see `aliasing_available` overload in `ext/MPIExt.jl`) -- are exactly the
    # ones that are *not* directly tracked; they only underlie wrapper
    # arguments (the same situation as the fallthrough case below). A previous
    # version of this fallback only synced when `key_ainfo` happened to already
    # be a registered ainfo (`haskey(state.ainfos_overlaps, ...)`), which was
    # close to never true, silently producing zero syncdeps and freeing a
    # buffer still in use. Route through the same interval-tree overlap search
    # used below instead.
    if !aliasing_obtainable(remote_arg)
        wrapped = key_ainfo isa AliasingWrapper ? key_ainfo : AliasingWrapper(key_ainfo)
        gather_overlap_syncdeps!(state, wrapped, write_num, syncdeps)
        assert_free_syncdeps!(state, wrapped, write_num, syncdeps)
        return
    end

    # Buffer underlies wrapper arguments: find all tracked ainfos overlapping
    # it and sync with their owners/readers.
    buf_ainfo = AliasingWrapper(aliasing(remote_arg))
    gather_overlap_syncdeps!(state, buf_ainfo, write_num, syncdeps)
    assert_free_syncdeps!(state, buf_ainfo, write_num, syncdeps)
    return
end
function add_writer!(state::DataDepsState, arg_w::ArgumentWrapper, dest_space::MemorySpace, ainfo::AbstractAliasing, task, write_num; copy_src::Union{MemorySpace,Nothing}=nothing)
    state.ainfos_owner[ainfo] = task=>write_num
    empty!(state.ainfos_readers[ainfo])

    # Clear the history for this target, since this is a new write event
    empty!(state.arg_history[arg_w])

    # Add our own history
    push!(state.arg_history[arg_w], HistoryEntry(ainfo, dest_space, write_num, task))

    # Find overlapping arguments and update their history
    for other_arg_w in state.arg_overlaps[arg_w]
        other_arg_w == arg_w && continue
        push!(state.arg_history[other_arg_w], HistoryEntry(ainfo, dest_space, write_num, task))
    end

    # Track which spaces hold a fully-current replica of this region
    if copy_src === nothing
        # Task write: only the written space is current, and other spaces'
        # replicas of overlapping regions become stale
        # N.B. The `Set` is reused in place rather than replaced; nothing ever
        # holds a reference to it past the call that reads it (see
        # `distribute_tasks!`'s copy-from elision and `arg_current` reads in
        # `hierarchical.jl`), so mutating is equivalent to rebinding.
        current = get!(Set{MemorySpace}, state.arg_current, arg_w)
        empty!(current)
        push!(current, dest_space)
        for other_arg_w in state.arg_overlaps[arg_w]
            other_arg_w == arg_w && continue
            other_current = get(state.arg_current, other_arg_w, nothing)
            if other_current !== nothing
                intersect!(other_current, (dest_space,))
            else
                # Lazily-tracked args are current only at their origin
                other_origin = state.arg_origin[other_arg_w.arg]
                state.arg_current[other_arg_w] = other_origin == dest_space ?
                    Set{MemorySpace}((dest_space,)) : Set{MemorySpace}()
            end
        end
    else
        # Copy: the destination joins the current set; the source (and any
        # other current replica) stays current, and since a copy only moves
        # already-canonical bytes, overlapping arguments are unaffected
        current = get!(state.arg_current, arg_w) do
            Set{MemorySpace}((state.arg_origin[arg_w.arg],))
        end
        push!(current, dest_space)
    end

    # Record the last place we were fully written to
    state.arg_owner[arg_w] = dest_space

    # Not necessary to assert a read, but conceptually it's true
    add_reader!(state, arg_w, dest_space, ainfo, task, write_num)
end
function add_reader!(state::DataDepsState, arg_w::ArgumentWrapper, dest_space::MemorySpace, ainfo::AbstractAliasing, task, write_num)
    push!(state.ainfos_readers[ainfo], task=>write_num)
end

# Make a copy of each piece of data on each worker
# memory_space => {arg => copy_of_arg}
isremotehandle(x) = false
isremotehandle(x::DTask) = true
isremotehandle(x::Chunk) = true
"""
    slot_rewrap_is_identity(::Type{T}) -> Bool

Whether `move_rewrap` of a `T` that already sits in the destination space would
rebuild a structurally identical value, making the rewrap pure overhead.

`move_rewrap` does more than move bytes: it also *resolves handles*, turning a
`ChunkView` into a `Chunk` over a real `SubArray` and flattening a nested
`Chunk`, so that what reaches the task is a plain destination-space value. A type
containing such a handle therefore always has to go through it. Anything else --
a leaf, or a wrapper (`SubArray`, `HaloArray`, triangular, ...) whose payload is
plain data -- is reproduced unchanged when no space transition is involved.

Only concrete types can be judged: an abstract `chunktype` may be hiding a handle
in a field this cannot see.
"""
function slot_rewrap_is_identity(::Type{T}) where T
    isconcretetype(T) || return false
    isremotehandle_type(T) && return false
    child_types = move_rewrap_child_types(T)
    child_types === nothing && return true
    return all(slot_rewrap_is_identity, child_types)
end

# Handle types, whose resolution is the reason `move_rewrap` cannot be skipped.
isremotehandle_type(::Type{<:Chunk}) = true
isremotehandle_type(::Type{<:DTask}) = true
isremotehandle_type(::Type) = false

"""
    slot_is_already_in_place(data, orig_space, dest_space) -> Bool

Whether `data` can serve as its own Datadeps slot in `dest_space`, so that no
copy of it need be allocated.

Requires a `Chunk` that already lives in `dest_space` and whose rewrap would be
the identity (see `slot_rewrap_is_identity`).

The test is deliberately type-based rather than value-based, which is what lets
it fire for data the planning process cannot touch: under uniform execution (MPI)
every rank plans every task, and under Distributed the chunk may be homed on
another worker. In both cases the alternative is a `move_rewrap` that reproduces,
message by message, data already sitting where it is needed -- for a wrapper like
`HaloArray` that is a broadcast of the header plus one transfer per child, per
argument, per region. `chunktype` is uniform across ranks, so the decision is
too, and the resulting slot is the caller's own `Chunk`, whose handle is uniform
as well.
"""
function slot_is_already_in_place(data, orig_space, dest_space)
    data isa Chunk || return false
    orig_space == dest_space || return false
    return slot_rewrap_is_identity(chunktype(data))
end

function generate_slot!(state::DataDepsState, dest_space, data)
    # N.B. We do not perform any sync/copy with the current owner of the data,
    # because all we want here is to make a copy of some version of the data,
    # even if the data is not up to date.
    orig_space = memory_space(data)
    @check_uniform(orig_space)
    to_proc = first(processors(dest_space))
    @check_uniform(to_proc)
    from_proc = first(processors(orig_space))
    @check_uniform(from_proc)
    @check_uniform(typeof(data))
    dest_space_args = get!(IdDict{Any,Any}, state.remote_args, dest_space)
    aliased_object_cache = AliasedObjectCache(current_acceleration(), dest_space, state.ainfo_backing_chunk)
    # N.B. Same gate as `@maybelog`, written out so the event `id` (a `rand`
    # call, plus the boxing it feeds) is only generated when logging is enabled.
    ctx = Sch.eager_context()
    logging = !(ctx.log_sink isa TimespanLogging.NoOpLog)
    id = logging ? rand(Int) : 0
    logging && timespan_start(ctx, :move, (;thunk_id=0, id, position=ArgPosition(), processor=to_proc), (;f=nothing, data))
    tid = something(DATADEPS_CURRENT_TASK[], (;uid=0)).uid
    t0 = time_ns()
    reused = reusable_slot(data, orig_space, dest_space)
    data_chunk = if reused !== nothing
        # A buffer an earlier region built for exactly this data and space. Route
        # it through `aliased_object!` like the other paths, so the object cache
        # records it as this key's storage in `dest_space` and the rest of
        # planning cannot tell it apart from one made here.
        hier_stat_add!(:slot_reused_ns, time_ns() - t0)
        aliased_object!(Returns(reused), aliased_object_cache, data)::Chunk
    elseif slot_is_already_in_place(data, orig_space, dest_space)
        # Nothing to move: the slot for data already in `dest_space` is the data
        # itself. Going through `move_rewrap` here would allocate a second Chunk
        # (and DRef) over the very same memory, which costs a MemPool round-trip
        # per argument per region and buys nothing. Still route through
        # `aliased_object!` so the cache records this object as the (never-freed)
        # original for its aliasing key, exactly as the general path does.
        aliased_object!(Returns(data), aliased_object_cache, data)::Chunk
    else
        moved = with(DATADEPS_THUNK_ID=>tid) do
            remotecall_endpoint_toplevel(move_rewrap, current_acceleration(), aliased_object_cache, from_proc, to_proc, orig_space, dest_space, data)
        end
        hier_stat_add!(:slot_moved_ns, time_ns() - t0)
        orig_space == dest_space && hier_stat_add!(:slot_samespace_ns, time_ns() - t0)
        moved
    end
    hier_stat_add!(:slot_ns, time_ns() - t0)
    logging && timespan_finish(ctx, :move, (;thunk_id=0, id, position=ArgPosition(), processor=to_proc), (;f=nothing, data=data_chunk))
    @assert memory_space(data_chunk) == dest_space "space mismatch! $dest_space (dest) != $(memory_space(data_chunk)) (actual) ($(typeof(data)) (data) vs. $(typeof(data_chunk)) (chunk)), spaces ($orig_space -> $dest_space)"
    dest_space_args[data] = data_chunk
    state.remote_arg_to_original[data_chunk] = data

    @check_uniform(memory_space(dest_space_args[data]))
    @check_uniform(processor(dest_space_args[data]))
    @check_uniform(dest_space_args[data].handle)

    return dest_space_args[data]
end
function get_or_generate_slot!(state, dest_space, data)
    @assert !(data isa ArgumentWrapper)
    if !haskey(state.remote_args, dest_space)
        state.remote_args[dest_space] = IdDict{Any,Any}()
    end
    if !haskey(state.remote_args[dest_space], data)
        return generate_slot!(state, dest_space, data)
    end
    return state.remote_args[dest_space][data]
end

function is_local(accel::DistributedAcceleration, target)
    return root_worker_id(target) == myid()
end

function remotecall_endpoint_toplevel(f, accel::DistributedAcceleration, cache::AliasedObjectCache, from_proc, to_proc, from_space, to_space, data::Chunk)
    wid = root_worker_id(from_proc)
    if wid == myid()
        return f(accel, cache, from_proc, to_proc, from_space, to_space, unwrap(data))::Chunk
    end
    return remotecall_fetch(wid, f, accel, cache, from_proc, to_proc, from_space, to_space, data) do f, accel, cache, from_proc, to_proc, from_space, to_space, data
        return f(accel, cache, from_proc, to_proc, from_space, to_space, unwrap(data))::Chunk
    end
end
function remotecall_endpoint_transfer(f, accel::DistributedAcceleration, from_proc, to_proc, from_space, to_space, data)
    wid = root_worker_id(to_proc)
    if wid == myid()
        return f(accel, from_proc, to_proc, from_space, to_space, data)
    end
    return remotecall_fetch(wid, f, accel, from_proc, to_proc, from_space, to_space, data) do f, accel, from_proc, to_proc, from_space, to_space, data
        return f(accel, from_proc, to_proc, from_space, to_space, data)
    end
end

#==============================================================================
  move_rewrap header + children protocol

  Wrappers that share underlying storage (SubArray, triangular, ChunkView,
  HaloArray, ...) register:
    move_rewrap_parts(x) -> (children::Tuple, header) | nothing (leaf)
    move_rewrap_build(T, children, header) -> reconstructed value
    move_rewrap_child_types(T) -> Tuple of child types | nothing (leaf)
    move_rewrap_header_mode(T) -> :none | :broadcast | :replicated
    move_rewrap_result_type(T, child_chunktypes, header) -> result DataType

  Distributed and MPI accelerations share one generic move_rewrap that
  transfers/shares children, carries or broadcasts the header, then rebuilds.
==============================================================================#

# Leaf defaults
move_rewrap_parts(x) = nothing
move_rewrap_child_types(::Type) = nothing
move_rewrap_header_mode(::Type) = :none

# SubArray: parent payload + indices header
move_rewrap_parts(x::SubArray) = ((parent(x),), parentindices(x))
move_rewrap_build(::Type{<:SubArray}, (p,), inds) = view(p, inds...)
move_rewrap_child_types(::Type{T}) where {T<:SubArray} = (T.parameters[3],)
move_rewrap_header_mode(::Type{<:SubArray}) = :broadcast
move_rewrap_result_type(::Type{<:SubArray}, (PT,), inds) =
    Base.promote_op(view, PT, typeof.(inds)...)

# Triangular wrappers: parent payload, empty header
for wrapper in (UpperTriangular, LowerTriangular, UnitUpperTriangular, UnitLowerTriangular)
    @eval begin
        move_rewrap_parts(x::$wrapper) = ((parent(x),), nothing)
        move_rewrap_build(::Type{<:$wrapper}, (p,), ::Nothing) = $wrapper(p)
        move_rewrap_child_types(::Type{T}) where {T<:$wrapper} = (T.parameters[2],)
        move_rewrap_result_type(::Type{<:$wrapper}, (PT,), ::Nothing) =
            $wrapper{eltype(PT),PT}
    end
end

# Nested Chunk: unwrap on the owner and recurse
move_rewrap(accel, cache::AliasedObjectCache, from_proc::Processor, to_proc::Processor, from_space::MemorySpace, to_space::MemorySpace, data::Chunk) =
    remotecall_endpoint_toplevel(move_rewrap, accel, cache, from_proc, to_proc, from_space, to_space, data)

function move_rewrap(accel, cache::AliasedObjectCache, from_proc::Processor, to_proc::Processor, from_space::MemorySpace, to_space::MemorySpace, data)
    parts = move_rewrap_parts(data)
    if parts === nothing
        # Leaf: transfer the value, sharing via the aliased-object cache
        return aliased_object!(cache, data) do data
            return remotecall_endpoint_transfer(accel, from_proc, to_proc, from_space, to_space, data) do accel, from_proc, to_proc, from_space, to_space, data
                return tochunk(libc_backed(move(from_proc, to_proc, data)), to_proc, to_space)
            end
        end
    end
    # Wrapper: recurse on children, then rebuild with header on the destination
    children, header = parts
    T = typeof(data)
    child_chunks = map(c -> move_rewrap(accel, cache, from_proc, to_proc, from_space, to_space, c), children)
    for cc in child_chunks
        @check_uniform(cc.handle)
    end
    return remotecall_endpoint_transfer(accel, from_proc, to_proc, from_space, to_space, child_chunks) do accel, from_proc, to_proc, from_space, to_space, child_chunks
        children_local = map(c -> move(from_proc, to_proc, c), child_chunks)
        v_new = move_rewrap_build(T, children_local, header)
        return tochunk(v_new, to_proc, to_space)
    end
end
