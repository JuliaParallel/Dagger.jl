### Cross-region reuse of Datadeps slots ###
#
# A "slot" is the per-memory-space buffer Datadeps allocates to stand in for an
# argument that a task will touch from somewhere other than where the argument
# lives. Slots are created by `generate_slot!` and, until now, thrown away at the
# end of every region.
#
# That is expensive for iterative code -- a stencil loop, an iterative solver --
# which runs the *same* region over the *same* arguments many times, and so pays
# per buffer, per sweep: an allocation, a `move_rewrap` that transfers the whole
# buffer, an aliasing exchange to register it, and a free task. The transfer is
# the dominant term, and it is pure waste: `generate_slot!` deliberately does not
# synchronize with the argument's owner (a slot is "some version" of the data),
# and the region's own copy-to brings the buffer up to date before anything reads
# it. Under MPI the transfer is worse than its bytes suggest, because it is a
# rendezvous every rank has to reach, in the middle of planning.
#
# Reuse removes all four costs from the second sweep onwards. Its one obligation
# is the one thing a fresh slot did provide for free: a fresh slot holds a
# snapshot of the whole origin buffer, so any part of it the region's copies do
# *not* refresh still reads as the origin's data. A reused slot instead holds
# whatever the previous region left. `slot_reuse_eligible_args` is what closes
# that gap -- see it for the exact condition.

"""
Whether Datadeps may reuse a slot buffer allocated by an earlier region instead
of allocating and populating a fresh one. See the top of this file.

Turning this off costs performance for iterative workloads but is never
incorrect, so it is a usable escape hatch if a workload's slot lifetimes are
surprising. Also settable at startup with `JULIA_DAGGER_SLOT_REUSE=0`.
"""
const DATADEPS_SLOT_REUSE = Ref(true)

"""
How many slots may be retained across regions.

Retained slots are memory held on the chance that the next region wants them, so
this is a cache size, not a correctness bound: exceeding it only means the oldest
entries are dropped and their regions pay full price again. It is a count rather
than a byte budget because a `Chunk`'s footprint is not knowable from the
planning process (the buffer lives on another worker or rank).

Eviction is first-in-first-out. For the iterative workloads this cache exists for
that is the same thing as least-recently-used -- every sweep touches the whole
working set -- and unlike LRU it does not reorder on a hit, which keeps the
eviction sequence identical on every rank under SPMD planning.
"""
const DATADEPS_SLOT_CACHE_LIMIT = Ref(1024)

const SlotCacheKey = Tuple{UInt,MemorySpace}

mutable struct SlotCacheEntry
    const slot::Chunk
    # Weak so that retaining a slot never keeps the user's own data alive. It is
    # only ever read to reject a key collision; see `slot_cache_take!`.
    const origin::WeakRef
    const origin_type::Type
    # Whether a region currently has this slot in hand. Regions can run
    # concurrently from different Julia tasks, and two of them must not be handed
    # the same buffer.
    inuse::Bool
end

struct SlotCacheStore
    entries::Dict{SlotCacheKey,SlotCacheEntry}
    # Keys in insertion order, oldest first, for FIFO eviction.
    order::Vector{SlotCacheKey}
end
SlotCacheStore() = SlotCacheStore(Dict{SlotCacheKey,SlotCacheEntry}(), SlotCacheKey[])

const SLOT_CACHE = LockedObject(SlotCacheStore())

"""
    SlotReuseRegion

One region's participation in the slot cache.

Holds which arguments the region may reuse slots for (`eligible`, as identity
hashes), which cache entries it has taken (`checked_out`, released once the
region's tasks have finished), and which buffers the epilogue must therefore
leave alone rather than free (`retained`, as `Chunk` handles).

`retained` holds handles rather than the `Chunk`s themselves because the two
sides do not agree on the wrapper: a slot is retained as the `Chunk` recorded in
`state.remote_args`, and freed as the `Chunk` recorded in the aliased-object
cache, and those can be distinct objects over one buffer. What must not be freed
is the buffer.
"""
struct SlotReuseRegion
    eligible::Set{UInt}
    checked_out::Vector{SlotCacheKey}
    retained::Set{Any}
    lock::ReentrantLock
end
SlotReuseRegion(eligible::Set{UInt}) =
    SlotReuseRegion(eligible, SlotCacheKey[], Set{Any}(), ReentrantLock())

const SLOT_REUSE_REGION = ScopedValue{Union{SlotReuseRegion,Nothing}}(nothing)

"""
    slot_reuse_eligible_args(arg_ws) -> Set{UInt}

The identity hashes of the arguments in `arg_ws` whose slots this region may
reuse: those used only with `dep_mod === identity`.

This is the condition that makes a stale slot safe. Datadeps refreshes a slot
with `move!(dep_mod, ...)`, which writes only the part of the buffer that
`dep_mod` selects; the rest is left as the slot was created. That is fine for a
fresh slot, whose creating transfer filled the whole buffer with the origin's
contents, and not fine for a reused one, which still holds the previous region's
bytes there.

An argument used only under `identity` has no such gap: the first copy into its
slot is a whole-buffer copy (`compute_remainder_for_arg!` returns `FullCopy` on
first touch, since the argument has no history yet and its owner is elsewhere),
so nothing a task can see survives from the previous region. An argument used
under any partial modifier -- `Diagonal`, `UpperTriangular`, the blocked
factorizations -- is excluded wholesale, including its `identity` uses, since
they all share the one slot.

The result is keyed by `_identity_hash` rather than by object identity because
the same argument reaches slot generation as a re-derived `Chunk`, and because
these hashes are what the cache itself is keyed on.
"""
function slot_reuse_eligible_args(arg_ws)
    eligible = Set{UInt}()
    partial = Set{UInt}()
    for arg_w in arg_ws
        h = _identity_hash(arg_w.arg)
        push!(arg_w.dep_mod === identity ? eligible : partial, h)
    end
    setdiff!(eligible, partial)
    return eligible
end

"""
    slot_reusable(region, data, orig_space, dest_space) -> Bool

Whether a slot for `data` in `dest_space` is a candidate for cross-region reuse.

Beyond the region's own eligibility rule, two structural requirements:

* The slot must be a *leaf* buffer (`move_rewrap_child_types` finds no children).
  A wrapper's slot is rebuilt over separately-registered child buffers, so
  retaining the wrapper while the epilogue frees a child would leave a slot
  pointing at freed memory. Leaves are one buffer, registered once, and can be
  retained as a unit.
* The slot must actually be a copy (`orig_space != dest_space`). A same-space
  slot is either the argument itself or a copy that nothing refreshes -- it is
  read as a *source*, and its contents are load-bearing.
"""
function slot_reusable(region::SlotReuseRegion, data, orig_space, dest_space)
    DATADEPS_SLOT_REUSE[] || return false
    data isa Chunk || return false
    orig_space == dest_space && return false
    _identity_hash(data) in region.eligible || return false
    T = chunktype(data)
    isconcretetype(T) || return false
    return move_rewrap_child_types(T) === nothing
end

"""
    reusable_slot(data, orig_space, dest_space) -> Union{Chunk,Nothing}

The slot retained by an earlier region for `data` in `dest_space`, or `nothing`
if there is none to reuse.

Every input to this decision is rank-uniform under SPMD planning -- the identity
hash, the chunk type, the eligibility set (derived from the region's own uniform
argument list), and the cache contents (which evolve identically because every
rank replays the same regions in the same order) -- so all ranks hit and miss
together, which they must: a hit skips a transfer that is a rendezvous.
"""
function reusable_slot(data, orig_space, dest_space)
    region = SLOT_REUSE_REGION[]
    region === nothing && return nothing
    slot_reusable(region, data, orig_space, dest_space) || return nothing
    slot = slot_cache_take!(region, data, dest_space)
    check_uniform(slot !== nothing)
    return slot
end

function slot_cache_take!(region::SlotReuseRegion, data::Chunk, dest_space::MemorySpace)
    key = (_identity_hash(data), dest_space)
    return lock(SLOT_CACHE) do cache
        entry = get(cache.entries, key, nothing)
        entry === nothing && return nothing
        entry.inuse && return nothing
        # A stale or colliding entry: the hash names a chunk that is either gone
        # or was never this one. Unreachable short of a 64-bit hash collision on
        # a never-recycled handle id, but the alternative to checking is handing
        # back a buffer of the wrong shape.
        if entry.origin.value !== data.handle || entry.origin_type !== chunktype(data)
            slot_cache_delete!(cache, key)
            return nothing
        end
        entry.inuse = true
        @lock region.lock push!(region.checked_out, key)
        return entry.slot
    end
end

"""
    retain_slot!(region, data, dest_space, slot)

Keep `slot` for the next region that wants a slot for `data` in `dest_space`, and
record that this region's epilogue must not free it.
"""
function retain_slot!(region::SlotReuseRegion, data::Chunk, dest_space::MemorySpace, slot::Chunk)
    key = (_identity_hash(data), dest_space)
    lock(SLOT_CACHE) do cache
        existing = get(cache.entries, key, nothing)
        if existing !== nothing
            existing.slot.handle == slot.handle && return
            # The region ended up using a different buffer than the one we had
            # (the object cache dedups by aliasing, so an overlapping argument
            # may have supplied one first). Keep the one that is real.
            slot_cache_delete!(cache, key)
        end
        while !isempty(cache.order) && length(cache.order) >= DATADEPS_SLOT_CACHE_LIMIT[]
            evicted = popfirst!(cache.order)
            # Dropping the reference is all the freeing we do: an `unsafe_free!`
            # here would race whatever region still holds the buffer, and the
            # storage is reclaimed by MemPool refcounting / the array's finalizer
            # once nothing references it.
            delete!(cache.entries, evicted)
        end
        cache.entries[key] = SlotCacheEntry(slot, WeakRef(data.handle), chunktype(data), true)
        push!(cache.order, key)
        @lock region.lock push!(region.checked_out, key)
        return
    end
    @lock region.lock push!(region.retained, slot.handle)
    return
end

function slot_cache_delete!(cache::SlotCacheStore, key::SlotCacheKey)
    delete!(cache.entries, key)
    idx = findfirst(==(key), cache.order)
    idx === nothing || deleteat!(cache.order, idx)
    return
end

"""
    retain_reusable_slots!(state)

Hand every reusable slot this region built to the cache, before the epilogue
decides what to free.
"""
function retain_reusable_slots!(state::DataDepsState)
    region = SLOT_REUSE_REGION[]
    region === nothing && return
    # Sorted so that, when the working set is larger than the cache, every rank
    # retains and evicts in the same order (`remote_args` is an `IdDict`, whose
    # iteration order is address-dependent and so rank-local).
    spaces = sort!(collect(keys(state.remote_args)); by=short_name)
    for space in spaces
        for (data, slot) in state.remote_args[space]
            slot === data && continue
            slot_reusable(region, data, memory_space(data), space) || continue
            retain_slot!(region, data, space, slot)
        end
    end
    return
end

"Whether the region's epilogue must leave `slot`'s buffer alone rather than free it."
function slot_is_retained(slot)
    region = SLOT_REUSE_REGION[]
    region === nothing && return false
    slot isa Chunk || return false
    return @lock region.lock slot.handle in region.retained
end

"""
    release_slot_reuse_region!(region)

Give the region's cache entries back, once its tasks have finished and its slots
are quiescent. Called after the region's `wait_all`, not after planning: the
copy and free tasks that touch a slot outlive the planner.
"""
function release_slot_reuse_region!(region::SlotReuseRegion)
    keys_held = @lock region.lock copy(region.checked_out)
    lock(SLOT_CACHE) do cache
        for key in keys_held
            entry = get(cache.entries, key, nothing)
            entry === nothing && continue
            entry.inuse = false
        end
    end
    return
end

"Drop every retained slot. Exposed for tests and for reclaiming the memory."
function empty_slot_cache!()
    lock(SLOT_CACHE) do cache
        empty!(cache.entries)
        empty!(cache.order)
    end
    return
end
