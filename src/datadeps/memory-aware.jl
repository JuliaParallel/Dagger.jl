# Memory-aware Datadeps adjustment
#
# Datadeps is a static planner: `distribute_tasks!` records every task up front,
# then a user-selected scheduler (`datadeps_schedule_task`) assigns each task to
# a `MemorySpace`. That assignment determines where Datadeps allocates the
# argument copies ("slots") that feed each task. Nothing, however, bounds the
# *peak* number of bytes simultaneously live in a space, so wide task graphs
# routinely exhaust CPU RAM or GPU VRAM.
#
# This file adds a memory-aware adjustment layer that runs *after* the
# user-selected scheduler has chosen a space for a task (it does not replace the
# scheduler). Working from the static graph, it:
#
#   1. (reassignment) optionally moves a task to a different in-scope space when
#      the scheduler's choice would blow past that space's budget but another
#      space has room;
#   2. (throttling) injects extra syncdeps from a task onto the *last consumers*
#      of already-resident slots, so those slots' frees (which Datadeps already
#      emits, gated on their last consumers) reclaim memory before the task's
#      new slots are allocated. This trades parallelism for a bounded footprint;
#   3. (spilling, Phase 4) when a space's genuinely-required working set cannot
#      fit even after throttling, falls back to spilling cold data: on CPU via
#      MemPool's disk cache, and (best-effort) on GPU by routing the slot back
#      to host memory.
#
# Budgets are a fraction of each space's total capacity (see `memory_capacity`),
# which is predictable and independent of other processes on the machine.
#
# The whole layer is opt-in and off by default; enable it with
# [`enable_memory_aware_scheduling!`](@ref). When disabled, Datadeps behaves
# exactly as before.

"""
    MemoryAwareConfig

Configuration for the memory-aware Datadeps adjustment layer. The active
configuration is the global [`MEMORY_AWARE_CONFIG`](@ref); use
[`enable_memory_aware_scheduling!`](@ref) to mutate it.

Fields:
- `enabled`: whether the layer runs at all.
- `mem_fraction`: per-space budget as a fraction of total capacity (0 < f ≤ 1).
- `limits`: explicit per-space byte budgets that override `mem_fraction`.
- `reassign`: whether tasks may be moved to a less-loaded in-scope space.
- `spill_to_disk`, `disk_limit_mb`: reserved for a future swapping phase.
   Currently unsupported, because relocating data breaks Datadeps' pointer-based
   aliasing tracking (see [`memory_aware_spill!`](@ref)); ignored for now.
- `verbose`: emit `@debug` diagnostics about reassignment and reclamation.
"""
Base.@kwdef mutable struct MemoryAwareConfig
    enabled::Bool = false
    mem_fraction::Float64 = 0.8
    limits::Dict{MemorySpace,UInt64} = Dict{MemorySpace,UInt64}()
    reassign::Bool = true
    spill_to_disk::Bool = false
    disk_limit_mb::Int = 16 * 2^10
    verbose::Bool = false
end

"The global, mutable memory-aware configuration. See [`MemoryAwareConfig`](@ref)."
const MEMORY_AWARE_CONFIG = MemoryAwareConfig()

"""
    enable_memory_aware_scheduling!(; mem_fraction=0.8, reassign=true,
                                      spill_to_disk=true, disk_limit_mb=16*2^10,
                                      limits=Dict{MemorySpace,UInt64}(),
                                      verbose=false)

Enable memory-aware adjustment of Datadeps regions, bounding the live data in
each `MemorySpace` to roughly `mem_fraction` of its total capacity (or to an
explicit byte budget given in `limits`). This is a global setting, analogous to
[`enable_disk_caching!`](@ref); it affects all subsequent `spawn_datadeps`
regions until [`disable_memory_aware_scheduling!`](@ref) is called.

See [`MemoryAwareConfig`](@ref) for the meaning of each option.
"""
function enable_memory_aware_scheduling!(; mem_fraction::Real=0.8,
                                           reassign::Bool=true,
                                           spill_to_disk::Bool=false,
                                           disk_limit_mb::Integer=16 * 2^10,
                                           limits::Dict{MemorySpace,UInt64}=Dict{MemorySpace,UInt64}(),
                                           verbose::Bool=false)
    if !(0 < mem_fraction <= 1)
        throw(ArgumentError("mem_fraction must be in (0, 1], got $mem_fraction"))
    end
    cfg = MEMORY_AWARE_CONFIG
    cfg.enabled = true
    cfg.mem_fraction = Float64(mem_fraction)
    cfg.reassign = reassign
    cfg.spill_to_disk = spill_to_disk
    cfg.disk_limit_mb = Int(disk_limit_mb)
    cfg.limits = limits
    cfg.verbose = verbose
    return cfg
end

"Disable memory-aware adjustment of Datadeps regions (see [`enable_memory_aware_scheduling!`](@ref))."
function disable_memory_aware_scheduling!()
    MEMORY_AWARE_CONFIG.enabled = false
    return nothing
end

"""
    memory_limit(space::MemorySpace, cfg::MemoryAwareConfig=MEMORY_AWARE_CONFIG) -> UInt64

The byte budget for `space`: an explicit override from `cfg.limits` if present,
otherwise `cfg.mem_fraction * memory_capacity(space)`. Returns `typemax(UInt64)`
(i.e. unbounded) for spaces whose capacity cannot be queried, so unsupported
spaces degrade gracefully instead of erroring.
"""
function memory_limit(space::MemorySpace, cfg::MemoryAwareConfig=MEMORY_AWARE_CONFIG)
    haskey(cfg.limits, space) && return cfg.limits[space]
    cap = try
        memory_capacity(space)
    catch err
        @debug "Could not query capacity for $space; treating as unbounded" exception=err
        return typemax(UInt64)
    end
    return floor(UInt64, cfg.mem_fraction * cap)
end

# Per-slot info from the scheduler-independent pre-pass. `key` identifies the
# argument object (`objectid`, matching the `IdDict` keys Datadeps uses for its
# per-space slot maps, so the same datum maps to one slot); `size` is its byte
# size; `origin` is the space where the user's data already lives (a use there
# is the original, not a Datadeps-allocated copy).
struct DatadepsSlotInfo
    key::UInt64
    size::UInt64
    origin::MemorySpace
end

"""
    DatadepsMemoryTracker

Mutable accounting state for a memory-aware Datadeps region. Datadeps allocates
each argument copy ("slot") *eagerly and synchronously* in `get_or_generate_slot!`,
so the budget is enforced there: before allocating a copy that would exceed a
space's budget, [`memory_aware_reserve!`](@ref) synchronously frees dead copies
(read-only buffers whose last use has passed), reusing the same `unsafe_free!`
path as the end-of-region cleanup.

Only Datadeps-allocated *copies* are tracked/bounded; the user's original data
is out of our control and never freed here. Written copies are never reclaimed
mid-region (their results must be preserved); they are freed at region end after
any write-back.
"""
mutable struct DatadepsMemoryTracker
    cfg::MemoryAwareConfig
    # Per-task slot lists, aligned with `queue.seen_tasks` (1-based index). Used
    # by reassignment to estimate per-space footprints.
    task_slots::Vector{Vector{DatadepsSlotInfo}}
    # Global last task index that uses each slot key (reclaim allowed after).
    last_use::Dict{UInt64,Int}
    # Slot keys that are written somewhere in the region (never mid-reclaimed).
    written::Set{UInt64}
    # Cached budgets per space.
    limits::Dict{MemorySpace,UInt64}
    # Current live copy bytes per space.
    live::Dict{MemorySpace,UInt64}
    # Maps an in-loop slot source object (e.g. a `tochunk`-generated `Chunk`) to
    # the pre-pass key of its original argument, so `last_use` lookups line up.
    # Populated by `populate_task_info!`.
    orig_key::IdDict{Any,UInt64}
    # Resident copies: space -> key -> (slot chunk, size).
    resident::Dict{MemorySpace,Dict{UInt64,Tuple{Any,UInt64}}}
    # Buffers already freed (shared with the region-end cleanup to avoid double frees).
    freed::IdDict{Any,Nothing}
    # Spaces for which disk spilling has been enabled.
    spilled::Set{MemorySpace}
    # Index of the task currently being distributed (defines what is "dead").
    current_idx::Base.RefValue{Int}
    # Whether mid-region reclaim is permitted (false outside the main loop, so
    # region-end source slots for write-back copies are never reclaimed).
    active::Base.RefValue{Bool}
end

function DatadepsMemoryTracker(cfg::MemoryAwareConfig,
                               task_slots::Vector{Vector{DatadepsSlotInfo}},
                               written::Set{UInt64})
    last_use = Dict{UInt64,Int}()
    for (i, slots) in enumerate(task_slots)
        for slot in slots
            last_use[slot.key] = i  # processed in order, so this ends at the max
        end
    end
    return DatadepsMemoryTracker(cfg, task_slots, last_use, written,
                                 Dict{MemorySpace,UInt64}(),
                                 Dict{MemorySpace,UInt64}(),
                                 IdDict{Any,UInt64}(),
                                 Dict{MemorySpace,Dict{UInt64,Tuple{Any,UInt64}}}(),
                                 IdDict{Any,Nothing}(),
                                 Set{MemorySpace}(),
                                 Base.RefValue{Int}(0),
                                 Base.RefValue{Bool}(false))
end

tracker_limit(tracker::DatadepsMemoryTracker, space::MemorySpace) =
    get!(() -> memory_limit(space, tracker.cfg), tracker.limits, space)
tracker_live(tracker::DatadepsMemoryTracker, space::MemorySpace) =
    get(tracker.live, space, UInt64(0))

# Bytes that scheduling task `i` onto `space` would newly allocate as copies:
# the sum of its slots not already resident in `space`, excluding those whose
# origin is `space` (the user's own data, allocated nothing).
function incremental_bytes(tracker::DatadepsMemoryTracker, space::MemorySpace, i::Int)
    resident = get(tracker.resident, space, nothing)
    total = UInt64(0)
    for slot in tracker.task_slots[i]
        slot.origin == space && continue
        (resident !== nothing && haskey(resident, slot.key)) && continue
        total += slot.size
    end
    return total
end

"""
    memory_aware_reassign!(tracker, our_proc, candidate_procs, i) -> Processor

Given the scheduler's chosen `our_proc` for task `i`, optionally pick a
different in-scope processor whose memory space currently has more headroom (so
fewer copies pile up there). Returns `our_proc` unchanged when reassignment is
disabled, when the chosen space already fits, or when no candidate is better.
"""
function memory_aware_reassign!(tracker::DatadepsMemoryTracker, our_proc::Processor,
                                candidate_procs::Vector{Processor}, i::Int)
    tracker.cfg.reassign || return our_proc
    our_space = only(memory_spaces(our_proc))
    our_limit = tracker_limit(tracker, our_space)
    if tracker_live(tracker, our_space) + incremental_bytes(tracker, our_space, i) <= our_limit
        return our_proc
    end

    best_proc = our_proc
    best_new = incremental_bytes(tracker, our_space, i)
    best_room = our_limit > tracker_live(tracker, our_space) ?
        our_limit - tracker_live(tracker, our_space) : UInt64(0)
    best_fits = false
    for proc in candidate_procs
        space = only(memory_spaces(proc))
        limit = tracker_limit(tracker, space)
        live = tracker_live(tracker, space)
        new_bytes = incremental_bytes(tracker, space, i)
        fits = live + new_bytes <= limit
        room = limit > live ? limit - live : UInt64(0)
        better = if fits != best_fits
            fits
        elseif new_bytes != best_new
            new_bytes < best_new
        else
            room > best_room
        end
        if better
            best_proc, best_new, best_room, best_fits = proc, new_bytes, room, fits
        end
    end
    if best_proc !== our_proc && tracker.cfg.verbose
        @debug "Memory-aware: reassigned datadeps task $i from $our_space to $(only(memory_spaces(best_proc)))"
    end
    return best_proc
end

# Wait for a (prior) task to finish, if it has been launched. Reader/owner tasks
# of a dead slot were distributed earlier in the loop, so they are launched and
# will complete; this blocks planning until the data is safe to free.
function _wait_started(t::DTask)
    istaskstarted(t) && wait(t)
    return
end

# Collect the tracked ainfos backed by `chunk` (a slot), without mutating the
# aliasing lookup. We use this (rather than `gather_free_syncdeps!`'s fallback,
# which `push!`es into the lookup) because mid-region we must not perturb the
# overlap oracle that subsequent tasks rely on.
function ainfos_for_chunk(state::DataDepsState, chunk)
    ainfos = AliasingWrapper[]
    for (ainfo, arg_ws) in state.ainfo_arg
        for aw in arg_ws
            if aw.arg === chunk
                push!(ainfos, ainfo)
                break
            end
        end
    end
    return ainfos
end

"""
    memory_aware_reserve!(tracker, state, space, data)

Called just before `generate_slot!` allocates a copy of `data` in `space`. If
that allocation would exceed `space`'s budget, synchronously reclaim dead
read-only copies until it fits (or nothing more can be freed, in which case it
spills). A copy is dead when its last use has passed (`last_use < current_idx`)
and it was never written. Reclaiming waits on the buffer's existing reader/owner
tasks, frees it in place via `unsafe_free!(::Chunk)` (a synchronous
`remotecall_fetch`), and fully retracts the slot from the planner state (see
below), so the memory is reclaimed before the new allocation proceeds. No-op for
originals (origin == `space`).

We must not *spawn* a free task and wait on it here: spawning + waiting a new
thunk from inside the planning loop trips a scheduler invariant. And because the
planner state is otherwise append-only (interval-tree overlap oracle, per-arg
histories, backing cache), a freed buffer's address may be reused by a later
allocation; a stale ainfo left behind would then collide with the new
allocation's spans. The reclaim therefore retracts the freed slot completely
(`forget_ainfo!` plus origin/history fixups) -- safe because we only reclaim
read-only copies that overlap nothing else live.
"""
function memory_aware_reserve!(tracker::DatadepsMemoryTracker, state::DataDepsState,
                               space::MemorySpace, data)
    tracker.active[] || return
    memory_space(data) == space && return  # original, not our allocation
    limit = tracker_limit(tracker, space)
    limit == typemax(UInt64) && return
    size = try
        data_size(data)
    catch
        UInt64(0)
    end
    live = tracker_live(tracker, space)
    live + size <= limit && return

    resident = get(tracker.resident, space, nothing)
    if resident !== nothing
        idx = tracker.current_idx[]
        # Soonest-dead-first so we keep the data needed soonest.
        freeable = Tuple{Int,UInt64}[]
        for (key, _) in resident
            (key in tracker.written) && continue
            lu = get(tracker.last_use, key, typemax(Int))
            lu < idx && push!(freeable, (lu, key))
        end
        sort!(freeable; by=first)
        for (_, key) in freeable
            (live + size <= limit) && break
            chunk, sz = resident[key]
            ainfos = ainfos_for_chunk(state, chunk)
            isempty(ainfos) && continue  # can't safely bound its readers; skip
            # Aliasing-safety guard: only reclaim a copy whose memory overlaps
            # nothing else tracked. Otherwise its backing buffer may be shared
            # (e.g. a parent array still aliased by a not-yet-dead view), and
            # freeing it would corrupt those. Such copies are left to the
            # (safe) end-of-region cleanup. Distinct tiles overlap nothing, so
            # this admits the common case while staying correct under aliasing.
            own = Set{AliasingWrapper}(ainfos)
            overlaps_other = any(ainfos) do a
                any(o -> !(o in own), state.ainfos_overlaps[a])
            end
            overlaps_other && continue
            haskey(tracker.freed, chunk) && (delete!(resident, key); continue)

            # Free synchronously, in place. We must NOT spawn a free *thunk* and
            # wait on it here: spawning + waiting a new task from inside the
            # planning loop trips a scheduler invariant (`load_result` asserts
            # the thunk is finished). Instead we wait on the slot's existing
            # reader/owner tasks (the same kind of wait `populate_task_info!`
            # already performs on `DTask` arguments) and then call
            # `unsafe_free!(::Chunk)`, which is itself a synchronous
            # `remotecall_fetch`. The aliasing guard above guarantees these
            # ainfos cover every task touching this buffer.
            for a in ainfos
                owner = get(state.ainfos_owner, a, nothing)
                owner === nothing || _wait_started(owner[1])
                for reader in get(state.ainfos_readers, a, ())
                    _wait_started(reader[1])
                end
            end
            tracker.freed[chunk] = nothing
            unsafe_free!(chunk)
            # Fully retract the freed slot from the planner's append-only state.
            # The slot is read-only (we never reclaim written keys), so its
            # origin data is still authoritative and no write-back is ever owed.
            # Dependency state, however, keys on the *original* argument with the
            # slot's ainfo (see `add_writer!`), and the region-end write-back pass
            # walks that history -- a dangling reference to the freed ainfo would
            # `KeyError` there. So, for each freed ainfo we:
            #   * drop the slot wrappers' own `arg_owner`/`arg_history`;
            #   * strip the freed ainfo from the original argument's history and
            #     reset its owner to the origin space (read-only => origin holds
            #     the current value), so write-back resolves to a no-op; and
            #   * forget the ainfo from the overlap oracle, so a later allocation
            #     reusing this address is not aliased against the dead slot.
            for a in ainfos
                for slot_arg_w in get(state.ainfo_arg, a, ())
                    delete!(state.arg_owner, slot_arg_w)
                    delete!(state.arg_history, slot_arg_w)
                    orig = get(state.remote_arg_to_original, slot_arg_w.arg, nothing)
                    orig === nothing && continue
                    orig_w = ArgumentWrapper(orig, slot_arg_w.dep_mod)
                    hist = get(state.arg_history, orig_w, nothing)
                    hist !== nothing && filter!(e -> e.ainfo != a, hist)
                    haskey(state.arg_origin, orig) && (state.arg_owner[orig_w] = state.arg_origin[orig])
                end
                forget_ainfo!(state, a)
            end
            live -= min(live, sz)
            delete!(resident, key)
            tracker.live[space] = live
            if tracker.cfg.verbose
                @debug "Memory-aware: reclaimed $(sz) bytes in $space (task $idx)"
            end
        end
    end

    if live + size > limit
        memory_aware_spill!(tracker, space, tracker.current_idx[])
    end
    return
end

"""
    memory_aware_record!(tracker, space, data, chunk)

Record that `chunk` (a freshly-allocated copy of `data`) now occupies `space`,
adding its size to the live total and (unless it is the user's original) making
it eligible for later reclamation.
"""
function memory_aware_record!(tracker::DatadepsMemoryTracker, space::MemorySpace, data, chunk)
    memory_space(data) == space && return  # original, untracked
    key = get(tracker.orig_key, data, objectid(data))
    size = try
        data_size(data)
    catch
        UInt64(0)
    end
    resident = get!(() -> Dict{UInt64,Tuple{Any,UInt64}}(), tracker.resident, space)
    haskey(resident, key) && return
    resident[key] = (chunk, size)
    tracker.live[space] = tracker_live(tracker, space) + size
    return
end

"""
    memory_aware_spill!(tracker, space, i)

Fallback when a space's required working set exceeds its budget and cannot be
reduced by reclaiming dead copies. We only *warn* here.

Phase 4 swapping (spilling cold data to disk, or evicting GPU data to host) is
deliberately NOT performed: Datadeps' aliasing oracle tracks raw memory spans
(pointers), and any swap that relocates a buffer (MemPool disk spill + re-read,
or a GPU↔CPU round-trip) moves it to a new address, invalidating those spans and
corrupting dependency analysis (it trips the `span_diff` assertion in
`compute_remainder_for_arg!`). Safe swapping therefore requires the aliasing
tracking to be updated on relocation, which is out of scope for this change.

Until then, bound memory by throttling/reclaiming (which keeps data in place) and
by choosing a `mem_fraction`/`limits` at or above the irreducible working set.
"""
function memory_aware_spill!(tracker::DatadepsMemoryTracker, space::MemorySpace, i::Int)
    space in tracker.spilled && return
    push!(tracker.spilled, space)
    @warn "Memory-aware: working set for $space exceeds its budget and cannot be reduced by reclaiming dead copies; proceeding without swapping (risk of OOM). Consider raising the budget. Relocating swaps (disk/GPU↔CPU) are unsafe with Datadeps' pointer-based aliasing and are not performed." maxlog=1
    return
end

# --- Scheduler-independent pre-pass -----------------------------------------

# Resolve one (already `unwrap_inout`'d) argument to a slot descriptor, WITHOUT
# blocking. This runs during planning, before tasks execute, so we must never
# `fetch` an in-region `DTask` (that would block until it completes and can
# deadlock against the very loop that launches it). We therefore size/identify
# args from data that is available immediately:
#
#   - `Chunk`s carry their backing `DRef` (size + owner) directly.
#   - `ChunkView`s are keyed by their parent chunk (views share the parent slot,
#     mirroring the parent-sharing in the aliased object cache).
#   - `DTask`s are keyed by object identity; sized only if already completed
#     (a non-blocking `istaskdone` check), else treated as 0 bytes. Chained
#     `DTask`-result arguments are thus not sized -- a known limitation; the
#     dominant case (operating on `DArray`/`Chunk` tiles) is sized exactly.
#   - other data falls back to `sizeof`/`summarysize` and object identity.
#
# Only arguments whose (resolved) type may alias get a slot; non-aliasing args
# (scalars, functions) never allocate a tracked copy. Returns `nothing` to skip.
# The slot key must match the `IdDict` key that `get_or_generate_slot!` uses,
# which is the argument object Datadeps actually passes there. For the common,
# important case (a `Chunk`, e.g. a `DArray` tile) that object is identical here
# and in the emit loop, so `objectid` agrees. For args that the emit loop
# re-wraps (raw arrays -> `tochunk`, some views) or for `DTask` results (sized
# only once complete), the keys may differ; that only makes those slots
# un-reclaimable (a safe loss of effectiveness), never incorrect.
function datadeps_slot_descriptor(arg)
    if arg isa Chunk
        type_may_alias(arg.chunktype) || return nothing
        sz = arg.handle isa Union{DRef,FileRef} ? UInt64(arg.handle.size) : UInt64(0)
        return DatadepsSlotInfo(objectid(arg), sz, memory_space(arg))
    elseif arg isa ChunkView
        c = arg.chunk
        type_may_alias(c.chunktype) || return nothing
        sz = c.handle isa Union{DRef,FileRef} ? UInt64(c.handle.size) : UInt64(0)
        return DatadepsSlotInfo(objectid(arg), sz, memory_space(c))
    elseif arg isa DTask
        rt = isdefined(arg, :metadata) ? arg.metadata.return_type : Any
        type_may_alias(rt) || return nothing
        sz = UInt64(0)
        origin = CPURAMMemorySpace(myid())
        if istaskdone(arg)
            try
                c = fetch(arg; raw=true)
                sz = data_size(c)
                origin = memory_space(c)
            catch
            end
        end
        return DatadepsSlotInfo(objectid(arg), sz, origin)
    else
        type_may_alias(typeof(arg)) || return nothing
        sz = try
            data_size(arg)
        catch
            UInt64(0)
        end
        key = ismutable(arg) ? objectid(arg) : hash(arg)
        return DatadepsSlotInfo(UInt64(key), sz, memory_space(arg))
    end
end

# Compute the tracked-slot list and the set of written keys for one task's spec
# (non-blocking; see `datadeps_slot_descriptor`). A key is "written" if any of
# its dependencies is a write (`Out`/`InOut`).
function datadeps_task_slots!(written::Set{UInt64}, spec::DTaskSpec)
    slots = DatadepsSlotInfo[]
    fargs = spec.fargs
    n = length(fargs)
    for idx in 1:n
        arg_with_deps = value(fargs[idx])
        arg, deps = unwrap_inout(arg_with_deps)
        slot = datadeps_slot_descriptor(arg)
        slot === nothing && continue
        push!(slots, slot)
        if any(d -> d[3], deps)
            push!(written, slot.key)
        end
    end
    return slots
end

# Build the tracker for an entire region by running the pre-pass over every task.
function build_memory_tracker(cfg::MemoryAwareConfig, seen_tasks)
    task_slots = Vector{Vector{DatadepsSlotInfo}}(undef, length(seen_tasks))
    written = Set{UInt64}()
    for (i, pair) in enumerate(seen_tasks)
        task_slots[i] = datadeps_task_slots!(written, pair.spec)
    end
    return DatadepsMemoryTracker(cfg, task_slots, written)
end
