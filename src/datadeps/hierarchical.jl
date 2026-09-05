# Hierarchical scheduling for datadeps
# Spreads scheduling work across multiple threads/workers/MPI ranks via a
# 4-phase pipeline:
# Phase 1: Parallel aliasing info construction
# Phase 2: Sequential DAG construction from aliasing overlaps
# Phase 3: Data-affinity DAG partitioning (by Distributed worker or MPI rank
#          via `partition_affinity_id`)
# Phase 4: Per-partition scheduling -- under Distributed each partition runs on
#          its own task concurrently; under MPI (uniform execution) partitions
#          are scheduled sequentially in global topological order for SPMD
#          safety -- as are all multi-owner regions (see note 2 below). Each
#          partition assigns processors with its own scheduler shard, restricted
#          to just its processors.
#
# N.B. Concurrent task submission from many partition tasks used to intermittently
# hang in the core eager scheduler. The root cause was `Distributed.Future`,
# which is unsafe under concurrent same-process access; Dagger now backs task
# futures with `MemPool.DFuture` (a thread-safe, `DEvent`-based future), so
# parallel Phase 4 is safe. The actual task submission (`enqueue!`) is still
# serialized via `LockedEnqueueQueue`, but the (expensive) per-task
# `distribute_task!` preparation runs concurrently across partitions.
#
# ### Not-yet-parallelized work (performance only; results are unaffected)
#
# These are the known gaps between what this pipeline does and what it could do.
# Each is marked with a `# PERF(hier-N)` comment at the relevant site.
#
# 1. MPI plans entirely sequentially. Uniform execution needs rank-identical
#    ordering for tag / `MPIRefID` allocation, and aliasing may run collectives
#    that must be ordered identically everywhere, so Phase 1 uses `nchunks == 1`,
#    `_compute_aliasing_batch` refuses to thread, and Phase 4 uses
#    `schedule_partitions_sequential!`. Under MPI this pipeline therefore buys
#    rank affinity but *not* parallel planning. Lifting this needs a
#    deterministic parallel order (e.g. pre-allocating tag ranges per partition)
#    rather than simply enabling the threaded paths.
# 2. Multi-owner regions plan Phase 4 sequentially, not just MPI ones. Partitions
#    still carry worker/rank affinity, but they are planned in global topological
#    order on one shared `DataDepsState` (`use_shared_state` below). The parallel
#    per-partition path is correct only when every partition shares one memory
#    space, because `DataDepsState` keys its slot / ownership / currency
#    bookkeeping by memory *space* and cannot represent two partitions' distinct
#    slots for one chunk. Fixing that -- tracking slot identity rather than space
#    -- is what would re-enable parallel planning across workers, and is the
#    single highest-value follow-up here.
# 3. Planning is centralized on the calling process. Phase 1's aliasing is
#    genuinely distributed (`remotecall` per worker), but Phase 4 runs every
#    partition's `distribute_task!` locally. Workers never plan their own
#    partitions, so planning does not scale out with the cluster.
# 4. Phases 2 and 3 are single-threaded. Both are incremental and
#    order-dependent (interval-tree insertion; sequential owner/reader state),
#    so they resist naive parallelization. Cheap relative to Phases 1 and 4
#    today, but they become the ceiling once those scale.

struct HierarchicalTaskInfo
    arg_w::ArgumentWrapper
    readdep::Bool
    writedep::Bool
end

"""
    BatchedEnqueueQueue(inner, lock; limit=DATADEPS_BATCH_LIMIT[])

Buffers tasks locally and submits them to `inner` in batches of at most `limit`.

Submitting one task at a time costs a scheduler round-trip each; batching pays
that once per group. It also collapses the lock traffic, which matters because
every partition of the hierarchical path shares a single lock over the region's
queue -- with one acquisition per task the partitions spend most of their time
convoyed behind each other rather than preparing tasks in parallel.

`limit` trades that amortization against latency: buffered tasks cannot start
executing, so an unbounded batch would keep a partition's whole workload idle
until it finished planning, serializing planning against execution. A small
limit keeps the pipeline fed while still amortizing most of the per-submission
cost.

`flush_batch!` must additionally be called before anything outside this
partition can observe the buffered tasks (see `schedule_partition_full!`), since
a task's syncdeps must already be submitted when a dependent task is prepared.
"""
struct BatchedEnqueueQueue <: AbstractTaskQueue
    inner::AbstractTaskQueue
    lock::ReentrantLock
    pending::Vector{DTaskPair}
    limit::Int
end
BatchedEnqueueQueue(inner::AbstractTaskQueue, lock::ReentrantLock;
                    limit::Int=DATADEPS_BATCH_LIMIT[]) =
    BatchedEnqueueQueue(inner, lock, DTaskPair[], limit)
function enqueue!(beq::BatchedEnqueueQueue, pair::DTaskPair)
    push!(beq.pending, pair)
    length(beq.pending) >= beq.limit && flush_batch!(beq)
    return
end
function enqueue!(beq::BatchedEnqueueQueue, pairs::Vector{DTaskPair})
    append!(beq.pending, pairs)
    length(beq.pending) >= beq.limit && flush_batch!(beq)
    return
end
function flush_batch!(beq::BatchedEnqueueQueue)
    isempty(beq.pending) && return
    @lock beq.lock enqueue!(beq.inner, beq.pending)
    empty!(beq.pending)
    return
end

"Maximum number of tasks a hierarchical partition buffers before submitting."
const DATADEPS_BATCH_LIMIT = Ref(4)

struct HierarchicalTaskMeta
    pair::DTaskPair
    # Wrapped argument identity for the task's first aliasing arg. Under MPI
    # this may be a raw `ChunkView` (kept unwrapped by `datadeps_arg_wrap`).
    arg_chunk::Any
    may_alias::Bool
    inplace_move::Bool
    deps::Vector{HierarchicalTaskInfo}
    # Indices of same-region producer tasks whose results are passed as
    # arguments (e.g. `In(t1)`). These cannot be `fetch`'d during the
    # pre-scan because they are not launched yet; they become hard DAG edges.
    value_deps::Vector{Int}
end

### Cross-partition chunk ownership ###
#
# !!! DEAD CODE (unreachable as of the `use_shared_state` gate below) !!!
#
# Everything in this section -- `ChunkOwnership`, `SharedChunkRegistry`,
# `build_shared_chunk_registry`, `is_shared_chunk`, `_sync_incoming_ownership!`,
# `_commit_ownership!`, and the `ownership=` path they drive in
# `distribute_task!` -- only runs on the parallel per-partition path. That path
# is now taken only when every processor shares a single memory space, and
# `build_shared_chunk_registry` returns `nothing` unless partitions span two or
# more spaces. The two conditions are mutually exclusive, so `registry` is
# always `nothing` wherever it is consulted.
#
# It is retained (rather than deleted) because it is the scaffolding for the
# follow-up that re-enables parallel planning across memory spaces. Note that it
# was *not* sufficient on its own: it patches ownership hand-off but not the
# space-keyed currency tracking (`arg_current` / `arg_owner` / per-space slots),
# which is what actually breaks. It also keys on the argument object, so a
# `ChunkView` and the `Chunk` it views are tracked separately and their sharing
# is missed. Anyone reviving this must fix both before flipping the gate.
#
# Each partition schedules with its own `DataDepsState`, so `arg_owner` /
# `arg_history` / physical slots are per-partition. When a backing chunk is
# written by tasks in >=2 partitions that live in *different* memory spaces,
# each partition would otherwise copy that chunk from its origin, write its own
# sub-range, and record itself as owner -- the physical copies then diverge and
# the final copy-back keeps only one, silently losing the others' writes.
#
# The registry below carries the single authoritative version ("ownership") of
# each such shared chunk across partition boundaries. It is deadlock-free by
# construction: a single lock (never per-argument locks acquired in different
# orders) plus the global DAG ordering -- a consumer partition always `wait`s on
# its cross-partition predecessor's `task_submitted` event before scheduling, so
# the producer's ownership commit is always visible before the consumer reads it.
# The lock only provides memory-safety for concurrent access to *different*
# chunks; per-chunk correctness comes from the DAG order.
"Authoritative, cross-partition ownership state for a single shared backing chunk."
mutable struct ChunkOwnership
    owner_space::MemorySpace              # space holding the current version
    owner_slot::Any                       # physical slot chunk in `owner_space`
    owner_task::Union{DTask,Nothing}      # producer of the current version (nothing => origin data)
    owner_state::Union{DataDepsState,Nothing} # owning partition's state (for copy-back)
    const origin_space::MemorySpace       # the chunk's home space
end

struct SharedChunkRegistry
    entries::IdDict{Any,ChunkOwnership}   # backing chunk (identity) => ownership
    lock::ReentrantLock
end
SharedChunkRegistry() = SharedChunkRegistry(IdDict{Any,ChunkOwnership}(), ReentrantLock())

is_shared_chunk(::Nothing, @nospecialize(chunk)) = false
is_shared_chunk(reg::SharedChunkRegistry, @nospecialize(chunk)) = haskey(reg.entries, chunk)

"""
    build_shared_chunk_registry(task_metas, vertex_to_partition, partition_space) -> SharedChunkRegistry or nothing

Detects backing chunks accessed by partitions spanning >=2 distinct memory
spaces (the only case that can split-brain) and returns a registry seeded with
origin ownership for each. Returns `nothing` when all partitions share a single
space (e.g. single-worker), so the fast path is entirely unchanged there.
"""
function build_shared_chunk_registry(task_metas::Vector{HierarchicalTaskMeta},
                                     vertex_to_partition::Vector{Int},
                                     partition_space::Vector{<:MemorySpace})
    length(unique(partition_space)) <= 1 && return nothing

    chunk_spaces = IdDict{Any,Set{MemorySpace}}()
    chunk_origin = IdDict{Any,MemorySpace}()
    for v in eachindex(task_metas)
        pspace = partition_space[vertex_to_partition[v]]
        for dep in task_metas[v].deps
            chunk = dep.arg_w.arg
            push!(get!(()->Set{MemorySpace}(), chunk_spaces, chunk), pspace)
            haskey(chunk_origin, chunk) || (chunk_origin[chunk] = memory_space(chunk))
        end
    end

    reg = SharedChunkRegistry()
    for (chunk, spaces) in chunk_spaces
        length(spaces) >= 2 || continue
        origin = chunk_origin[chunk]
        reg.entries[chunk] = ChunkOwnership(origin, chunk, nothing, nothing, origin)
    end
    isempty(reg.entries) && return nothing
    return reg
end

"""
    _sync_incoming_ownership!(state, registry, our_space, task_arg_ws, write_num)

Before a task's copy-to phase, for each shared backing-chunk argument whose
globally-current owner (per `registry`) lives in a space other than `our_space`,
seed this partition's `state` so the existing copy-to machinery pulls a fresh
whole-chunk copy from the true owner (with a syncdep on the producing task). A
no-op for private chunks or when we already hold the authoritative version.
"""
function _sync_incoming_ownership!(state::DataDepsState, registry::SharedChunkRegistry,
                                   our_space::MemorySpace, task_arg_ws::Vector{TaskArgInfo}, write_num::Int)
    deps_vec = state.scratch_deps
    for arg_ws in task_arg_ws
        (arg_ws.may_alias && arg_ws.inplace_move) || continue
        chunk = arg_ws.arg
        entry = get(registry.entries, chunk, nothing)
        entry === nothing && continue

        owner_space, owner_slot, owner_task = @lock registry.lock begin
            (entry.owner_space, entry.owner_slot, entry.owner_task)
        end
        owner_space == our_space && continue # we already hold the authoritative copy

        # Register the owner's physical slot so slot/aliasing lookups in this
        # partition reuse it (rather than generating a fresh, stale copy).
        dest_args = get!(IdDict{Any,Any}, state.remote_args, owner_space)
        if !haskey(dest_args, chunk)
            dest_args[chunk] = owner_slot
            state.remote_arg_to_original[owner_slot] = chunk
        end

        for di in arg_deps_range(arg_ws)
            dep = deps_vec[di]
            arg_w = dep.arg_w
            # Point ownership at the owner space and clear any locally-merged
            # history, so `compute_remainder_for_arg!` takes the `FullCopy`
            # (whole-chunk) path from the owner rather than a partial remainder.
            state.arg_owner[arg_w] = owner_space
            haskey(state.arg_history, arg_w) && empty!(state.arg_history[arg_w])
            src_ainfo = aliasing!(state, owner_space, arg_w)
            if owner_task !== nothing
                # Make the ensuing copy-to (via `get_read_deps!`) wait on the
                # producer, so we never copy the owner slot before it is written.
                state.ainfos_owner[src_ainfo] = owner_task => (write_num - 1)
            end
        end
    end
    return
end

"""
    _commit_ownership!(state, registry, our_space, task, task_arg_ws)

After a task is recorded as a writer, publish it as the new authoritative owner
of each shared backing chunk it writes, so subsequent cross-partition consumers
pull from here. Ordering (and thus visibility) is guaranteed by the DAG's
`task_submitted` handshake; the lock only guards concurrent commits for other
chunks.
"""
function _commit_ownership!(state::DataDepsState, registry::SharedChunkRegistry,
                            our_space::MemorySpace, task::DTask, task_arg_ws::Vector{TaskArgInfo})
    deps_vec = state.scratch_deps
    for arg_ws in task_arg_ws
        (arg_ws.may_alias && arg_ws.inplace_move) || continue
        chunk = arg_ws.arg
        entry = get(registry.entries, chunk, nothing)
        entry === nothing && continue

        any(di->deps_vec[di].writedep, arg_deps_range(arg_ws)) || continue

        dest_args = get(state.remote_args, our_space, nothing)
        slot = dest_args === nothing ? nothing : get(dest_args, chunk, nothing)
        @lock registry.lock begin
            entry.owner_space = our_space
            slot !== nothing && (entry.owner_slot = slot)
            entry.owner_task = task
            entry.owner_state = state
        end
    end
    return
end

# Below this many tasks, the fixed costs of spawning threads and merging
# per-thread results outweigh the benefit of parallelizing the pre-scan.
const COLLECT_ALIASED_ARGS_MIN_CHUNK = 256

# Thread-safe "get or compute" against a shared `IdDict` cache. The
# expensive computation (`f`) is performed outside of the lock so that
# multiple threads can make progress on distinct keys concurrently; if two
# threads race on the same key, the loser's result is simply discarded (the
# corresponding `Chunk`/Bool is cheap to let the GC reclaim) so that every
# thread agrees on a single canonical value (e.g. `Chunk`) per raw argument.
@inline function _cached_get!(f, cache::IdDict{Any,V}, cache_lock::Union{ReentrantLock,Nothing}, key) where V
    if cache_lock === nothing
        return get!(f, cache, key)
    end
    @lock cache_lock begin
        haskey(cache, key) && return cache[key]::V
    end
    result = f()::V
    @lock cache_lock begin
        return get!(cache, key, result)::V
    end
end

"""
    collect_aliased_args(seen_tasks) -> (task_metas, unique_arg_ws)

Pre-scans all tasks to collect per-task dependency metadata and the set of
unique `ArgumentWrapper`s that need aliasing analysis. This mirrors the logic
in `populate_task_info!` but only inspects arguments without modifying any
scheduling state.

For large batches of tasks (the common case for e.g. panel-factorization
algorithms which submit many small tasks per `spawn_datadeps` region), the
pre-scan itself (not just the aliasing computation in
`build_aliasing_parallel`) can dominate scheduling time, since it touches
every argument of every task. This is parallelized across threads: each
thread scans a contiguous range of `seen_tasks` into its own disjoint slice
of `task_metas` and its own local `unique_arg_ws` map (merged at the end),
while sharing (lock-protected) caches for `Chunk`-wrapping and
`supports_inplace_move`, ensuring a single canonical `Chunk` identity per
raw argument regardless of which thread first observes it.
"""
function collect_aliased_args(seen_tasks::Vector{DTaskPair})
    n = length(seen_tasks)
    task_metas = Vector{HierarchicalTaskMeta}(undef, n)
    n == 0 && return task_metas, Dict{ArgumentWrapper,ArgumentWrapper}()

    # Map in-region tasks to vertex indices so we can record value deps
    # without fetching unlaunched DTasks during the pre-scan.
    task_to_idx = IdDict{DTask,Int}()
    for (i, pair) in enumerate(seen_tasks)
        task_to_idx[pair.task] = i
    end

    supports_cache = IdDict{Any,Bool}()
    # Values are Chunks, or raw remote handles kept by `datadeps_arg_wrap`
    # (e.g. ChunkView under MPI).
    raw_arg_cache = IdDict{Any,Any}()

    # Under uniform execution (MPI), `tochunk` / `MPIRefID` allocation must stay
    # sequential on the root task, and spawned threads would not inherit the
    # TaskLocalValue acceleration (falling back to Distributed → DRef chunks).
    # PERF(hier-1): this serializes all of Phase 1 under MPI. Parallelizing it
    # requires a rank-deterministic ID allocation scheme (e.g. per-chunk reserved
    # ID ranges), not just dropping the `uniform_execution()` guard.
    nchunks = if uniform_execution() || Threads.nthreads() <= 1
        1
    else
        min(Threads.nthreads(), cld(n, COLLECT_ALIASED_ARGS_MIN_CHUNK))
    end

    if nchunks <= 1
        unique_arg_ws = Dict{ArgumentWrapper, ArgumentWrapper}()
        _collect_aliased_args_range!(task_metas, unique_arg_ws, seen_tasks, 1:n,
                                      supports_cache, raw_arg_cache, nothing, task_to_idx)
        return task_metas, unique_arg_ws
    end

    cache_lock = ReentrantLock()
    chunk_size = cld(n, nchunks)
    starts = collect(1:chunk_size:n)
    per_chunk_arg_ws = Vector{Dict{ArgumentWrapper,ArgumentWrapper}}(undef, length(starts))
    # Propagate the caller's acceleration into each worker thread; TaskLocalValue
    # does not inherit across `Threads.@spawn`.
    parent_accel = current_acceleration()

    @sync for (ci, start) in enumerate(starts)
        range = start:min(start+chunk_size-1, n)
        Threads.@spawn begin
            set_task_acceleration!(parent_accel)
            local_arg_ws = Dict{ArgumentWrapper,ArgumentWrapper}()
            _collect_aliased_args_range!(task_metas, local_arg_ws, seen_tasks, range,
                                          supports_cache, raw_arg_cache, cache_lock, task_to_idx)
            per_chunk_arg_ws[ci] = local_arg_ws
        end
    end

    unique_arg_ws = per_chunk_arg_ws[1]
    for ci in 2:length(per_chunk_arg_ws)
        merge!(unique_arg_ws, per_chunk_arg_ws[ci])
    end

    return task_metas, unique_arg_ws
end

function _collect_aliased_args_range!(task_metas::Vector{HierarchicalTaskMeta},
                                      unique_arg_ws::Dict{ArgumentWrapper,ArgumentWrapper},
                                      seen_tasks::Vector{DTaskPair},
                                      range::UnitRange{Int},
                                      supports_cache::IdDict{Any,Bool},
                                      raw_arg_cache::IdDict{Any,Any},
                                      cache_lock::Union{ReentrantLock,Nothing},
                                      task_to_idx::IdDict{DTask,Int})
    for task_idx in range
        pair = seen_tasks[task_idx]
        spec = pair.spec
        task = pair.task
        fargs = spec.fargs

        all_deps = HierarchicalTaskInfo[]
        value_deps = Int[]
        first_chunk = nothing
        task_may_alias = false
        task_inplace = false

        for arg_idx in (is_typed(spec) ? (1:length(fargs)) : eachindex(fargs))
            _arg = fargs[arg_idx]
            _arg_with_deps = value(_arg)

            arg_pre_unwrap, deps = unwrap_inout(_arg_with_deps)

            # An unlaunched `DTask` argument cannot be `fetch`ed here -- that is
            # what `distribute_task!` does later, once the producer has actually
            # been submitted. For a *same-region* producer we record a value
            # dependency, which `build_dependency_dag` turns into a hard edge;
            # aliasing of its result is picked up in `distribute_task!`.
            #
            # N.B. An unlaunched producer from *outside* the region (`pred_idx ==
            # 0`, reachable when the region is nested inside a `spawn_bulk` that
            # has not flushed) is skipped entirely: no value dep and no aliasing
            # entry. `distribute_task!` still fetches and tracks it, so the task
            # itself is correct, but it contributes no DAG edges -- so if another
            # in-region task aliases the same underlying data, the dependency
            # between them is missed. Handling this needs the pre-scan to be able
            # to wait on out-of-region producers.
            if arg_pre_unwrap isa DTask && !istaskstarted(arg_pre_unwrap)
                pred_idx = get(task_to_idx, arg_pre_unwrap, 0)
                if pred_idx != 0 && pred_idx != task_idx
                    push!(value_deps, pred_idx)
                end
                continue
            end

            arg = arg_pre_unwrap isa DTask ? fetch(arg_pre_unwrap; raw=true) : arg_pre_unwrap

            may_alias = type_may_alias(typeof(arg))
            inplace_move = may_alias && _cached_get!(supports_cache, cache_lock, arg) do
                supports_inplace_move(arg)
            end

            if !may_alias || !inplace_move
                continue
            end

            arg_chunk = _cached_get!(raw_arg_cache, cache_lock, arg) do
                # Match `populate_task_info!`: acceleration-aware wrap (MPIRef
                # under MPI) rather than a bare `tochunk` that can produce a
                # rank-local DRef when TLS acceleration is unset.
                arg isa Chunk ? arg : datadeps_arg_wrap(arg)
            end

            if first_chunk === nothing
                first_chunk = arg_chunk
                task_may_alias = true
                task_inplace = true
            end

            for (dep_mod, readdep, writedep) in deps
                arg_w = ArgumentWrapper(arg_chunk, dep_mod)
                unique_arg_ws[arg_w] = arg_w
                push!(all_deps, HierarchicalTaskInfo(arg_w, readdep, writedep))
            end
        end

        task_metas[task_idx] = HierarchicalTaskMeta(
            pair, first_chunk, task_may_alias, task_inplace, all_deps, value_deps
        )
    end
end

"""
    build_aliasing_parallel(unique_arg_ws) -> (lookup, ainfos_overlaps, arg_to_ainfo)

Phase 1: Computes `AliasingWrapper` for every unique `ArgumentWrapper` in
parallel. On each worker, threads are used to compute aliasing info for local
data. Results are gathered and reduced into a single `AliasingLookup` with
overlap information.
"""
function build_aliasing_parallel(unique_arg_ws::Dict{ArgumentWrapper, ArgumentWrapper})
    arg_ws_vec = collect(values(unique_arg_ws))

    by_worker = Dict{Int, Vector{ArgumentWrapper}}()
    for arg_w in arg_ws_vec
        wid = root_worker_id(memory_space(arg_w.arg))
        worker_args = get!(Vector{ArgumentWrapper}, by_worker, wid)
        push!(worker_args, arg_w)
    end

    arg_to_ainfo = Dict{ArgumentWrapper, AliasingWrapper}()

    # PERF(hier-3): this is the one phase that genuinely scales out -- each
    # worker computes aliasing for its own data. Phase 4 does not yet do the
    # same for `distribute_task!`, which is the more expensive half.
    if length(by_worker) == 1
        # Common single-worker case: avoid the `@sync`/`Threads.@spawn`/lock
        # overhead entirely, since there's nothing to run concurrently with.
        # `_compute_aliasing_batch` still uses threads internally when there
        # are enough args to make it worthwhile.
        wid, worker_args = only(by_worker)
        results = wid == myid() ? _compute_aliasing_batch(worker_args) :
                                   remotecall_fetch(_compute_aliasing_batch, wid, worker_args)
        # Key by the *local* `arg_w`, not the pair's: for a remote worker the
        # returned `ArgumentWrapper` is a deserialized copy that need not be
        # identity/hash-equal to the entry in `arg_ws_vec` we later look up
        # (which would raise a `KeyError`). `_compute_aliasing_batch` preserves
        # input order, so pair by index.
        @assert length(results) == length(worker_args) "build_aliasing_parallel: _compute_aliasing_batch returned $(length(results)) results for $(length(worker_args)) args (wid=$wid)"
        for i in eachindex(worker_args)
            arg_to_ainfo[worker_args[i]] = results[i].second
        end
    else
        all_results_lock = ReentrantLock()
        @sync for (wid, worker_args) in by_worker
            Threads.@spawn begin
                # `local` is REQUIRED. The single-worker branch above assigns to
                # `results` in this function's scope, which makes `results` a
                # local of `build_aliasing_parallel` -- so without `local` here,
                # every spawned closure would capture and assign to that *same*
                # binding. All partitions then race on one cell: the last writer
                # wins and every reader sees its vector instead of its own,
                # which surfaces as a `BoundsError` at `results[i]` whenever the
                # winner's `worker_args` is shorter than the reader's (and, more
                # insidiously, as silently wrong aliasing when it is longer).
                local results
                results = if wid == myid()
                    _compute_aliasing_batch(worker_args)
                else
                    remotecall_fetch(_compute_aliasing_batch, wid, worker_args)
                end
                # Key by the *local* `arg_w` (see single-worker note above): a
                # remote worker returns deserialized `ArgumentWrapper` copies
                # that may not compare equal to our `arg_ws_vec` lookup keys.
                @assert length(results) == length(worker_args) "build_aliasing_parallel: _compute_aliasing_batch returned $(length(results)) results for $(length(worker_args)) args (wid=$wid)"
                @lock all_results_lock begin
                    for i in eachindex(worker_args)
                        arg_to_ainfo[worker_args[i]] = results[i].second
                    end
                end
            end
        end
    end

    lookup = AliasingLookup()
    ainfos_overlaps = Dict{AliasingWrapper, Set{AliasingWrapper}}()

    for arg_w in arg_ws_vec
        ainfo = arg_to_ainfo[arg_w]
        if haskey(ainfos_overlaps, ainfo)
            continue
        end

        ainfo_idx = push!(lookup, ainfo)
        overlaps = Set{AliasingWrapper}()
        push!(overlaps, ainfo)
        for other_ainfo in intersect(lookup, ainfo; ainfo_idx)
            ainfo == other_ainfo && continue
            push!(overlaps, other_ainfo)
            push!(ainfos_overlaps[other_ainfo], ainfo)
        end
        ainfos_overlaps[ainfo] = overlaps
    end

    return lookup, ainfos_overlaps, arg_to_ainfo
end

# Below this many args, the fixed cost of forking/joining `Threads.@threads`
# outweighs the benefit of parallelizing the (typically cheap) `aliasing()` calls.
const COMPUTE_ALIASING_BATCH_MIN_PARALLEL = 8

function _compute_aliasing_batch(arg_ws::Vector{ArgumentWrapper})
    n = length(arg_ws)
    results = Vector{Pair{ArgumentWrapper, AliasingWrapper}}(undef, n)
    accel = current_acceleration()
    # Under uniform execution (MPI), aliasing may perform collectives that must
    # run in the same sequential order on every rank -- never Threads.@threads.
    # PERF(hier-1): the MPI arm of this is a correctness requirement, not an
    # oversight; only a collective-free aliasing path could relax it.
    # Also dispatch through `aliasing(accel, ...)` so MPI Chunks go through the
    # owner-broadcast path rather than a local unwrap.
    can_parallel = !uniform_execution(accel) &&
                   n >= COMPUTE_ALIASING_BATCH_MIN_PARALLEL &&
                   Threads.nthreads() > 1
    if can_parallel
        Threads.@threads for i in 1:n
            arg_w = arg_ws[i]
            ainfo = AliasingWrapper(aliasing(accel, arg_w.arg, arg_w.dep_mod))
            results[i] = arg_w => ainfo
        end
    else
        for i in 1:n
            arg_w = arg_ws[i]
            ainfo = AliasingWrapper(aliasing(accel, arg_w.arg, arg_w.dep_mod))
            results[i] = arg_w => ainfo
        end
    end
    return results
end

"""
    build_dependency_dag(task_metas, arg_to_ainfo, ainfos_overlaps)
        -> SimpleDiGraph

Phase 2: Walks tasks in submission order and builds a `SimpleDiGraph` encoding
data dependencies based on the pre-computed aliasing overlaps. Uses the same
WAW / RAW / WAR rules as `get_write_deps!` / `get_read_deps!`.

PERF(hier-4): single-threaded, and inherently so -- the owner/reader state is
carried forward across vertices in submission order. Cheap next to Phases 1 and
4 today; revisit if it starts to show up in profiles.
"""
function build_dependency_dag(task_metas::Vector{HierarchicalTaskMeta},
                              arg_to_ainfo::Dict{ArgumentWrapper, AliasingWrapper},
                              ainfos_overlaps::Dict{AliasingWrapper, Set{AliasingWrapper}})
    n = length(task_metas)
    dag = SimpleDiGraph(n)

    ainfos_owner = Dict{AliasingWrapper, Union{Nothing, Pair{Int,Int}}}()
    ainfos_readers = Dict{AliasingWrapper, Vector{Pair{Int,Int}}}()

    write_num = 1
    for v in 1:n
        meta = task_metas[v]

        # Hard edges from same-region DTask value arguments (producer must
        # be launched before we can fetch its result in distribute_task!).
        for pred_v in meta.value_deps
            if pred_v != v
                add_edge!(dag, pred_v, v)
            end
        end

        # Add dependency edges
        for dep in meta.deps
            ainfo = get(arg_to_ainfo, dep.arg_w, nothing)
            ainfo === nothing && continue

            if !haskey(ainfos_owner, ainfo)
                ainfos_owner[ainfo] = nothing
                ainfos_readers[ainfo] = Pair{Int,Int}[]
            end

            overlaps = get(ainfos_overlaps, ainfo, Set{AliasingWrapper}())

            if dep.writedep
                for other_ainfo in overlaps
                    owner = get(ainfos_owner, other_ainfo, nothing)
                    if owner !== nothing
                        pred_v, pred_wn = owner
                        if pred_wn != write_num && pred_v != v
                            add_edge!(dag, pred_v, v)
                        end
                    end
                    for (reader_v, reader_wn) in get(ainfos_readers, other_ainfo, Pair{Int,Int}[])
                        if reader_wn != write_num && reader_v != v
                            add_edge!(dag, reader_v, v)
                        end
                    end
                end
            else
                for other_ainfo in overlaps
                    owner = get(ainfos_owner, other_ainfo, nothing)
                    if owner !== nothing
                        pred_v, pred_wn = owner
                        if pred_wn != write_num && pred_v != v
                            add_edge!(dag, pred_v, v)
                        end
                    end
                end
            end
        end

        # Update ownership tracking
        for dep in meta.deps
            ainfo = get(arg_to_ainfo, dep.arg_w, nothing)
            ainfo === nothing && continue

            if !haskey(ainfos_owner, ainfo)
                ainfos_owner[ainfo] = nothing
                ainfos_readers[ainfo] = Pair{Int,Int}[]
            end

            if dep.writedep
                ainfos_owner[ainfo] = v => write_num
                empty!(ainfos_readers[ainfo])
            else
                push!(ainfos_readers[ainfo], v => write_num)
            end
        end

        write_num += 1
    end

    return dag
end

"""
Target number of tasks per partition when partitioning across the local
processors of a single owner.

Partitioning a single owner parallelizes *planning* only -- every partition
still places work on the same processors -- so it has to earn back what it
duplicates. Each partition builds its own `DataDepsState`, so an argument shared
between partitions has its aliasing and slot bookkeeping redone once per
partition; with neighbour-sharing workloads (stencils) that duplication grows
quickly with the partition count.
"""
const HIER_TASKS_PER_PARTITION = 16

"""
    single_owner_partition_count(ntasks, nprocs) -> Int

How many partitions to split `ntasks` into when all processors share an owner.

Currently always 1 (plan flat): with the flat planner's per-task cost now in
the tens of microseconds, measurements on 8 threads show partitioned planning
losing to flat at every tested single-owner scale — 576 tasks: 26 ms flat vs
84 ms partitioned; 4096 tasks: 187 ms flat vs 384 ms partitioned — because the
partition machinery (aliasing prescan, DAG build, per-partition states,
cross-partition syncdeps) costs more than the planning it parallelizes.
Multi-owner regions are unaffected (they partition by data ownership, not by
this throughput heuristic). Revisit if per-partition overheads shrink or a
scale is found where serial planning dominates.

Capped at half of `nprocs` because partition planning tasks and the processor
runners execute on the same threads: one partition per processor leaves nothing
to run the work being planned, and measures slower than not partitioning at all.
"""
single_owner_partition_count(ntasks::Int, nprocs::Int) =
    1

"""
    partition_dag(dag, task_metas, all_procs) -> (vertex_to_partition, n_partitions, partition_procs)

Phase 3: Assigns each task vertex to a partition using data-affinity. For
multi-owner setups (Distributed workers, or MPI ranks via
`partition_affinity_id`), tasks are assigned to the owner holding the most
argument data. For single-owner multi-threaded setups, tasks are balanced
across available processors in topological order.
"""
function partition_dag(dag::SimpleDiGraph, task_metas::Vector{HierarchicalTaskMeta},
                       all_procs::Vector{<:Processor})
    n = length(task_metas)
    # Stable order so SPMD ranks (and Distributed workers) agree on partition
    # indexing when affinity ids are collected from an unordered processor set.
    owners = sort!(unique(partition_affinity_id.(all_procs)))
    n_owners = length(owners)

    procs_by_owner = Dict{Int, Vector{Processor}}()
    for proc in all_procs
        oid = partition_affinity_id(proc)
        push!(get!(Vector{Processor}, procs_by_owner, oid), proc)
    end

    multi_owner = n_owners > 1
    if multi_owner
        # PERF(hier-2): one partition per owner caps planning concurrency at the
        # worker/rank count, ignoring each owner's thread count. Sub-partitioning
        # each owner's vertices across its own procs (cf. the single-owner branch
        # below) would let a 2-worker x 16-thread cluster plan on more than 2
        # threads. Doing so must keep each sub-partition's `local_procs` within a
        # single owner, so data affinity is preserved.
        n_partitions = n_owners
        partition_owner = owners
    else
        n_partitions = single_owner_partition_count(n, length(all_procs))
        partition_owner = fill(first(owners), n_partitions)
    end

    vertex_to_partition = Vector{Int}(undef, n)

    if multi_owner
        owner_to_partition = Dict(o => i for (i, o) in enumerate(owners))
        default_scope = DefaultScope()
        for v in 1:n
            meta = task_metas[v]
            task_scope = @something(meta.pair.spec.options.compute_scope, meta.pair.spec.options.scope, default_scope)

            # Owners whose processors are eligible under this task's scope.
            # A non-default scope that spans multiple owners must still spread
            # work across those owners (via affinity / round-robin) -- picking
            # only the first match pins everything to owner 1 and breaks
            # multi-owner execution.
            if task_scope == default_scope
                matching = collect(1:n_partitions)
            else
                matching = Int[]
                for (pid, oid) in enumerate(owners)
                    oprocs = procs_by_owner[oid]
                    if any(proc -> proc_in_scope(proc, task_scope), oprocs)
                        push!(matching, pid)
                    end
                end
                if isempty(matching)
                    matching = [1]
                end
            end

            if length(matching) == 1
                vertex_to_partition[v] = only(matching)
                continue
            end

            # Owners are ranked by (written args, read args) rather than by one
            # combined count, because a written argument is worth strictly more
            # than any number of read ones: running away from it copies it in
            # *and* back out again at region end, while a read argument only
            # copies in. Counting them together lets a task with many small
            # read-only arguments (a stencil's halo neighbors, say) be dragged
            # away from the chunk it writes, so every sweep ships the output
            # chunk both ways.
            #
            # N.B. Argument *counts*, not byte counts: `datasize` of a chunk is
            # only known on its owning rank (see `datasize(::MPIRef)`), and this
            # decision must come out identical on every rank under SPMD.
            write_affinity = zeros(Int, n_owners)
            read_affinity = zeros(Int, n_owners)
            for dep in meta.deps
                arg_space = memory_space(dep.arg_w.arg)
                arg_oid = partition_affinity_id(arg_space)
                idx = get(owner_to_partition, arg_oid, 0)
                if idx > 0 && idx in matching
                    if dep.writedep
                        write_affinity[idx] += 1
                    else
                        read_affinity[idx] += 1
                    end
                end
            end
            best_pid = matching[1]
            best_aff = (-1, -1)
            for pid in matching
                aff = (write_affinity[pid], read_affinity[pid])
                if aff > best_aff
                    best_aff = aff
                    best_pid = pid
                end
            end
            if best_aff == (0, 0)
                vertex_to_partition[v] = matching[mod1(v, length(matching))]
            else
                vertex_to_partition[v] = best_pid
            end
        end
    else
        topo = try
            topological_sort_by_dfs(dag)
        catch
            collect(1:n)
        end

        default_scope = DefaultScope()
        partition_load = zeros(Int, n_partitions)
        for v in topo
            meta = task_metas[v]
            task_scope = @something(meta.pair.spec.options.compute_scope, meta.pair.spec.options.scope, default_scope)

            if task_scope != default_scope
                assigned = false
                for pid in 1:n_partitions
                    pidx = mod1(pid, length(all_procs))
                    if proc_in_scope(all_procs[pidx], task_scope)
                        vertex_to_partition[v] = pid
                        partition_load[pid] += 1
                        assigned = true
                        break
                    end
                end
                if !assigned
                    best = argmin(partition_load)
                    vertex_to_partition[v] = best
                    partition_load[best] += 1
                end
            else
                best = argmin(partition_load)
                vertex_to_partition[v] = best
                partition_load[best] += 1
            end
        end
    end

    partition_procs = Vector{Vector{Processor}}(undef, n_partitions)
    if multi_owner
        for pid in 1:n_partitions
            oid = partition_owner[pid]
            partition_procs[pid] = procs_by_owner[oid]
        end
    else
        for pid in 1:n_partitions
            partition_procs[pid] = copy(all_procs)
        end
    end

    return vertex_to_partition, n_partitions, partition_procs, multi_owner
end


"""
    _schedule_vertex!(v, partition_id, temp_queue, state, local_procs,
                      local_scope, dag, seen_tasks, vertex_to_partition,
                      proc_to_scope_lfu, write_num) -> write_num

Schedules a single (already topologically-ordered) task vertex `v` into its
partition's `state` via `distribute_task!`, returning the updated `write_num`.

Records cross-partition predecessors as explicit `ThunkSyncdep`s (same-partition
deps are derived from `state` by `distribute_task!`); processor assignment is
performed by `temp_queue`'s partition-local scheduler over `local_procs`.

Callers must guarantee that every predecessor of `v` (in any partition) has
already been submitted before calling this, so the `ThunkSyncdep`s are valid.
"""
function _schedule_vertex!(v::Int, partition_id::Int,
                           temp_queue::DataDepsTaskQueue,
                           state::DataDepsState,
                           local_procs::Vector{<:Processor},
                           local_scope,
                           dag::SimpleDiGraph,
                           seen_tasks::Vector{DTaskPair},
                           vertex_to_partition::Vector{Int},
                           proc_to_scope_lfu,
                           write_num::Int,
                           registry::Union{SharedChunkRegistry,Nothing})
    pair = seen_tasks[v]
    spec = pair.spec
    task = pair.task

    if spec.options.syncdeps === nothing
        spec.options.syncdeps = Set{ThunkSyncdep}()
    end
    for pred_v in inneighbors(dag, v)
        if vertex_to_partition[pred_v] != partition_id
            pred_task = seen_tasks[pred_v].task
            push!(spec.options.syncdeps, ThunkSyncdep(pred_task))
        end
    end

    return distribute_task!(temp_queue, state, local_procs, local_scope,
                            spec, task, spec.fargs,
                            proc_to_scope_lfu, write_num; ownership=registry)
end

"""
    schedule_partition_full!(queue, batch_queue, partition_id, partition_verts,
                             dag, seen_tasks, local_procs,
                             vertex_to_partition, task_submitted,
                             value_dep_verts, registry) -> DataDepsState

Parallel per-partition scheduling, used only when every processor shares one
memory space (see `use_shared_state` in `distribute_tasks_hierarchical!`);
multi-space regions take `schedule_partitions_sequential!` instead. Uses the
existing `distribute_task!` logic with a per-partition `DataDepsState`,
`all_procs` limited to this partition's processors, and cross-partition syncdeps
from the precomputed DAG.

Processor assignment is performed by a partition-local scheduler shard (see
`similar` below) over just `local_procs`.
"""
function schedule_partition_full!(queue::DataDepsTaskQueue,
                                  batch_queue::BatchedEnqueueQueue,
                                  partition_id::Int,
                                  partition_verts::Vector{Int},
                                  dag::SimpleDiGraph,
                                  seen_tasks::Vector{DTaskPair},
                                  local_procs::Vector{<:Processor},
                                  vertex_to_partition::Vector{Int},
                                  task_submitted::Vector{Base.Event},
                                  value_dep_verts::Set{Int},
                                  registry::Union{SharedChunkRegistry,Nothing})
    if isempty(partition_verts) || isempty(local_procs)
        return DataDepsState()
    end

    local_scope = UnionScope(map(ExactScope, local_procs))

    state = DataDepsState()
    write_num = 1
    proc_to_scope_lfu = BasicLFUCache{Processor,AbstractScope}(1024)

    vert_set = Set{Int}(partition_verts)
    topo = try
        topological_sort_by_dfs(dag)
    catch
        collect(vertices(dag))
    end
    ordered_verts = filter(v -> v in vert_set, topo)

    # N.B. Each partition gets its own fresh scheduler shard via `similar`
    # rather than sharing `queue.scheduler` across all partitions. Two
    # independent problems would arise from sharing a single scheduler
    # instance here:
    #  1) Data race: e.g. `RoundRobinScheduler.proc_idx` would be
    #     concurrently read/written by every partition's `Threads.@spawn`
    #     task with no synchronization.
    #  2) Semantic bug (worse than the race, and *not* fixed by adding a
    #     lock): each partition schedules only onto its own worker's
    #     `local_procs`, which generally has a *different length* than
    #     other partitions' (or the global) processor list. A `proc_idx`
    #     counter advanced by one partition's `local_procs` is meaningless
    #     -- and can be out-of-bounds -- when applied to another
    #     partition's differently-sized `local_procs`. This reliably
    #     crashes with a `BoundsError` under multi-worker hierarchical
    #     scheduling. Giving each partition its own scheduler instance,
    #     scoped to its own `local_procs`, fixes both issues at once.
    temp_queue = DataDepsTaskQueue(batch_queue; scheduler=similar(queue.scheduler))

    # N.B. If this partition throws partway through (e.g. from
    # `distribute_task!`), any of our vertices that haven't yet been
    # `notify`'d will never be, which would leave other partitions blocked
    # forever in `wait(task_submitted[pred_v])` below -- turning a normal,
    # reportable exception into a silent, permanent hang (since the
    # enclosing `@sync` in `distribute_tasks_hierarchical!` can't finish,
    # and thus can't propagate our exception, until *every* spawned
    # partition task completes, including the ones stuck waiting on us).
    # The `finally` ensures every one of our events gets notified no matter
    # how we exit, so that sibling partitions can unblock (and themselves
    # fail/finish) and our real exception can actually surface.
    # A vertex whose successors all live in this partition is only ever observed
    # by us, so its submission can stay buffered; one with a successor elsewhere
    # must be submitted before we notify, because that successor will record it
    # as a syncdep.
    has_external_successor = Set{Int}()
    for v in ordered_verts
        for succ_v in outneighbors(dag, v)
            if vertex_to_partition[succ_v] != partition_id
                push!(has_external_successor, v)
                break
            end
        end
    end

    try
        for v in ordered_verts
            for pred_v in inneighbors(dag, v)
                if vertex_to_partition[pred_v] != partition_id
                    # Anything we have buffered may be a dependency of tasks the
                    # producing partition is about to prepare; get it submitted
                    # before we block, or two partitions can deadlock waiting on
                    # each other's unflushed tasks.
                    flush_batch!(batch_queue)
                    wait(task_submitted[pred_v])
                end
            end

            # A task taking another in-region task's *value* as an argument has
            # `distribute_task!` `fetch` that producer, which requires it to have
            # actually been launched. A same-partition producer is topologically
            # earlier, so it is prepared but possibly still sitting in our batch.
            v in value_dep_verts && flush_batch!(batch_queue)

            # N.B. The per-task `distribute_task!` preparation runs concurrently
            # across partitions; only the final task submission is serialized
            # (via `BatchedEnqueueQueue`, `temp_queue`'s upper queue). This is
            # safe now that task futures are backed by `MemPool.DFuture` rather
            # than the concurrency-unsafe `Distributed.Future`. The
            # cross-partition `wait`s above happen before scheduling `v`, so the
            # `ThunkSyncdep`s recorded for `v` are valid.
            write_num = _schedule_vertex!(
                v, partition_id, temp_queue, state, local_procs, local_scope,
                dag, seen_tasks, vertex_to_partition,
                proc_to_scope_lfu, write_num, registry)

            v in has_external_successor && flush_batch!(batch_queue)
            notify(task_submitted[v])
        end
        flush_batch!(batch_queue)
    finally
        flush_batch!(batch_queue)
        for v in ordered_verts
            notify(task_submitted[v])
        end
    end

    return state
end

"""
    schedule_partitions_sequential!(...) -> Vector{DataDepsState}

SPMD-safe Phase 4: schedule every partition's tasks on the root task in global
topological order. Processor assignment remains partition-local (MPI-rank /
worker affinity), but a *single* shared `DataDepsState` is used so argument
history, remainders, and final copy-back stay coherent across ranks -- the same
model as flat `distribute_tasks!`. Per-partition states would otherwise
split-brain overlapping writes (e.g. whole-array + view + triangular dep_mods).

Returns a one-element vector containing the shared state (for the hierarchical
copy-from/free epilogue).
"""
function schedule_partitions_sequential!(queue::DataDepsTaskQueue,
                                         queue_lock::ReentrantLock,
                                         partitions::Vector{Vector{Int}},
                                         dag::SimpleDiGraph,
                                         seen_tasks::Vector{DTaskPair},
                                         partition_procs::Vector{<:Vector{<:Processor}},
                                         vertex_to_partition::Vector{Int},
                                         registry::Union{SharedChunkRegistry,Nothing},
                                         wait_all_queue)
    n_partitions = length(partitions)
    temp_queues = Vector{DataDepsTaskQueue}(undef, n_partitions)
    local_scopes = Vector{AbstractScope}(undef, n_partitions)
    proc_to_scope_lfus = [BasicLFUCache{Processor,AbstractScope}(1024) for _ in 1:n_partitions]
    shared_state = DataDepsState()
    write_num = 1
    # Shared state already tracks global ownership/history like flat
    # `distribute_tasks!`. Do not pass `registry` as `ownership`: sync/commit
    # would fight the single-state history, and the epilogue must then ignore
    # the registry too (see `distribute_tasks_hierarchical!`).
    ownership = nothing

    for pid in 1:n_partitions
        local_procs = partition_procs[pid]
        if isempty(partitions[pid]) || isempty(local_procs)
            temp_queues[pid] = DataDepsTaskQueue(wait_all_queue; scheduler=similar(queue.scheduler))
            local_scopes[pid] = DefaultScope()
            continue
        end
        local_scope = UnionScope(map(ExactScope, local_procs))
        local_scopes[pid] = local_scope
        locked_queue = LockedEnqueueQueue(wait_all_queue, queue_lock)
        temp_queues[pid] = DataDepsTaskQueue(locked_queue; scheduler=similar(queue.scheduler))
    end

    topo = try
        topological_sort_by_dfs(dag)
    catch
        collect(vertices(dag))
    end

    with_options(; task_queue=LockedEnqueueQueue(wait_all_queue, queue_lock)) do
        for v in topo
            pid = vertex_to_partition[v]
            local_procs = partition_procs[pid]
            isempty(local_procs) && continue
            write_num = _schedule_vertex!(
                v, pid, temp_queues[pid], shared_state, local_procs,
                local_scopes[pid], dag, seen_tasks, vertex_to_partition,
                proc_to_scope_lfus[pid], write_num, ownership)
        end
    end

    return DataDepsState[shared_state]
end

struct LockedEnqueueQueue <: AbstractTaskQueue
    inner::AbstractTaskQueue
    lock::ReentrantLock
end
function enqueue!(leq::LockedEnqueueQueue, pair::DTaskPair)
    @lock leq.lock enqueue!(leq.inner, pair)
end
function enqueue!(leq::LockedEnqueueQueue, pairs::Vector{DTaskPair})
    @lock leq.lock enqueue!(leq.inner, pairs)
end

# Parallel partition scheduling wraps errors in TaskFailedException /
# CompositeException via `@sync`/`Threads.@spawn`. Unwrap so callers and tests
# see the root `SchedulingException` / scheduler error.
function _unwrap_partition_exception(e)
    while true
        if e isa CompositeException && !isempty(e.exceptions)
            e = e.exceptions[1]
        elseif e isa TaskFailedException
            e = something(e.task.exception, e)
        else
            return e
        end
    end
end

"""
    distribute_tasks_hierarchical!(queue)

Main entry point for hierarchical scheduling. Runs the 4-phase pipeline:
1. Parallel aliasing construction
2. DAG construction
3. Partitioning (by Distributed-worker / MPI-rank affinity, or across local
   procs on one owner)
4. Per-partition scheduling via `distribute_task!`

Each partition assigns processors with its own `DataDepsScheduler` shard over
just its processors (see `schedule_partition_full!`); there is no global
scheduling pass or global synchronization, which is what allows this to scale
under Distributed. Both drivers use `distribute_task!` for argument preparation
and `DataDepsScheduler` dispatch; the old single-worker "batch enqueue with DAG
syncdeps only" path is intentionally not used (it skipped `distribute_task!` and
broke `ChunkView` / custom schedulers).

Under uniform execution (MPI), Phase 4 runs sequentially on the root task so
SPMD tag / `MPIRefID` allocation stays deterministic across ranks.
"""
function distribute_tasks_hierarchical!(queue::DataDepsTaskQueue)
    seen_tasks = queue.seen_tasks
    if isempty(seen_tasks)
        return
    end

    # Match flat `distribute_tasks!`: acceleration-aware processor enumeration
    # (Distributed `OSProc`s, or MPI `MPIOSProc`s → `MPIProcessor`s).
    accel = current_acceleration()
    accel_procs = filter(procs(Sch.eager_context())) do proc
        accel_matches_proc(accel, proc)
    end
    all_procs = unique(vcat([collect(get_processors(gp)) for gp in accel_procs]...))
    select_processors_uniform!(all_procs, accel)
    scope = get_compute_scope()
    filter!(proc->proc_in_scope(proc, scope), all_procs)
    if isempty(all_procs)
        throw(Sch.SchedulingException("No processors available, try widening scope"))
    end
    if uniform_execution(accel)
        for proc in all_procs
            @check_uniform(proc)
        end
    end

    # With a single owner, partitioning buys parallel planning and nothing else
    # (see `HIER_TASKS_PER_PARTITION`). A region too small to fill more than one
    # partition would pay for the extra prescan/DAG/partition phases and the
    # per-partition state without ever running two planners at once, so plan it
    # flat instead. Multi-owner regions always partition: there the split is by
    # data ownership, not a throughput heuristic.
    if !uniform_execution(accel) &&
       allequal(partition_affinity_id(proc) for proc in all_procs) &&
       single_owner_partition_count(length(seen_tasks), length(all_procs)) == 1
        return distribute_tasks!(queue)
    end

    # Phase 1: Collect arguments and compute aliasing in parallel
    task_metas, unique_arg_ws = collect_aliased_args(seen_tasks)
    _lookup, ainfos_overlaps, arg_to_ainfo = build_aliasing_parallel(unique_arg_ws)

    # Phase 2: Build dependency DAG
    dag = build_dependency_dag(task_metas, arg_to_ainfo, ainfos_overlaps)

    # Phase 3: Partition the DAG
    vertex_to_partition, n_partitions, partition_procs, multi_owner =
        partition_dag(dag, task_metas, all_procs)

    # Detect backing chunks shared across partitions in different memory spaces.
    # These need runtime ownership transfer to avoid split-brain concurrent
    # writes; `nothing` when all partitions share one space (single-owner).
    partition_space = MemorySpace[only(memory_spaces(first(pp))) for pp in partition_procs]
    registry = build_shared_chunk_registry(task_metas, vertex_to_partition, partition_space)

    # Vertices whose `distribute_task!` will `fetch` an in-region producer's
    # value, and so cannot run until that producer has actually been submitted.
    value_dep_verts = Set{Int}(v for v in 1:length(task_metas)
                                 if !isempty(task_metas[v].value_deps))

    # Group vertices by partition
    partitions = [Int[] for _ in 1:n_partitions]
    for v in 1:length(seen_tasks)
        pid = vertex_to_partition[v]
        push!(partitions[pid], v)
    end

    queue_lock = ReentrantLock()
    task_submitted = [Base.Event() for _ in 1:length(seen_tasks)]
    wait_all_queue = get_options(:task_queue)

    # Phase 4: per-partition scheduling. Two strategies:
    #
    #  * Shared state, sequential (`schedule_partitions_sequential!`): one
    #    `DataDepsState` for the whole region, planned on the root task in global
    #    topological order. Processor assignment stays partition-local, so worker
    #    / rank affinity is preserved, but planning does not run in parallel.
    #
    #  * Per-partition state, parallel (`schedule_partition_full!`): each
    #    partition plans concurrently on its own task with its own state.
    #
    # Uniform execution (MPI) requires the sequential form so `to_tag` / generic
    # `MPIRefID` allocation is SPMD-deterministic and cross-partition deps cannot
    # deadlock.
    #
    # It is also required for *every* multi-owner region. Per-partition states
    # each maintain their own `arg_owner` / `arg_history` / `arg_current` and
    # their own physical slot per (chunk, space). That bookkeeping is keyed by
    # memory *space*, so it cannot represent "two different buffers for one chunk
    # in one space" -- which is exactly what two partitions produce for a chunk
    # they both touch (`slot_is_already_in_place` refuses to reuse a chunk homed
    # on another process, so each partition allocates its own slot). The
    # `SharedChunkRegistry` hand-off patches ownership across that boundary but
    # cannot repair the currency tracking: the region-end write-back concludes
    # the origin is already up to date and silently drops the final writes. A
    # differential test (random datadeps DAGs, flat vs hierarchical, data spread
    # across workers) reproduces this within two tasks:
    #     copyto!(Out(B), In(A)); B .+= 1     # B homed on another worker
    #
    # N.B. The condition is the *number of distinct memory spaces*, deliberately
    # not `registry !== nothing` and not `multi_owner`:
    #
    #  * `registry !== nothing` is too weak. The registry keys on the argument
    #    object, so a `ChunkView` and the `Chunk` it views are separate keys and
    #    their sharing goes undetected -- a 3-worker differential case diverges
    #    exactly that way. Worse, in the single-owner branch every partition gets
    #    the *same* `local_procs`, so `partition_space` (built from each
    #    partition's first proc) is uniform and the registry is never even built.
    #  * `multi_owner` is also too weak: one worker hosting both CPU and GPU
    #    processors is single-owner, yet its partitions still span several memory
    #    spaces and can each hold a distinct slot for one chunk.
    #
    # Requiring a single memory space across `all_procs` covers both, and is the
    # precise condition under which "one slot per (chunk, space)" makes the
    # space-keyed bookkeeping exact. It subsumes `multi_owner`, since procs on
    # different workers necessarily live in different `CPURAMMemorySpace`s.
    # See PERF(hier-2)/(hier-3).
    exec_spaces = unique(Iterators.flatten(memory_spaces(proc) for proc in all_procs))
    use_shared_state = uniform_execution(accel) || length(exec_spaces) > 1
    partition_states = try
        if use_shared_state
            schedule_partitions_sequential!(
                queue, queue_lock, partitions, dag, seen_tasks,
                partition_procs, vertex_to_partition, registry,
                wait_all_queue)
        else
            states = Vector{DataDepsState}(undef, n_partitions)
            @sync for pid in 1:n_partitions
                Threads.@spawn begin
                    batch_queue = BatchedEnqueueQueue(wait_all_queue, queue_lock)
                    # Copy tasks spawned from within `distribute_task!` pick the
                    # queue up from options, so they batch alongside their task.
                    with_options(; task_queue=batch_queue) do
                        try
                            states[pid] = schedule_partition_full!(
                                queue, batch_queue, pid, partitions[pid],
                                dag, seen_tasks,
                                partition_procs[pid], vertex_to_partition,
                                task_submitted, value_dep_verts,
                                registry
                            )
                        finally
                            flush_batch!(batch_queue)
                        end
                    end
                end
            end
            states
        end
    catch e
        rethrow(_unwrap_partition_exception(e))
    end

    # The shared-state path returns a one-element state vector and does not
    # commit into `registry` (`ownership=nothing` there); copy-back must therefore
    # ignore the registry and use the shared state's full history (same as flat).
    # Passing the registry would skip every multi-partition chunk: the registry
    # path needs `owner_state` (never set), and the private path skips shared keys.
    # Distributed keeps per-partition states and needs the registry for coherent
    # cross-partition write-back.
    epilogue_registry = use_shared_state ? nothing : registry
    _hierarchical_copy_from_and_free!(partition_states, length(partition_states), epilogue_registry)
end

function _hierarchical_max_write_num(state::DataDepsState, arg_w::ArgumentWrapper)
    wn = 0
    if haskey(state.arg_history, arg_w)
        for entry in state.arg_history[arg_w]
            wn = max(wn, entry.write_num)
        end
    end
    return wn
end

function _hierarchical_copy_from!(state::DataDepsState, arg_w::ArgumentWrapper, write_num::Int)
    haskey(state.arg_origin, arg_w.arg) || return
    origin_space = state.arg_origin[arg_w.arg]
    # Mirror flat `distribute_tasks!`: when the origin still holds a fully-current
    # replica -- the argument was only read, or copies merely propagated it back --
    # the write-back is pure overhead, so elide it. Safe only here at region end;
    # mid-region the copy tasks also serialize readers against later writers and
    # must not be skipped.
    #
    # N.B. `arg_current` is per-partition state. A partition only records a space
    # as current for an argument it actually planned, so this cannot elide a
    # write-back owed by a *different* partition: shared (multi-partition) chunks
    # are written back from the registry's authoritative `owner_state` above, and
    # private chunks are written back from their last-writer partition's state.
    current = get(state.arg_current, arg_w, nothing)
    if current !== nothing && origin_space in current
        remainder = NoAliasing()
    else
        remainder, _ = compute_remainder_for_arg!(state, origin_space, arg_w, write_num)
    end
    if remainder isa MultiRemainderAliasing
        origin_scope = UnionScope(map(ExactScope, collect(processors(origin_space)))...)
        enqueue_remainder_copy_from!(state, origin_space, arg_w, remainder, origin_scope, write_num)
    elseif remainder isa FullCopy
        origin_scope = UnionScope(map(ExactScope, collect(processors(origin_space)))...)
        enqueue_copy_from!(state, origin_space, arg_w, origin_scope, write_num)
    else
        @assert remainder isa NoAliasing "Expected NoAliasing, got $(typeof(remainder))"
        # Emit the same elision event as flat `distribute_tasks!`. Consumers of
        # the log (e.g. dataflow validation) treat this as the argument coming to
        # rest at its origin; without it an elided write-back looks like data
        # stranded in the remote space. `thunk_id=0` marks it as a non-task flow.
        @dagdebug nothing :spawn_datadeps "Skipped copy-from (up-to-date): $origin_space"
        arg = arg_w.arg
        ctx = Sch.eager_context()
        id = rand(UInt)
        @maybelog ctx timespan_start(ctx, :datadeps_copy_skip, (;id), (;))
        @maybelog ctx timespan_finish(ctx, :datadeps_copy_skip, (;id), (;thunk_id=0, from_space=origin_space, to_space=origin_space, arg_w, from_arg=arg, to_arg=arg))
    end
    return
end

# Deterministic sort key for shared-chunk registry / free-list iteration so
# SPMD ranks enqueue copy-back and free tasks in the same order.
_hierarchical_chunk_sort_key(chunk::Chunk) = (short_name(chunk.space), hash(chunk.handle))
_hierarchical_chunk_sort_key(chunk) = ("", _identity_hash(chunk))

function _hierarchical_copy_from_and_free!(partition_states::Vector{DataDepsState}, n_partitions::Int,
                                           registry::Union{SharedChunkRegistry,Nothing})
    # 1. Shared chunks: DEAD -- `registry` is always `nothing` here (see the
    #    "Cross-partition chunk ownership" note above). Kept alongside the rest
    #    of that machinery.
    #    Write back from the registry's authoritative owner state
    #    (whose per-partition history is coherent), not the cross-partition
    #    max-`write_num` heuristic below (per-partition write_nums are not
    #    comparable across partitions).
    if registry !== nothing
        shared_chunks = sort!(collect(keys(registry.entries)); by=_hierarchical_chunk_sort_key)
        for chunk in shared_chunks
            entry = registry.entries[chunk]
            state = entry.owner_state
            state === nothing && continue                       # never written
            # N.B. Do NOT skip when `owner_space == origin_space`. Sharing a
            # memory space does not mean the owner slot *is* the origin data:
            # `slot_is_already_in_place` requires the chunk to be owned by the
            # planning process, so any chunk homed on a remote worker gets a
            # freshly-allocated slot even when the slot's space equals the
            # chunk's own. Skipping there strands the final writes in that slot
            # and the user observes stale data. Let `_hierarchical_copy_from!`
            # decide: it elides via `arg_current` exactly when the origin really
            # does hold a current replica, matching flat `distribute_tasks!`.
            arg_ws = sort!(ArgumentWrapper[arg_w for (arg_w, _) in state.arg_owner if arg_w.arg === chunk];
                           by=arg_w->arg_w.hash)
            for arg_w in arg_ws
                _hierarchical_copy_from!(state, arg_w, _hierarchical_max_write_num(state, arg_w) + 1)
            end
        end
    end

    # 2. Private chunks: the last writer across partitions is the authoritative
    #    owner (shared chunks are skipped here, handled above).
    merged_arg_owner = Dict{ArgumentWrapper, Tuple{MemorySpace, Int, DataDepsState}}()
    for pid in 1:n_partitions
        state = partition_states[pid]
        for (arg_w, space) in state.arg_owner
            is_shared_chunk(registry, arg_w.arg) && continue
            wn = _hierarchical_max_write_num(state, arg_w)
            if !haskey(merged_arg_owner, arg_w) || wn > merged_arg_owner[arg_w][2]
                merged_arg_owner[arg_w] = (space, wn, state)
            end
        end
    end

    for arg_w in sort(collect(keys(merged_arg_owner)); by=arg_w->arg_w.hash)
        _space, wn, state = merged_arg_owner[arg_w]
        _hierarchical_copy_from!(state, arg_w, wn + 1)
    end

    # 3. Free Datadeps-allocated slots. For shared chunks, also sync on the final
    #    global writer: an intermediate owner's slot may still be read by a
    #    cross-partition boundary copy recorded in *another* partition's state,
    #    and the final writer transitively depends on all such copies.
    #    Iteration is sorted for SPMD-uniform free-task tagging/enqueue order.
    # N.B. `freed` spans *all* partitions, not just one: a buffer reachable from
    # several ainfos, or recorded in more than one partition's object cache, must
    # be freed exactly once. A double `unsafe_free!` is harmless on CPU (refcount
    # decrement) but releases device memory twice on GPU backends.
    freed = IdDict{Any,Nothing}()
    for pid in 1:n_partitions
        state = partition_states[pid]
        obj_cache = unwrap(state.ainfo_backing_chunk)
        write_num = typemax(Int) - 1

        # Map each tracked slot chunk to its ainfos, exactly as flat
        # `distribute_tasks!` does. A slot's object-cache *key* ainfo is computed
        # from the source object, so it is frequently absent from `ainfo_arg`
        # (which is keyed by destination-space ainfos). Keying the syncdep lookup
        # on the key ainfo alone therefore yields an empty syncdep set, and the
        # resulting `unsafe_free!` races the very tasks still reading that slot
        # -- freeing e.g. the copy-in buffer for an `In(::DTask)` argument out
        # from under its consumer.
        chunk_to_ainfos = IdDict{Any,Vector{AliasingWrapper}}()
        for (ainfo, remote_arg_ws) in state.ainfo_arg
            for remote_arg_w in remote_arg_ws
                push!(get!(Vector{AliasingWrapper}, chunk_to_ainfos, remote_arg_w.arg), ainfo)
            end
        end

        remote_spaces = sort!(collect(keys(obj_cache.values)); by=short_name)
        for remote_space in remote_spaces
            remote_proc = first(processors(remote_space))
            free_scope = ExactScope(remote_proc)
            space_entries = sort!(collect(obj_cache.values[remote_space]); by=p->hash(p.first))
            for (ainfo, remote_arg) in space_entries
                # Skip the user's original data; only free copies we allocated.
                is_original(obj_cache, remote_space, ainfo) && continue
                haskey(freed, remote_arg) && continue
                freed[remote_arg] = nothing
                free_syncdeps = Set{ThunkSyncdep}()
                gather_free_syncdeps!(state, remote_space, ainfo, remote_arg,
                                      write_num, chunk_to_ainfos, free_syncdeps)
                if registry !== nothing
                    orig = get(state.remote_arg_to_original, remote_arg, nothing)
                    if orig !== nothing
                        entry = get(registry.entries, orig, nothing)
                        if entry !== nothing && entry.owner_task !== nothing
                            push!(free_syncdeps, ThunkSyncdep(entry.owner_task))
                        end
                    end
                end
                Dagger.@spawn scope=free_scope syncdeps=free_syncdeps tag=datadeps_task_tag() Dagger.unsafe_free!(remote_arg)
            end
        end
    end
end
