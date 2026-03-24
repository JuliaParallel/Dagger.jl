# Hierarchical scheduling for datadeps
# Spreads scheduling work across multiple threads/workers via a 4-phase pipeline:
# Phase 1: Parallel aliasing info construction
# Phase 2: Sequential DAG construction from aliasing overlaps
# Phase 3: Data-affinity DAG partitioning
# Phase 4: Parallel local scheduling per partition -- each partition runs on its
#          own task, prepares/submits its tasks concurrently with the others,
#          and computes its *own* (partition-local) AOT schedule over just its
#          processors. This maximizes scheduling parallelism and scalability;
#          no global synchronization or global AOT pass is imposed.
#
# N.B. Concurrent task submission from many partition tasks used to intermittently
# hang in the core eager scheduler. The root cause was `Distributed.Future`,
# which is unsafe under concurrent same-process access; Dagger now backs task
# futures with `MemPool.DFuture` (a thread-safe, `DEvent`-based future), so
# parallel Phase 4 is safe. The actual task submission (`enqueue!`) is still
# serialized via `LockedEnqueueQueue`, but the (expensive) per-task
# `distribute_task!` preparation runs concurrently across partitions.

struct HierarchicalTaskInfo
    arg_w::ArgumentWrapper
    readdep::Bool
    writedep::Bool
end

struct HierarchicalTaskMeta
    pair::DTaskPair
    arg_chunk::Union{Chunk, Nothing}
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
                                   our_space::MemorySpace, task_arg_ws, write_num::Int)
    map_or_ntuple(task_arg_ws) do idx
        arg_ws = task_arg_ws[idx]
        (arg_ws.may_alias && arg_ws.inplace_move) || return
        chunk = arg_ws.arg
        entry = get(registry.entries, chunk, nothing)
        entry === nothing && return

        owner_space, owner_slot, owner_task = @lock registry.lock begin
            (entry.owner_space, entry.owner_slot, entry.owner_task)
        end
        owner_space == our_space && return # we already hold the authoritative copy

        # Register the owner's physical slot so slot/aliasing lookups in this
        # partition reuse it (rather than generating a fresh, stale copy).
        dest_args = get!(IdDict{Any,Any}, state.remote_args, owner_space)
        if !haskey(dest_args, chunk)
            dest_args[chunk] = owner_slot
            state.remote_arg_to_original[owner_slot] = chunk
        end

        map_or_ntuple(arg_ws.deps) do dep_idx
            dep = arg_ws.deps[dep_idx]
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
            return
        end
        return
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
                            our_space::MemorySpace, task::DTask, task_arg_ws)
    map_or_ntuple(task_arg_ws) do idx
        arg_ws = task_arg_ws[idx]
        (arg_ws.may_alias && arg_ws.inplace_move) || return
        chunk = arg_ws.arg
        entry = get(registry.entries, chunk, nothing)
        entry === nothing && return

        wrote = false
        map_or_ntuple(arg_ws.deps) do dep_idx
            arg_ws.deps[dep_idx].writedep && (wrote = true)
            return
        end
        wrote || return

        dest_args = get(state.remote_args, our_space, nothing)
        slot = dest_args === nothing ? nothing : get(dest_args, chunk, nothing)
        @lock registry.lock begin
            entry.owner_space = our_space
            slot !== nothing && (entry.owner_slot = slot)
            entry.owner_task = task
            entry.owner_state = state
        end
        return
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
    raw_arg_cache = IdDict{Any,Chunk}()

    nchunks = Threads.nthreads() <= 1 ? 1 : min(Threads.nthreads(), cld(n, COLLECT_ALIASED_ARGS_MIN_CHUNK))

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

    @sync for (ci, start) in enumerate(starts)
        range = start:min(start+chunk_size-1, n)
        Threads.@spawn begin
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
                                      raw_arg_cache::IdDict{Any,Chunk},
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

            # Same-region DTask arguments are not launched yet, so we cannot
            # `fetch` them here (that is what the sequential `distribute_task!`
            # path does *after* launching the producer). Record a value
            # dependency instead; aliasing of the result is handled later in
            # `distribute_task!` once the producer has been submitted.
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
                arg isa Chunk ? arg : tochunk(arg)
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
        for i in eachindex(worker_args)
            arg_to_ainfo[worker_args[i]] = results[i].second
        end
    else
        all_results_lock = ReentrantLock()
        @sync for (wid, worker_args) in by_worker
            Threads.@spawn begin
                results = if wid == myid()
                    _compute_aliasing_batch(worker_args)
                else
                    remotecall_fetch(_compute_aliasing_batch, wid, worker_args)
                end
                # Key by the *local* `arg_w` (see single-worker note above): a
                # remote worker returns deserialized `ArgumentWrapper` copies
                # that may not compare equal to our `arg_ws_vec` lookup keys.
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
    if n >= COMPUTE_ALIASING_BATCH_MIN_PARALLEL && Threads.nthreads() > 1
        Threads.@threads for i in 1:n
            arg_w = arg_ws[i]
            ainfo = AliasingWrapper(aliasing(arg_w.arg, arg_w.dep_mod))
            results[i] = arg_w => ainfo
        end
    else
        for i in 1:n
            arg_w = arg_ws[i]
            ainfo = AliasingWrapper(aliasing(arg_w.arg, arg_w.dep_mod))
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
    partition_dag(dag, task_metas, all_procs) -> (vertex_to_partition, n_partitions, partition_procs)

Phase 3: Assigns each task vertex to a partition using data-affinity. For
multi-worker setups, tasks are assigned to the worker owning the most argument
data. For single-worker multi-threaded setups, tasks are balanced across
available processors in topological order.
"""
function partition_dag(dag::SimpleDiGraph, task_metas::Vector{HierarchicalTaskMeta},
                       all_procs::Vector{<:Processor})
    n = length(task_metas)
    workers = unique(root_worker_id.(only.(memory_spaces.(all_procs))))
    n_workers = length(workers)

    procs_by_worker = Dict{Int, Vector{Processor}}()
    for proc in all_procs
        wid = root_worker_id(only(memory_spaces(proc)))
        push!(get!(Vector{Processor}, procs_by_worker, wid), proc)
    end

    multi_worker = n_workers > 1
    if multi_worker
        n_partitions = n_workers
        partition_worker = workers
    else
        n_partitions = min(length(all_procs), n)
        partition_worker = fill(first(workers), n_partitions)
    end

    vertex_to_partition = Vector{Int}(undef, n)

    if multi_worker
        worker_to_partition = Dict(w => i for (i, w) in enumerate(workers))
        default_scope = DefaultScope()
        for v in 1:n
            meta = task_metas[v]
            task_scope = @something(meta.pair.spec.options.compute_scope, meta.pair.spec.options.scope, default_scope)

            # Workers whose processors are eligible under this task's scope.
            # A non-default scope that spans multiple workers must still spread
            # work across those workers (via affinity / round-robin) -- picking
            # only the first match pins everything to worker 1 and breaks
            # multi-worker execution.
            if task_scope == default_scope
                matching = collect(1:n_partitions)
            else
                matching = Int[]
                for (pid, wid) in enumerate(workers)
                    wprocs = procs_by_worker[wid]
                    if any(proc -> proc_in_scope(proc, task_scope), wprocs)
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

            affinity = zeros(Int, n_workers)
            for dep in meta.deps
                arg_space = memory_space(dep.arg_w.arg)
                arg_wid = root_worker_id(arg_space)
                idx = get(worker_to_partition, arg_wid, 0)
                if idx > 0 && idx in matching
                    affinity[idx] += 1
                end
            end
            best_pid = matching[1]
            best_aff = -1
            for pid in matching
                if affinity[pid] > best_aff
                    best_aff = affinity[pid]
                    best_pid = pid
                end
            end
            if best_aff <= 0
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
    if multi_worker
        for pid in 1:n_partitions
            wid = partition_worker[pid]
            partition_procs[pid] = procs_by_worker[wid]
        end
    else
        for pid in 1:n_partitions
            partition_procs[pid] = copy(all_procs)
        end
    end

    return vertex_to_partition, n_partitions, partition_procs, multi_worker
end


"""
    _schedule_vertex!(v, partition_id, temp_queue, state, local_procs,
                      local_scope, dag, seen_tasks, vertex_to_partition,
                      schedule, proc_to_scope_lfu, write_num) -> write_num

Schedules a single (already topologically-ordered) task vertex `v` into its
partition's `state` via `distribute_task!`, returning the updated `write_num`.

Records cross-partition predecessors as explicit `ThunkSyncdep`s (same-partition
deps are derived from `state` by `distribute_task!`), and passes the
partition-local AOT-assigned processor when one is available and usable for this
partition (else falls back to JIT scheduling).

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
                           schedule::Dict{DTask,Processor},
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

    # Use the AOT-assigned processor when one is available and it lies within
    # this partition's processors; otherwise fall back to JIT scheduling
    # (`proc === nothing`) via `temp_queue`'s scheduler. (An AOT proc can lie
    # outside `local_procs` only in the multi-worker case, where partitioning
    # restricts each partition to one worker's processors; single-worker
    # partitions always span all procs, so the AOT assignment is always usable
    # there.)
    proc = get(schedule, task, nothing)
    if proc !== nothing && !(proc in local_procs)
        proc = nothing
    end

    return distribute_task!(temp_queue, state, local_procs, local_scope,
                            spec, task, spec.fargs,
                            proc_to_scope_lfu, write_num; proc, ownership=registry)
end

"""
    schedule_partition_full!(queue, queue_lock, partition_id, partition_verts,
                             dag, seen_tasks, local_procs,
                             vertex_to_partition, task_submitted,
                             region_uids) -> DataDepsState

Per-partition scheduling for both single-worker and multi-worker hierarchical
paths. Uses existing `distribute_task!` logic with per-partition
`DataDepsState`, `all_procs` limited to this partition's processors, and
cross-partition syncdeps from the precomputed DAG.

AOT scheduling is run *locally per partition*: a partition-local `DAGSpec` is
built from just this partition's tasks and scheduled over just `local_procs` via
`datadeps_build_schedule!`. `region_uids` (all in-region task uids) lets that
builder bail cleanly when a task depends on a same-region producer in another
partition (which it must not `fetch`). Tasks without an AOT assignment fall back
to JIT scheduling in `distribute_task!`.
"""
function schedule_partition_full!(queue::DataDepsTaskQueue,
                                  queue_lock::ReentrantLock,
                                  partition_id::Int,
                                  partition_verts::Vector{Int},
                                  dag::SimpleDiGraph,
                                  seen_tasks::Vector{DTaskPair},
                                  local_procs::Vector{<:Processor},
                                  vertex_to_partition::Vector{Int},
                                  task_submitted::Vector{Base.Event},
                                  region_uids::Set{UInt},
                                  registry::Union{SharedChunkRegistry,Nothing})
    if isempty(partition_verts) || isempty(local_procs)
        return DataDepsState(DAGSpec())
    end

    local_scope = UnionScope(map(ExactScope, local_procs))

    # N.B. Hierarchical scheduling doesn't build a global `DAGSpec` (that's
    # only used by the AOT DAG-caching path in `distribute_tasks!`), so each
    # partition just gets an empty one; `distribute_task!` never reads
    # `state.dag_spec` directly.
    state = DataDepsState(DAGSpec())
    write_num = 1
    proc_to_scope_lfu = BasicLFUCache{Processor,AbstractScope}(1024)

    vert_set = Set{Int}(partition_verts)
    topo = try
        topological_sort_by_dfs(dag)
    catch
        collect(vertices(dag))
    end
    ordered_verts = filter(v -> v in vert_set, topo)

    locked_queue = LockedEnqueueQueue(get_options(:task_queue), queue_lock)
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
    temp_queue = DataDepsTaskQueue(locked_queue; scheduler=similar(queue.scheduler))

    # Per-partition (local) AOT scheduling over just this partition's procs.
    # `partition_verts` is in ascending vertex (= submission) order.
    partition_pairs = DTaskPair[seen_tasks[v] for v in partition_verts]
    _pdag, schedule = datadeps_build_schedule!(temp_queue.scheduler, partition_pairs,
                                               local_procs, local_scope; region_uids)

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
    try
        for v in ordered_verts
            for pred_v in inneighbors(dag, v)
                if vertex_to_partition[pred_v] != partition_id
                    wait(task_submitted[pred_v])
                end
            end

            # N.B. The per-task `distribute_task!` preparation runs concurrently
            # across partitions; only the final task submission is serialized
            # (via `LockedEnqueueQueue`, `temp_queue`'s upper queue). This is
            # safe now that task futures are backed by `MemPool.DFuture` rather
            # than the concurrency-unsafe `Distributed.Future`. The
            # cross-partition `wait`s above happen before scheduling `v`, so the
            # `ThunkSyncdep`s recorded for `v` are valid.
            write_num = _schedule_vertex!(
                v, partition_id, temp_queue, state, local_procs, local_scope,
                dag, seen_tasks, vertex_to_partition, schedule,
                proc_to_scope_lfu, write_num, registry)

            notify(task_submitted[v])
        end
    finally
        for v in ordered_verts
            notify(task_submitted[v])
        end
    end

    return state
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
3. Partitioning (by worker affinity, or across local procs on one worker)
4. Parallel per-partition scheduling via `distribute_task!`

Each partition runs on its own task and computes its *own* partition-local AOT
schedule over just its processors (see `schedule_partition_full!`); there is no
global AOT pass or global synchronization, which is what allows this to scale.
AOT scheduling here therefore need not match the flat `distribute_tasks!` path
exactly. Both drivers use `distribute_task!` for argument preparation and
`DataDepsScheduler` dispatch; the old single-worker "batch enqueue with DAG
syncdeps only" path is intentionally not used (it skipped `distribute_task!` and
broke `ChunkView` / custom schedulers).
"""
function distribute_tasks_hierarchical!(queue::DataDepsTaskQueue)
    seen_tasks = queue.seen_tasks
    if isempty(seen_tasks)
        return
    end

    # Get the set of all processors
    all_procs = Processor[]
    scope = get_compute_scope()
    for w in procs()
        append!(all_procs, get_processors(OSProc(w)))
    end
    filter!(proc->proc_in_scope(proc, scope), all_procs)
    if isempty(all_procs)
        throw(Sch.SchedulingException("No processors available, try widening scope"))
    end
    all_scope = UnionScope(map(ExactScope, all_procs))

    # All in-region task uids, so each partition's local AOT-schedule builder
    # can tell same-region producers (which it must not `fetch`) apart from
    # already-materialized external values.
    region_uids = Set{UInt}(pair.task.uid for pair in seen_tasks)

    # Phase 1: Collect arguments and compute aliasing in parallel
    task_metas, unique_arg_ws = collect_aliased_args(seen_tasks)
    _lookup, ainfos_overlaps, arg_to_ainfo = build_aliasing_parallel(unique_arg_ws)

    # Phase 2: Build dependency DAG
    dag = build_dependency_dag(task_metas, arg_to_ainfo, ainfos_overlaps)

    # Phase 3: Partition the DAG
    vertex_to_partition, n_partitions, partition_procs, _multi_worker =
        partition_dag(dag, task_metas, all_procs)

    # Detect backing chunks shared across partitions in different memory spaces.
    # These need runtime ownership transfer to avoid split-brain concurrent
    # writes; `nothing` when all partitions share one space (single-worker).
    partition_space = MemorySpace[only(memory_spaces(first(pp))) for pp in partition_procs]
    registry = build_shared_chunk_registry(task_metas, vertex_to_partition, partition_space)

    # Group vertices by partition
    partitions = [Int[] for _ in 1:n_partitions]
    for v in 1:length(seen_tasks)
        pid = vertex_to_partition[v]
        push!(partitions[pid], v)
    end

    queue_lock = ReentrantLock()
    task_submitted = [Base.Event() for _ in 1:length(seen_tasks)]
    wait_all_queue = get_options(:task_queue)

    # Phase 4: parallel per-partition scheduling. Each partition runs on its own
    # task, prepares its tasks concurrently, computes its own partition-local
    # AOT schedule, and coordinates cross-partition dependencies via the
    # `task_submitted` events. Concurrent submission is safe now that task
    # futures are backed by `MemPool.DFuture` (see the header note).
    partition_states = Vector{DataDepsState}(undef, n_partitions)
    try
        @sync for pid in 1:n_partitions
            Threads.@spawn begin
                locked_queue = LockedEnqueueQueue(wait_all_queue, queue_lock)
                with_options(; task_queue=locked_queue) do
                        partition_states[pid] = schedule_partition_full!(
                            queue, queue_lock, pid, partitions[pid],
                            dag, seen_tasks,
                            partition_procs[pid], vertex_to_partition,
                            task_submitted, region_uids, registry
                        )
                end
            end
        end
    catch e
        rethrow(_unwrap_partition_exception(e))
    end

    _hierarchical_copy_from_and_free!(partition_states, n_partitions, registry)
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
    remainder, _ = compute_remainder_for_arg!(state, origin_space, arg_w, write_num)
    if remainder isa MultiRemainderAliasing
        origin_scope = UnionScope(map(ExactScope, collect(processors(origin_space)))...)
        enqueue_remainder_copy_from!(state, origin_space, arg_w, remainder, origin_scope, write_num)
    elseif remainder isa FullCopy
        origin_scope = UnionScope(map(ExactScope, collect(processors(origin_space)))...)
        enqueue_copy_from!(state, origin_space, arg_w, origin_scope, write_num)
    end
    return
end

function _hierarchical_copy_from_and_free!(partition_states::Vector{DataDepsState}, n_partitions::Int,
                                           registry::Union{SharedChunkRegistry,Nothing})
    # 1. Shared chunks: write back from the registry's authoritative owner state
    #    (whose per-partition history is coherent), not the cross-partition
    #    max-`write_num` heuristic below (per-partition write_nums are not
    #    comparable across partitions).
    if registry !== nothing
        for (chunk, entry) in registry.entries
            state = entry.owner_state
            state === nothing && continue                       # never written
            entry.owner_space == entry.origin_space && continue # already in-place at origin
            for (arg_w, _space) in state.arg_owner
                arg_w.arg === chunk || continue
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
    for pid in 1:n_partitions
        state = partition_states[pid]
        obj_cache = unwrap(state.ainfo_backing_chunk)
        write_num = typemax(Int) - 1
        for remote_space in keys(obj_cache.values)
            for (ainfo, remote_arg) in obj_cache.values[remote_space]
                if !(ainfo in obj_cache.originals)
                    remote_proc = first(processors(remote_space))
                    free_scope = ExactScope(remote_proc)
                    free_syncdeps = Set{ThunkSyncdep}()
                    if haskey(state.ainfo_arg, ainfo)
                        get_write_deps!(state, remote_space, ainfo, write_num, free_syncdeps)
                    end
                    if registry !== nothing
                        orig = get(state.remote_arg_to_original, remote_arg, nothing)
                        if orig !== nothing
                            entry = get(registry.entries, orig, nothing)
                            if entry !== nothing && entry.owner_task !== nothing
                                push!(free_syncdeps, ThunkSyncdep(entry.owner_task))
                            end
                        end
                    end
                    Dagger.@spawn scope=free_scope syncdeps=free_syncdeps Dagger.unsafe_free!(remote_arg)
                end
            end
        end
    end
end
