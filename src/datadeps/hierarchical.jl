# Concurrent per-partition task submission requires MemPool.DFuture-backed
# task futures; the older Distributed.Future was not safe for concurrent
# same-process access.

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
    # Same-region unlaunched DTask arguments -- cannot fetch during pre-scan;
    # become hard DAG edges.
    value_deps::Vector{Int}
end

# Cross-partition ownership registry for chunks written from >=2 partitions in
# distinct memory spaces. Without this, per-partition copies diverge and the
# final copy-back silently loses all but one. Deadlock-free: one lock plus the
# DAG's `task_submitted` handshake orders the reads against the ownership
# commit; the lock only guards memory-safety across different chunks.
mutable struct ChunkOwnership
    owner_space::MemorySpace
    owner_slot::Any
    owner_task::Union{DTask,Nothing}
    owner_state::Union{DataDepsState,Nothing}
    const origin_space::MemorySpace
end

struct SharedChunkRegistry
    entries::IdDict{Any,ChunkOwnership}
    lock::ReentrantLock
end
SharedChunkRegistry() = SharedChunkRegistry(IdDict{Any,ChunkOwnership}(), ReentrantLock())

is_shared_chunk(::Nothing, @nospecialize(chunk)) = false
is_shared_chunk(reg::SharedChunkRegistry, @nospecialize(chunk)) = haskey(reg.entries, chunk)

"""
    build_shared_chunk_registry(task_metas, vertex_to_partition, partition_space)
        -> SharedChunkRegistry or nothing

Registry of chunks accessed from >=2 distinct memory spaces. Returns
`nothing` when all partitions share one space.
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

Seed `state` so the copy-to phase pulls a fresh whole-chunk copy from the
true cross-partition owner (with a syncdep on the producing task). No-op for
private chunks or when we already own.
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
        owner_space == our_space && return

        dest_args = get!(IdDict{Any,Any}, state.remote_args, owner_space)
        if !haskey(dest_args, chunk)
            dest_args[chunk] = owner_slot
            state.remote_arg_to_original[owner_slot] = chunk
        end

        map_or_ntuple(arg_ws.deps) do dep_idx
            dep = arg_ws.deps[dep_idx]
            arg_w = dep.arg_w
            # Clear local history so compute_remainder_for_arg! takes the
            # FullCopy path from the owner instead of a partial remainder.
            state.arg_owner[arg_w] = owner_space
            haskey(state.arg_history, arg_w) && empty!(state.arg_history[arg_w])
            src_ainfo = aliasing!(state, owner_space, arg_w)
            if owner_task !== nothing
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

Publish `task` as the new authoritative owner of each shared chunk it writes.
Visibility is guaranteed by the DAG's `task_submitted` handshake; the lock
only guards concurrent commits for other chunks.
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

# Fewer tasks than this: sequential pre-scan wins over the thread-spawn cost.
const COLLECT_ALIASED_ARGS_MIN_CHUNK = 256

# f() runs outside the lock so distinct keys make concurrent progress; racers
# on the same key drop their result and adopt whichever value landed first.
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

Pre-scan every task's arguments to build `task_metas` and the unique
`ArgumentWrapper` set for aliasing analysis. Parallelized across threads
with shared, lock-protected caches for `Chunk`-wrapping and
`supports_inplace_move` so every raw argument has one canonical `Chunk`.
"""
function collect_aliased_args(seen_tasks::Vector{DTaskPair})
    n = length(seen_tasks)
    task_metas = Vector{HierarchicalTaskMeta}(undef, n)
    n == 0 && return task_metas, Dict{ArgumentWrapper,ArgumentWrapper}()

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

            # Unlaunched same-region DTask: record a value dep; aliasing is
            # deferred to `distribute_task!` after the producer submits.
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
    build_aliasing_parallel(unique_arg_ws)
        -> (lookup, ainfos_overlaps, arg_to_ainfo)

Compute `AliasingWrapper` for every unique `ArgumentWrapper` in parallel and
reduce into a single `AliasingLookup` with overlap information.
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
        wid, worker_args = only(by_worker)
        results = wid == myid() ? _compute_aliasing_batch(worker_args) :
                                   remotecall_fetch(_compute_aliasing_batch, wid, worker_args)
        # Key by the local arg_w: remote-returned ArgumentWrappers are
        # deserialized copies. _compute_aliasing_batch preserves input order.
        @assert length(results) == length(worker_args) """build_aliasing_parallel: \
_compute_aliasing_batch input/output length mismatch (single-worker branch). \
wid=$wid myid=$(myid()) length(worker_args)=$(length(worker_args)) \
length(results)=$(length(results)) \
worker_args_hashes=$([w.hash for w in worker_args]) \
result_first_hashes=$([r.first.hash for r in results])"""
        for i in eachindex(worker_args)
            arg_to_ainfo[worker_args[i]] = results[i].second
        end
    else
        all_results_lock = ReentrantLock()
        @sync for (wid, worker_args) in by_worker
            Threads.@spawn begin
                # `local` is REQUIRED: the single-worker branch above binds
                # `results` in the enclosing scope, and without this every
                # spawned closure would race on that shared cell.
                local results
                results = if wid == myid()
                    _compute_aliasing_batch(worker_args)
                else
                    remotecall_fetch(_compute_aliasing_batch, wid, worker_args)
                end
                @assert length(results) == length(worker_args) """build_aliasing_parallel: \
_compute_aliasing_batch input/output length mismatch (multi-worker branch). \
wid=$wid myid=$(myid()) length(worker_args)=$(length(worker_args)) \
length(results)=$(length(results)) \
worker_args_hashes=$([w.hash for w in worker_args]) \
result_first_hashes=$([r.first.hash for r in results])"""
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

# Fewer args than this: `Threads.@threads` overhead exceeds the aliasing()
# cost.
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

Build the data-dependency DAG from pre-computed aliasing overlaps. Same
WAW/RAW/WAR rules as `get_write_deps!` / `get_read_deps!`.
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
    partition_dag(dag, task_metas, all_procs)
        -> (vertex_to_partition, n_partitions, partition_procs, multi_worker)

Data-affinity partitioning. Multi-worker: bin by the worker owning the most
arg data. Single-worker: load-balance across procs in topological order.
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

            # Non-default scopes spanning multiple workers must spread work
            # (via affinity / round-robin); first-match would pin to worker 1.
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
    partition_by_assigned_worker(seen_tasks, schedule, task_metas, all_procs)
        -> (vertex_to_partition, n_partitions, partition_procs, multi_worker)

Scheduler-first partitioning: bin each task by its AOT-chosen processor's
worker, preserving the scheduler's whole-DAG placement across the partition
boundary. Unscheduled tasks (past a `dag_add_task!` bail point) fall back to
data-affinity. Only workers with >=1 task get a partition; ordered ascending
by worker id.
"""
function partition_by_assigned_worker(seen_tasks::Vector{DTaskPair},
                                      schedule::Dict{DTask,Processor},
                                      task_metas::Vector{HierarchicalTaskMeta},
                                      all_procs::Vector{<:Processor})
    n = length(seen_tasks)

    procs_by_worker = Dict{Int, Vector{Processor}}()
    for proc in all_procs
        wid = root_worker_id(only(memory_spaces(proc)))
        push!(get!(Vector{Processor}, procs_by_worker, wid), proc)
    end
    known_workers = Set{Int}(keys(procs_by_worker))

    task_worker = Vector{Int}(undef, n)
    for v in 1:n
        task = seen_tasks[v].task
        proc = get(schedule, task, nothing)
        if proc !== nothing
            wid = root_worker_id(only(memory_spaces(proc)))
            if wid in known_workers
                task_worker[v] = wid
                continue
            end
        end
        # Fallback: affinity by arg data ownership.
        affinity = Dict{Int,Int}()
        for dep in task_metas[v].deps
            arg_wid = root_worker_id(memory_space(dep.arg_w.arg))
            arg_wid in known_workers || continue
            affinity[arg_wid] = get(affinity, arg_wid, 0) + 1
        end
        if isempty(affinity)
            task_worker[v] = first(sort!(collect(known_workers)))
        else
            best_wid = 0
            best_aff = -1
            for wid in sort!(collect(keys(affinity)))
                aff = affinity[wid]
                if aff > best_aff
                    best_aff = aff
                    best_wid = wid
                end
            end
            task_worker[v] = best_wid
        end
    end

    used_workers = Int[]
    seen_wids = Set{Int}()
    for wid in task_worker
        if !(wid in seen_wids)
            push!(used_workers, wid)
            push!(seen_wids, wid)
        end
    end
    sort!(used_workers)
    worker_to_partition = Dict(wid => i for (i, wid) in enumerate(used_workers))
    n_partitions = length(used_workers)

    vertex_to_partition = Vector{Int}(undef, n)
    for v in 1:n
        vertex_to_partition[v] = worker_to_partition[task_worker[v]]
    end

    partition_procs = Vector{Vector{Processor}}(undef, n_partitions)
    for (pid, wid) in enumerate(used_workers)
        partition_procs[pid] = procs_by_worker[wid]
    end

    multi_worker = n_partitions > 1
    return vertex_to_partition, n_partitions, partition_procs, multi_worker
end


"""
    _schedule_vertex!(v, partition_id, temp_queue, state, local_procs,
                      local_scope, dag, seen_tasks, vertex_to_partition,
                      schedule, proc_to_scope_lfu, write_num) -> write_num

Schedule a topologically-ordered vertex into its partition via
`distribute_task!`. Cross-partition predecessors become explicit
`ThunkSyncdep`s; AOT proc is passed through when it lies in `local_procs`,
else JIT. Callers must ensure every predecessor (any partition) has already
been submitted.
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

    # AOT-assigned proc, or JIT fallback if it's outside this partition.
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
                             region_uids, registry;
                             precomputed_schedule=nothing)
        -> (DataDepsState, Dict{DTask,Processor})

Per-partition scheduling. Runs partition-local AOT on `local_procs` (with
`region_uids` for the cross-partition bail), or filters
`precomputed_schedule` when supplied. Returns state and this partition's
task-to-processor mapping.
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
                                  registry::Union{SharedChunkRegistry,Nothing};
                                  precomputed_schedule::Union{Dict{DTask,Processor},Nothing}=nothing)
    if isempty(partition_verts) || isempty(local_procs)
        return DataDepsState(DAGSpec()), Dict{DTask,Processor}()
    end

    local_scope = UnionScope(map(ExactScope, local_procs))

    # No global `DAGSpec` under hierarchical; `distribute_task!` never reads
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
    # Fresh scheduler shard per partition: sharing `queue.scheduler` would
    # race on mutable state (e.g. `RoundRobinScheduler.proc_idx`) and, worse,
    # advance a counter meant for one `local_procs` against another's --
    # BoundsError under multi-worker.
    temp_queue = DataDepsTaskQueue(locked_queue; scheduler=similar(queue.scheduler))

    partition_pairs = DTaskPair[seen_tasks[v] for v in partition_verts]

    if precomputed_schedule === nothing
        _pdag, schedule = datadeps_build_schedule!(temp_queue.scheduler, partition_pairs,
                                                   local_procs, local_scope; region_uids)
    else
        schedule = Dict{DTask,Processor}()
        for pair in partition_pairs
            proc = get(precomputed_schedule, pair.task, nothing)
            if proc !== nothing
                schedule[pair.task] = proc
            end
        end
    end

    # `finally` notifies all our events even on exception: without it,
    # sibling partitions can wedge on `wait(task_submitted[pred_v])`
    # forever, and the enclosing `@sync` never surfaces our exception.
    try
        for v in ordered_verts
            for pred_v in inneighbors(dag, v)
                if vertex_to_partition[pred_v] != partition_id
                    wait(task_submitted[pred_v])
                end
            end

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

    return state, schedule
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

"""
    _hierarchical_dag_spec_and_cache_lookup(scheduler, seen_tasks)
        -> (dag_spec::DAGSpec, precomputed::Union{Dict{DTask,Processor},Nothing})

Build the full-region `DAGSpec` and check `scheduler`'s cache. On a hit,
return the recovered task-to-processor mapping; on miss, return `nothing`.
`dag_add_task!` may bail on within-DAG `DTask` args, in which case the
returned `DAGSpec` covers only the prefix that succeeded.
"""
function _hierarchical_dag_spec_and_cache_lookup(scheduler::DataDepsScheduler,
                                                   seen_tasks::Vector{DTaskPair})
    dag_spec = DAGSpec()
    dummy_state = DataDepsState(dag_spec)
    for (spec, task) in seen_tasks
        if !dag_add_task!(dag_spec, dummy_state, spec, task)
            break
        end
    end
    isempty(dag_spec) && return dag_spec, nothing

    for (other_spec, spec_schedule) in datadeps_schedule_cache(scheduler)
        if datadeps_dag_equivalent(scheduler, dag_spec, other_spec)
            @dagdebug nothing :spawn_datadeps "Hierarchical DAG cache hit"
            schedule = Dict{DTask,Processor}()
            uid_to_task = Dict{UInt,DTask}()
            for pair in seen_tasks
                uid_to_task[pair.task.uid] = pair.task
            end
            for (id, proc) in spec_schedule.id_to_proc
                uid = dag_spec.id_to_uid[id]
                task = get(uid_to_task, uid, nothing)
                task === nothing && continue
                schedule[task] = proc
            end
            return dag_spec, schedule
        end
    end
    return dag_spec, nothing
end

"""
    _hierarchical_persist_schedule!(scheduler, dag_spec, partition_schedules)

Merge the per-partition schedules and cache them keyed by `dag_spec`.
No-op on empty spec or empty schedules.
"""
function _hierarchical_persist_schedule!(scheduler::DataDepsScheduler,
                                          dag_spec::DAGSpec,
                                          partition_schedules::Vector{Dict{DTask,Processor}})
    isempty(dag_spec) && return
    spec_schedule = DAGSpecSchedule()
    for pschedule in partition_schedules
        for (task, proc) in pschedule
            haskey(dag_spec.uid_to_id, task.uid) || continue
            id = dag_spec.uid_to_id[task.uid]
            spec_schedule.id_to_proc[id] = proc
        end
    end
    isempty(spec_schedule.id_to_proc) && return
    push!(datadeps_schedule_cache(scheduler), dag_spec => spec_schedule)
    return
end

# Unwrap TaskFailedException/CompositeException so callers see the root
# scheduling error rather than the @sync/@spawn envelope.
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

Hierarchical scheduling entry point: parallel aliasing construction, DAG
build, partitioning, then parallel per-partition scheduling via
`distribute_task!`.
"""
function distribute_tasks_hierarchical!(queue::DataDepsTaskQueue)
    seen_tasks = queue.seen_tasks
    if isempty(seen_tasks)
        return
    end

    dag_spec, precomputed_schedule =
        _hierarchical_dag_spec_and_cache_lookup(queue.scheduler, seen_tasks)

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

    # In-region task uids let each partition's AOT builder skip same-region
    # producers rather than `fetch` them.
    region_uids = Set{UInt}(pair.task.uid for pair in seen_tasks)

    task_metas, unique_arg_ws = collect_aliased_args(seen_tasks)
    _lookup, ainfos_overlaps, arg_to_ainfo = build_aliasing_parallel(unique_arg_ws)

    dag = build_dependency_dag(task_metas, arg_to_ainfo, ainfos_overlaps)

    # Scheduler-first partitioning is only meaningful when arg data spans
    # multiple workers; otherwise affinity already picks the right worker.
    data_workers = Set{Int}()
    for meta in task_metas
        for dep in meta.deps
            push!(data_workers, root_worker_id(memory_space(dep.arg_w.arg)))
        end
    end
    data_is_distributed = length(data_workers) >= 2

    # Call `datadeps_schedule_dag_aot!` directly, not `datadeps_build_schedule!`:
    # the latter also writes the scheduler's per-type cache, which under
    # hierarchical is owned by `_hierarchical_persist_schedule!` below.
    cache_hit = precomputed_schedule !== nothing
    if !cache_hit && !isempty(dag_spec) && data_is_distributed
        trial = Dict{DTask,Processor}()
        datadeps_schedule_dag_aot!(queue.scheduler, trial, dag_spec, all_procs, all_scope)
        if !isempty(trial)
            precomputed_schedule = trial
        end
    end

    if precomputed_schedule !== nothing && data_is_distributed
        vertex_to_partition, n_partitions, partition_procs, _multi_worker =
            partition_by_assigned_worker(seen_tasks, precomputed_schedule, task_metas, all_procs)
    else
        vertex_to_partition, n_partitions, partition_procs, _multi_worker =
            partition_dag(dag, task_metas, all_procs)
    end

    partition_space = MemorySpace[only(memory_spaces(first(pp))) for pp in partition_procs]
    registry = build_shared_chunk_registry(task_metas, vertex_to_partition, partition_space)

    partitions = [Int[] for _ in 1:n_partitions]
    for v in 1:length(seen_tasks)
        pid = vertex_to_partition[v]
        push!(partitions[pid], v)
    end

    queue_lock = ReentrantLock()
    task_submitted = [Base.Event() for _ in 1:length(seen_tasks)]
    wait_all_queue = get_options(:task_queue)

    partition_states = Vector{DataDepsState}(undef, n_partitions)
    partition_schedules = Vector{Dict{DTask,Processor}}(undef, n_partitions)
    try
        @sync for pid in 1:n_partitions
            Threads.@spawn begin
                locked_queue = LockedEnqueueQueue(wait_all_queue, queue_lock)
                with_options(; task_queue=locked_queue) do
                        partition_states[pid], partition_schedules[pid] =
                            schedule_partition_full!(
                                queue, queue_lock, pid, partitions[pid],
                                dag, seen_tasks,
                                partition_procs[pid], vertex_to_partition,
                                task_submitted, region_uids, registry;
                                precomputed_schedule
                            )
                end
            end
        end
    catch e
        rethrow(_unwrap_partition_exception(e))
    end

    _hierarchical_copy_from_and_free!(partition_states, n_partitions, registry)

    if !cache_hit
        _hierarchical_persist_schedule!(queue.scheduler, dag_spec, partition_schedules)
    end
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
    # Shared chunks: use the registry's authoritative owner. Per-partition
    # write_nums are not comparable across partitions.
    if registry !== nothing
        for (chunk, entry) in registry.entries
            state = entry.owner_state
            state === nothing && continue
            entry.owner_space == entry.origin_space && continue
            for (arg_w, _space) in state.arg_owner
                arg_w.arg === chunk || continue
                _hierarchical_copy_from!(state, arg_w, _hierarchical_max_write_num(state, arg_w) + 1)
            end
        end
    end

    # Private chunks: last-writer-wins.
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

    # Free datadeps slots. For shared chunks, sync on the final global writer:
    # a stale owner slot may still be read via boundary copies recorded in
    # another partition's state.
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
