# Hierarchical scheduling for datadeps
# Spreads scheduling work across multiple threads/workers via a 4-phase pipeline:
# Phase 1: Parallel aliasing info construction
# Phase 2: Sequential DAG construction from aliasing overlaps
# Phase 3: Data-affinity DAG partitioning
# Phase 4: Parallel local scheduling per partition

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
end

"""
    collect_aliased_args(seen_tasks) -> (task_metas, unique_arg_ws)

Pre-scans all tasks to collect per-task dependency metadata and the set of
unique `ArgumentWrapper`s that need aliasing analysis. This mirrors the logic
in `populate_task_info!` but only inspects arguments without modifying any
scheduling state.
"""
function collect_aliased_args(seen_tasks::Vector{DTaskPair})
    supports_cache = IdDict{Any,Bool}()
    raw_arg_cache = IdDict{Any,Chunk}()
    unique_arg_ws = Dict{ArgumentWrapper, ArgumentWrapper}()
    task_metas = Vector{HierarchicalTaskMeta}(undef, length(seen_tasks))

    for (task_idx, pair) in enumerate(seen_tasks)
        spec = pair.spec
        task = pair.task
        fargs = spec.fargs

        all_deps = HierarchicalTaskInfo[]
        first_chunk = nothing
        task_may_alias = false
        task_inplace = false

        for arg_idx in (is_typed(spec) ? (1:length(fargs)) : eachindex(fargs))
            _arg = fargs[arg_idx]
            _arg_with_deps = value(_arg)

            arg_pre_unwrap, deps = unwrap_inout(_arg_with_deps)
            arg = arg_pre_unwrap isa DTask ? fetch(arg_pre_unwrap; raw=true) : arg_pre_unwrap

            may_alias = type_may_alias(typeof(arg))
            inplace_move = may_alias && get!(supports_cache, arg) do
                supports_inplace_move(arg)
            end

            if !may_alias || !inplace_move
                continue
            end

            if haskey(raw_arg_cache, arg)
                arg_chunk = raw_arg_cache[arg]
            else
                arg_chunk = arg isa Chunk ? arg : tochunk(arg)
                raw_arg_cache[arg] = arg_chunk
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
            pair, first_chunk, task_may_alias, task_inplace, all_deps
        )
    end

    return task_metas, unique_arg_ws
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
    all_results_lock = ReentrantLock()

    @sync for (wid, worker_args) in by_worker
        Threads.@spawn begin
            results = if wid == myid()
                _compute_aliasing_batch(worker_args)
            else
                remotecall_fetch(_compute_aliasing_batch, wid, worker_args)
            end
            @lock all_results_lock begin
                for (arg_w, ainfo) in results
                    arg_to_ainfo[arg_w] = ainfo
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

function _compute_aliasing_batch(arg_ws::Vector{ArgumentWrapper})
    n = length(arg_ws)
    results = Vector{Pair{ArgumentWrapper, AliasingWrapper}}(undef, n)
    if n > 1 && Threads.nthreads() > 1
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

            if task_scope != default_scope
                assigned = false
                for (pid, wid) in enumerate(workers)
                    wprocs = procs_by_worker[wid]
                    if any(proc -> proc_in_scope(proc, task_scope), wprocs)
                        vertex_to_partition[v] = pid
                        assigned = true
                        break
                    end
                end
                if !assigned
                    vertex_to_partition[v] = 1
                end
            else
                affinity = zeros(Int, n_workers)
                for dep in meta.deps
                    arg_space = memory_space(dep.arg_w.arg)
                    arg_wid = root_worker_id(arg_space)
                    idx = get(worker_to_partition, arg_wid, 0)
                    if idx > 0
                        affinity[idx] += 1
                    end
                end
                if all(==(0), affinity)
                    vertex_to_partition[v] = mod1(v, n_partitions)
                else
                    vertex_to_partition[v] = argmax(affinity)
                end
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

# Strip In/Out/InOut/Deps wrappers, returning the raw value
_unwrap_inout_value(x::In) = x.x
_unwrap_inout_value(x::Out) = x.x
_unwrap_inout_value(x::InOut) = x.x
_unwrap_inout_value(x::Deps) = x.x
_unwrap_inout_value(x) = x

"""
    schedule_partition_full!(queue, queue_lock, partition_id, partition_verts,
                             dag, seen_tasks, task_metas, local_procs,
                             vertex_to_partition, task_submitted) -> DataDepsState

Full path for multi-worker scheduling. Uses existing `distribute_task!` logic
with per-partition `DataDepsState`, `all_procs` limited to this partition's
processors, and cross-partition syncdeps.
"""
function schedule_partition_full!(queue::DataDepsTaskQueue,
                                  queue_lock::ReentrantLock,
                                  partition_id::Int,
                                  partition_verts::Vector{Int},
                                  dag::SimpleDiGraph,
                                  seen_tasks::Vector{DTaskPair},
                                  task_metas::Vector{HierarchicalTaskMeta},
                                  local_procs::Vector{<:Processor},
                                  vertex_to_partition::Vector{Int},
                                  task_submitted::Vector{Base.Event})
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

    locked_queue = LockedEnqueueQueue(get_options(:task_queue), queue_lock)
    temp_queue = DataDepsTaskQueue(locked_queue; scheduler=queue.scheduler)

    for v in ordered_verts
        for pred_v in inneighbors(dag, v)
            if vertex_to_partition[pred_v] != partition_id
                wait(task_submitted[pred_v])
            end
        end

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

        write_num = distribute_task!(temp_queue, state, local_procs, local_scope,
                                     spec, task, spec.fargs,
                                     proc_to_scope_lfu, write_num)

        notify(task_submitted[v])
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

"""
    distribute_tasks_hierarchical!(queue)

Main entry point for hierarchical scheduling. Runs the 4-phase pipeline:
1. Parallel aliasing construction
2. DAG construction
3. Partitioning
4. Parallel local scheduling (fast path for single-worker, full path for multi-worker)
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

    # Phase 1: Collect arguments and compute aliasing in parallel
    task_metas, unique_arg_ws = collect_aliased_args(seen_tasks)
    _lookup, ainfos_overlaps, arg_to_ainfo = build_aliasing_parallel(unique_arg_ws)

    # Phase 2: Build dependency DAG
    dag = build_dependency_dag(task_metas, arg_to_ainfo, ainfos_overlaps)

    # Phase 3: Partition the DAG
    vertex_to_partition, n_partitions, partition_procs, multi_worker = partition_dag(dag, task_metas, all_procs)

    # Group vertices by partition
    partitions = [Int[] for _ in 1:n_partitions]
    for v in 1:length(seen_tasks)
        pid = vertex_to_partition[v]
        push!(partitions[pid], v)
    end

    queue_lock = ReentrantLock()
    task_submitted = [Base.Event() for _ in 1:length(seen_tasks)]
    wait_all_queue = get_options(:task_queue)

    if !multi_worker
        # Single-worker fast path: submit tasks in topological order with DAG syncdeps
        local_scope = UnionScope(map(ExactScope, all_procs))

        topo = try
            topological_sort_by_dfs(dag)
        catch
            collect(1:length(seen_tasks))
        end

        batch = Vector{DTaskPair}(undef, length(topo))
        for (i, v) in enumerate(topo)
            pair = seen_tasks[v]
            spec = pair.spec
            task = pair.task

            syncdeps = Set{ThunkSyncdep}()
            for pred_v in inneighbors(dag, v)
                push!(syncdeps, ThunkSyncdep(seen_tasks[pred_v].task))
            end

            new_fargs = [Argument(arg.pos, _unwrap_inout_value(value(arg))) for arg in spec.fargs]

            new_spec = DTaskSpec(new_fargs, spec.options)
            new_spec.options.scope = local_scope
            new_spec.options.exec_scope = local_scope
            new_spec.options.occupancy = Dict(Any=>0)
            new_spec.options.syncdeps = syncdeps

            batch[i] = DTaskPair(new_spec, task)
        end
        enqueue!(wait_all_queue, batch)
    else
        # Multi-worker full path: uses distribute_task! with data movement
        partition_states = Vector{DataDepsState}(undef, n_partitions)

        @sync for pid in 1:n_partitions
            Threads.@spawn begin
                locked_queue = LockedEnqueueQueue(wait_all_queue, queue_lock)
                with_options(; task_queue=locked_queue) do
                    partition_states[pid] = schedule_partition_full!(
                        queue, queue_lock, pid, partitions[pid],
                        dag, seen_tasks, task_metas,
                        partition_procs[pid], vertex_to_partition,
                        task_submitted
                    )
                end
            end
        end

        # Copy-from and buffer freeing for multi-worker
        _hierarchical_copy_from_and_free!(partition_states, n_partitions)
    end
end

function _hierarchical_copy_from_and_free!(partition_states::Vector{DataDepsState}, n_partitions::Int)
    merged_arg_owner = Dict{ArgumentWrapper, Tuple{MemorySpace, Int, DataDepsState}}()
    for pid in 1:n_partitions
        state = partition_states[pid]
        for (arg_w, space) in state.arg_owner
            wn = 0
            if haskey(state.arg_history, arg_w)
                for entry in state.arg_history[arg_w]
                    wn = max(wn, entry.write_num)
                end
            end
            if !haskey(merged_arg_owner, arg_w) || wn > merged_arg_owner[arg_w][2]
                merged_arg_owner[arg_w] = (space, wn, state)
            end
        end
    end

    for arg_w in sort(collect(keys(merged_arg_owner)); by=arg_w->arg_w.hash)
        space, wn, state = merged_arg_owner[arg_w]
        arg = arg_w.arg
        haskey(state.arg_origin, arg) || continue
        origin_space = state.arg_origin[arg]
        write_num = wn + 1
        remainder, _ = compute_remainder_for_arg!(state, origin_space, arg_w, write_num)
        if remainder isa MultiRemainderAliasing
            origin_scope = UnionScope(map(ExactScope, collect(processors(origin_space)))...)
            enqueue_remainder_copy_from!(state, origin_space, arg_w, remainder, origin_scope, write_num)
        elseif remainder isa FullCopy
            origin_scope = UnionScope(map(ExactScope, collect(processors(origin_space)))...)
            enqueue_copy_from!(state, origin_space, arg_w, origin_scope, write_num)
        end
    end

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
                    Dagger.@spawn scope=free_scope syncdeps=free_syncdeps Dagger.unsafe_free!(remote_arg)
                end
            end
        end
    end
end
