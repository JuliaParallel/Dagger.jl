struct DataDepsTaskQueue{Scheduler<:DataDepsScheduler} <: AbstractTaskQueue
    # The queue above us
    upper_queue::AbstractTaskQueue
    # The set of tasks that have already been seen
    seen_tasks::Union{Vector{DTaskPair},Nothing}
    # The data-dependency graph of all tasks
    g::Union{SimpleDiGraph{Int},Nothing}
    # The mapping from task to graph ID
    task_to_id::Union{Dict{DTask,Int},Nothing}
    # Which scheduler to use to assign tasks to processors
    scheduler::Scheduler

    function DataDepsTaskQueue(upper_queue; scheduler::DataDepsScheduler)
        seen_tasks = DTaskPair[]
        g = SimpleDiGraph()
        task_to_id = Dict{DTask,Int}()
        return new{typeof(scheduler)}(upper_queue, seen_tasks, g, task_to_id, scheduler)
    end
end

function enqueue!(queue::DataDepsTaskQueue, pair::DTaskPair)
    push!(queue.seen_tasks, pair)
end
function enqueue!(queue::DataDepsTaskQueue, pairs::Vector{DTaskPair})
    append!(queue.seen_tasks, pairs)
end

const DATADEPS_CURRENT_TASK = TaskLocalValue{Union{DTask,Nothing}}(Returns(nothing))

# Tag for datadeps-internal tasks (copies, frees) launched outside the user
# task queue. Under uniform (MPI) execution every task needs a unique,
# rank-uniform tag for its P2P transfers; under Distributed this is unused.
datadeps_task_tag() = uniform_execution() ? UInt32(to_tag()) : nothing

"""
    spawn_datadeps(f::Base.Callable)

Constructs a "datadeps" (data dependencies) region and calls `f` within it.
Dagger tasks launched within `f` may wrap their arguments with `In`, `Out`, or
`InOut` to indicate whether the task will read, write, or read+write that
argument, respectively. These argument dependencies will be used to specify
which tasks depend on each other based on the following rules:

- Dependencies across unrelated arguments are independent; only dependencies on arguments which overlap in memory synchronize with each other
- `InOut` is the same as `In` and `Out` applied simultaneously, and synchronizes with the union of the `In` and `Out` effects
- Any two or more `In` dependencies do not synchronize with each other, and may execute in parallel
- An `Out` dependency synchronizes with any previous `In` and `Out` dependencies
- An `In` dependency synchronizes with any previous `Out` dependencies
- If unspecified, an `In` dependency is assumed

In general, the result of executing tasks following the above rules will be
equivalent to simply executing tasks sequentially and in order of submission.
Of course, if dependencies are incorrectly specified, undefined behavior (and
unexpected results) may occur.

Unlike other Dagger tasks, tasks executed within a datadeps region are allowed
to write to their arguments when annotated with `Out` or `InOut`
appropriately.

At the end of executing `f`, `spawn_datadeps` will wait for all launched tasks
to complete, rethrowing the first error, if any. The result of `f` will be
returned from `spawn_datadeps`.
"""
function spawn_datadeps(f::Base.Callable; static::Bool=true,
                        traversal::Symbol=:inorder,
                        scheduler::Union{DataDepsScheduler,Nothing}=nothing,
                        aliasing::Bool=true,
                        launch_wait::Union{Bool,Nothing}=nothing,
                        hierarchical::Union{Bool,Nothing}=nothing)
    if !static
        throw(ArgumentError("Dynamic scheduling is no longer available"))
    end
    if traversal != :inorder
        throw(ArgumentError("Traversal order is no longer configurable, and always :inorder"))
    end
    if !aliasing
        throw(ArgumentError("Aliasing analysis is no longer optional"))
    end
    # Save/restore the pin-budget throttle around the whole region: it is
    # installed by `distribute_tasks!` and must stay live until `wait_all` has
    # observed every task finish (tasks consult it from `Sch.do_task`).
    prev_throttle = DATADEPS_PIN_THROTTLE[]
    try
        wait_all(; check_errors=true) do
            scheduler = something(scheduler, DATADEPS_SCHEDULER[], RoundRobinScheduler())
            launch_wait = something(launch_wait, DATADEPS_LAUNCH_WAIT[], false)::Bool
            hierarchical = something(hierarchical, DATADEPS_HIERARCHICAL[], true)::Bool
            # N.B. Declared as a named function (rather than a closure bound to a
            # local) so it shows up by name in stacktraces and profiles, which is
            # the boundary between region setup and the whole planning pipeline.
            function run_distribute(queue)
                if hierarchical
                    distribute_tasks_hierarchical!(queue)
                else
                    distribute_tasks!(queue)
                end
            end
            if launch_wait
                result = spawn_bulk() do
                    queue = DataDepsTaskQueue(get_options(:task_queue); scheduler)
                    with_options(f; task_queue=queue)
                    run_distribute(queue)
                end
            else
                queue = DataDepsTaskQueue(get_options(:task_queue); scheduler)
                result = with_options(f; task_queue=queue)
                run_distribute(queue)
            end
            return result
        end
    finally
        DATADEPS_PIN_THROTTLE[] = prev_throttle
    end
end
const DATADEPS_SCHEDULER = ScopedValue{Union{DataDepsScheduler,Nothing}}(nothing)
const DATADEPS_LAUNCH_WAIT = ScopedValue{Union{Bool,Nothing}}(nothing)
const DATADEPS_HIERARCHICAL = ScopedValue{Union{Bool,Nothing}}(nothing)

# Current task uid, propagated into `tochunk` so uniform-execution backends
# (MPIExt) can derive deterministic, rank-agreed handle IDs. Core datadeps sets
# this during planning/execution; MPIExt reads it. 0 means "no task in scope".
const DATADEPS_THUNK_ID = ScopedValue{Int64}(0)

# Deterministic, rank-agreed task tag for uniform execution. Only invoked when
# `uniform_execution()` holds (i.e. under MPIExt), which provides the method.
function to_tag end

function distribute_tasks!(queue::DataDepsTaskQueue)
    #= TODO: Improvements to be made:
    # - Support for copying non-AbstractArray arguments
    # - Parallelize read copies
    # - Unreference unused slots
    # - Reuse memory when possible
    # - Account for differently-sized data
    =#

    # Get the set of all processors to be scheduled on
    accel = current_acceleration()
    accel_procs = filter(procs(Dagger.Sch.eager_context())) do proc
        Dagger.accel_matches_proc(accel, proc)
    end
    all_procs = unique(vcat([collect(Dagger.get_processors(gp)) for gp in accel_procs]...))
    select_processors_uniform!(all_procs, accel)
    scope = get_compute_scope()
    filter!(proc->proc_in_scope(proc, scope), all_procs)
    if isempty(all_procs)
        throw(Sch.SchedulingException("No processors available, try widening scope"))
    end
    if uniform_execution(accel)
        for proc in all_procs
            check_uniform(proc)
        end
    end
    all_scope = UnionScope(map(ExactScope, all_procs))
    exec_spaces = unique(vcat(map(proc->collect(memory_spaces(proc)), all_procs)...))

    # Round-robin assign tasks to processors
    upper_queue = get_options(:task_queue)

    # Build the memory-aware tracker (a no-op unless enabled). The pre-pass that
    # computes per-task slot sizes and last-use indices is scheduler-independent,
    # so this works on top of whichever `datadeps_schedule_task` the user chose.
    tracker = MEMORY_AWARE_CONFIG.enabled ?
        build_memory_tracker(MEMORY_AWARE_CONFIG, queue.seen_tasks) : nothing

    # Start launching tasks and necessary copies
    state = DataDepsState()
    if tracker !== nothing
        state.mem_tracker[] = tracker
        tracker.active[] = true
        # Establish the shared per-worker logical-memory budget so the Datadeps
        # allocator and MemPool's own allocator can't both claim all of RAM.
        apply_shared_mem_limits!(exec_spaces, tracker)
    end
    # Install the region's pin-budget throttle (concurrency cap) when swapping is
    # enabled: it bounds the simultaneously-pinned footprint of in-flight tasks
    # to the resident budget so MemPool can always evict to swap in the next
    # task. Cleared by `spawn_datadeps` once all tasks complete (it must outlive
    # this function, which only *launches* tasks). See `PinThrottle`.
    let budget = datadeps_pin_budget()
        DATADEPS_PIN_THROTTLE[] = budget === nothing ? nothing : PinThrottle(budget)
    end
    write_num = 1
    proc_to_scope_lfu = BasicLFUCache{Processor,AbstractScope}(1024)
    for (task_idx, pair) in enumerate(queue.seen_tasks)
        spec = pair.spec
        task = pair.task
        write_num = distribute_task!(queue, state, all_procs, all_scope, spec, task, spec.fargs, proc_to_scope_lfu, write_num; tracker, task_idx)
    end

    # Disable mid-region reclaim: from here on, slot generation (for write-back
    # copy sources) must not be reclaimed out from under the final copies.
    tracker !== nothing && (tracker.active[] = false)

    # Write back any *spilled written* tiles incrementally (reload -> copy to
    # origin -> free, one at a time), so the region-end write-back does not
    # reload the whole written footprint at once. After this, these tiles' owners
    # are their origins, so the loop below treats them as up-to-date.
    if tracker !== nothing
        write_num = drain_spilled_writebacks!(state, tracker, write_num)
    end

    # Copy args from remote to local
    # N.B. We sort the keys to ensure a deterministic order for uniformity
    check_uniform(length(state.arg_owner))
    for arg_w in sort(collect(keys(state.arg_owner)); by=arg_w->arg_w.hash)
        check_uniform(arg_w)
        arg = arg_w.arg
        origin_space = state.arg_origin[arg]
        # When the origin still holds a fully-current replica (the argument was
        # only read, or copies merely propagated it), the write-back is elided.
        # This is only safe here at region end: mid-region, the copy tasks also
        # serialize readers against later writers, so they must not be skipped.
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
            @dagdebug nothing :spawn_datadeps "Skipped copy-from (up-to-date): $origin_space"
            ctx = Sch.eager_context()
            id = rand(UInt)
            @maybelog ctx timespan_start(ctx, :datadeps_copy_skip, (;id), (;))
            @maybelog ctx timespan_finish(ctx, :datadeps_copy_skip, (;id), (;thunk_id=0, from_space=origin_space, to_space=origin_space, arg_w, from_arg=arg, to_arg=arg))
        end
    end
    write_num += 1

    # Free all Datadeps-allocated buffers.
    #
    # The object cache holds, per space, one entry per `key` ainfo mapping to
    # its backing buffer. Because related ainfos (e.g. overlapping `view`s and
    # their shared parent) are unified under a single `key`, iterating the cache
    # yields each distinct lowest-level buffer exactly once -- no duplication,
    # regardless of array wrappers. A buffer is a Datadeps-allocated copy (safe
    # to free) at every space except the `key`'s source space, where it is the
    # user's original data (`is_original`).
    #
    # N.B. Do not mpi_cleanup_tid the planning-time uid keys here. Eager uid ==
    # Sch thunk id, so planning (DATADEPS_THUNK_ID=>uid) and later in-task tochunk share
    # the same next_ref_sub_id! counter. Resetting it before execution would
    # reissue (tid, sub_id) pairs while planning-time MPIRefs are still live,
    # colliding ArgumentWrapper / Chunk identity hashes. Counters are reclaimed
    # in wait_all after the region finishes (thunks generate no further IDs).
    obj_cache = unwrap(state.ainfo_backing_chunk)
    # Map each tracked slot chunk to its ainfos, to compute free syncdeps.
    chunk_to_ainfos = IdDict{Any,Vector{AliasingWrapper}}()
    for (ainfo, remote_arg_ws) in state.ainfo_arg
        for remote_arg_w in remote_arg_ws
            push!(get!(Vector{AliasingWrapper}, chunk_to_ainfos, remote_arg_w.arg), ainfo)
        end
    end
    # Reuse the memory-aware tracker's `freed` set when present, so buffers it
    # already reclaimed mid-region are not freed again here.
    freed = tracker !== nothing ? tracker.freed : IdDict{Any,Nothing}()
    for remote_space in keys(obj_cache.values)
        for (ainfo, remote_arg) in obj_cache.values[remote_space]
            # Skip the user's original data; only free copies we allocated.
            is_original(obj_cache, remote_space, ainfo) && continue
            emit_slot_free!(state, remote_space, ainfo, remote_arg, write_num, chunk_to_ainfos, freed)
        end
    end

    # Free any memory-aware slots that bypass the object cache. Chunks reloaded
    # from a disk spill are created directly (not via `move_rewrap`), so they are
    # not in `obj_cache`; the tracker still holds them as resident. Already-freed
    # buffers are skipped via `freed`.
    if tracker !== nothing
        for (remote_space, resident) in tracker.resident
            for (_, (remote_arg, _)) in resident
                # No `AliasingWrapper` is available for a tracker-resident slot
                # (`resident` is keyed by the pre-pass's `UInt64` slot key, not an
                # ainfo); `nothing` is safe here since `gather_free_syncdeps!`
                # only consults `key_ainfo` on the MPI-only non-inspectable-data
                # fallback, which this Distributed-only spill path never hits.
                emit_slot_free!(state, remote_space, nothing, remote_arg, write_num, chunk_to_ainfos, freed)
            end
        end
        # Remove any spill temp files left on disk (read-only copies never reused;
        # written copies were reloaded by the write-back loop above).
        cleanup_spilled_files!(tracker)
    end

    # NOTE: originals are no longer pinned for the region (see
    # `populate_task_info!`), so there is nothing to unpin here.

    # Restore the shared logical-memory budget to unbounded now that the region
    # has been fully planned (all slot allocations/spills are scheduled).
    tracker !== nothing && restore_shared_mem_limits!(exec_spaces)
end

"""
    emit_slot_free!(state, space, key_ainfo, remote_arg, write_num, chunk_to_ainfos, freed) -> Union{DTask,Nothing}

Spawn a task that frees the Datadeps-allocated buffer `remote_arg` in `space`,
gated (via [`gather_free_syncdeps!`](@ref)) on every task that reads or writes
it. `key_ainfo` is the buffer's object-cache key ainfo, used as a fallback when
`remote_arg`'s own aliasing can't be inspected locally (see
[`gather_free_syncdeps!`](@ref)). Returns the spawned free task, or `nothing` if
it was already freed (tracked in `freed`). Used both for the end-of-region
cleanup and for mid-region reclamation under memory pressure.
"""
function emit_slot_free!(state::DataDepsState, space::MemorySpace, key_ainfo, remote_arg, write_num::Int, chunk_to_ainfos, freed)
    haskey(freed, remote_arg) && return nothing
    freed[remote_arg] = nothing
    free_proc = first(processors(space))
    free_scope = ExactScope(free_proc)
    free_syncdeps = Set{ThunkSyncdep}()
    gather_free_syncdeps!(state, space, key_ainfo, remote_arg, write_num, chunk_to_ainfos, free_syncdeps)
    # Capture the slot in the task closure rather than passing it as a dependency
    # argument: as an argument Dagger would `move` it (for a `ChunkView` this
    # resolves to a *different* `DRef` than the one we pinned/track), breaking the
    # free + unpin. Captured, the exact `Chunk` is freed on its owning worker.
    return Dagger.@spawn scope=free_scope syncdeps=free_syncdeps (()->(Dagger.datadeps_free!(remote_arg); nothing))()
end
struct DataDepsTaskDependency
    arg_w::ArgumentWrapper
    readdep::Bool
    writedep::Bool
end
DataDepsTaskDependency(arg, dep) =
    DataDepsTaskDependency(ArgumentWrapper(arg, dep[1]), dep[2], dep[3])
struct DataDepsTaskArgument
    arg
    pos::ArgPosition
    may_alias::Bool
    inplace_move::Bool
    deps::Vector{DataDepsTaskDependency}
end
struct TypedDataDepsTaskArgument{T,N}
    arg::T
    pos::ArgPosition
    may_alias::Bool
    inplace_move::Bool
    deps::NTuple{N,DataDepsTaskDependency}
end
map_or_ntuple(f, xs::Vector) = map(f, 1:length(xs))
# N.B. Accept any `Tuple` (typed specs produce heterogeneous tuples of
# `TypedArgument{T}`, not a homogeneous `NTuple{N,T}`).
@inline map_or_ntuple(@specialize(f), xs::Tuple) = ntuple(f, Val(length(xs)))
function distribute_task!(queue::DataDepsTaskQueue, state::DataDepsState, all_procs, all_scope, spec::DTaskSpec{typed}, task::DTask, fargs, proc_to_scope_lfu, write_num::Int; ownership=nothing, tracker::Union{DatadepsMemoryTracker,Nothing}=nothing, task_idx::Int=0) where typed
    @specialize spec fargs

    if typed
        fargs::Tuple
    else
        fargs::Vector{Argument}
    end

    DATADEPS_CURRENT_TASK[] = task

    task_scope = @something(spec.options.compute_scope, spec.options.scope, DefaultScope())
    scheduler = queue.scheduler
    our_proc = datadeps_schedule_task(scheduler, state, all_procs, all_scope, task_scope, spec, task)

    # Memory-aware post-scheduling adjustment: the scheduler has proposed a
    # processor (and thus space); optionally move the task to a less-loaded
    # in-scope space when the proposal would exceed that space's budget. The
    # actual budget enforcement (synchronous reclaim of dead copies) happens
    # later, inside `get_or_generate_slot!`, where slots are allocated.
    if tracker !== nothing
        tracker.current_idx[] = task_idx
        candidate_procs = Processor[p for p in all_procs if proc_in_scope(p, task_scope)]
        our_proc = memory_aware_reassign!(tracker, our_proc, candidate_procs, task_idx)
    end

    @assert our_proc in all_procs
    our_space = only(memory_spaces(our_proc))
    check_uniform(our_proc)
    check_uniform(our_space)

    # Find the scope for this task (and its copies)
    task_scope = @something(spec.options.compute_scope, spec.options.scope, DefaultScope())
    if task_scope == all_scope
        # Optimize for the common case, cache the proc=>scope mapping
        our_scope = get!(proc_to_scope_lfu, our_proc) do
            our_procs = filter(proc->proc in all_procs, collect(processors(our_space)))
            return constrain(UnionScope(map(ExactScope, our_procs)...), all_scope)
        end
    else
        # Use the provided scope and constrain it to the available processors
        our_procs = filter(proc->proc in all_procs, collect(processors(our_space)))
        our_scope = constrain(UnionScope(map(ExactScope, our_procs)...), task_scope)
    end
    if our_scope isa InvalidScope
        throw(Sch.SchedulingException("Scopes are not compatible: $(our_scope.x), $(our_scope.y)"))
    end

    tid = task.uid
    # N.B. `with_value` returns a fresh argument rather than mutating; this is
    # required for typed specs, whose `TypedArgument`s disallow `setproperty!`.
    f = with_value(spec.fargs[1], move(default_processor(), our_proc, value(spec.fargs[1])))
    @dagdebug tid :spawn_datadeps "($(repr(value(f)))) Scheduling: $our_proc ($our_space)"

    # Copy raw task arguments for analysis
    # N.B. Used later for checking dependencies. The moved function argument
    # (`f`) replaces the original at position 1.
    task_args = map_or_ntuple(idx->copy(idx == 1 ? f : spec.fargs[idx]), spec.fargs)

    # Populate all task dependencies
    task_arg_ws = populate_task_info!(state, task_args, spec, task)

    # Truncate the history for each argument
    map_or_ntuple(task_arg_ws) do idx
        arg_ws = task_arg_ws[idx]
        map_or_ntuple(arg_ws.deps) do dep_idx
            dep = arg_ws.deps[dep_idx]
            truncate_history!(state, dep.arg_w)
        end
        return
    end

    # Hierarchical scheduling only: for shared backing chunks whose current
    # version was produced by another partition, seed this partition's state so
    # the copy-to below pulls a fresh whole-chunk copy from the true owner (and
    # syncs on its producer). No-op on the flat path (`ownership === nothing`).
    # N.B. Currently always `nothing` -- see the dead-code note on
    # "Cross-partition chunk ownership" in `datadeps/hierarchical.jl`.
    if ownership !== nothing
        _sync_incoming_ownership!(state, ownership, our_space, task_arg_ws, write_num)
    end

    # Copy args from local to remote
    remote_args = map_or_ntuple(task_arg_ws) do idx
        arg_ws = task_arg_ws[idx]
        arg = arg_ws.arg
        pos = raw_position(arg_ws.pos)

        # Is the data written previously or now?
        if !arg_ws.may_alias
            @dagdebug tid :spawn_datadeps "($(repr(value(f))))[$(idx-1)] Skipped copy-to (immutable)"
            return arg
        end

        # Is the data writeable?
        if !arg_ws.inplace_move
            @dagdebug tid :spawn_datadeps "($(repr(value(f))))[$(idx-1)] Skipped copy-to (non-writeable)"
            return arg
        end

        # Is the source of truth elsewhere?
        arg_remote = get_or_generate_slot!(state, our_space, arg)
        map_or_ntuple(arg_ws.deps) do dep_idx
            dep = arg_ws.deps[dep_idx]
            arg_w = dep.arg_w
            dep_mod = arg_w.dep_mod
            remainder, _ = compute_remainder_for_arg!(state, our_space, arg_w, write_num)
            if remainder isa MultiRemainderAliasing
                enqueue_remainder_copy_to!(state, our_space, arg_w, remainder, value(f), idx, our_scope, task, write_num)
            elseif remainder isa FullCopy
                enqueue_copy_to!(state, our_space, arg_w, value(f), idx, our_scope, task, write_num)
            else
                @assert remainder isa NoAliasing "Expected NoAliasing, got $(typeof(remainder))"
                @dagdebug tid :spawn_datadeps "($(repr(value(f))))[$(idx-1)][$dep_mod] Skipped copy-to (up-to-date): $our_space"
            end
        end
        return arg_remote
    end
    write_num += 1

    # Validate that we're not accidentally performing a copy
    map_or_ntuple(task_arg_ws) do idx
        arg_ws = task_arg_ws[idx]
        arg = remote_args[idx]

        # Get the dependencies again as (dep_mod, readdep, writedep)
        deps = map_or_ntuple(arg_ws.deps) do dep_idx
            dep = arg_ws.deps[dep_idx]
            (dep.arg_w.dep_mod, dep.readdep, dep.writedep)
        end

        # Check that any mutable and written arguments are already in the correct space
        # N.B. We only do this check when the argument supports in-place
        # moves, because for the moment, we are not guaranteeing updates or
        # write-back of results
        if is_writedep(arg, deps, task) && arg_ws.may_alias && arg_ws.inplace_move
            arg_space = memory_space(arg)
            @assert arg_space == our_space "($(repr(value(f))))[$(idx-1)] Tried to pass $(typeof(arg)) from $arg_space to $our_space"
        end
    end

    # Calculate this task's syncdeps
    if spec.options.syncdeps === nothing
        spec.options.syncdeps = Set{ThunkSyncdep}()
    end
    if spec.options.tag === nothing && uniform_execution()
       spec.options.tag = to_tag()
    end
    syncdeps = spec.options.syncdeps
    map_or_ntuple(task_arg_ws) do idx
        arg_ws = task_arg_ws[idx]
        arg = arg_ws.arg
        arg_ws.may_alias || return
        arg_ws.inplace_move || return
        map_or_ntuple(arg_ws.deps) do dep_idx
            dep = arg_ws.deps[dep_idx]
            arg_w = dep.arg_w
            ainfo = aliasing!(state, our_space, arg_w)
            dep_mod = arg_w.dep_mod
            if dep.writedep
                @dagdebug tid :spawn_datadeps "($(repr(value(f))))[$(idx-1)][$dep_mod] Syncing as writer"
                get_write_deps!(state, our_space, ainfo, write_num, syncdeps)
            else
                @dagdebug tid :spawn_datadeps "($(repr(value(f))))[$(idx-1)][$dep_mod] Syncing as reader"
                get_read_deps!(state, our_space, ainfo, write_num, syncdeps)
            end
        end
        return
    end
    @dagdebug tid :spawn_datadeps "($(repr(value(f)))) Task has $(length(syncdeps)) syncdeps"

    # Launch user's task
    new_fargs = map_or_ntuple(task_arg_ws) do idx
        if is_typed(spec)
            return TypedArgument(task_arg_ws[idx].pos, remote_args[idx])
        else
            return Argument(task_arg_ws[idx].pos, remote_args[idx])
        end
    end
    new_spec = DTaskSpec(new_fargs, spec.options)
    new_spec.options.scope = our_scope
    new_spec.options.exec_scope = our_scope
    if uniform_execution()
        new_spec.options.occupancy = Dict(Any=>0)
    end
    ctx = Sch.eager_context()
    @maybelog ctx timespan_start(ctx, :datadeps_execute, (;thunk_id=task.uid), (;))
    enqueue!(queue.upper_queue, DTaskPair(new_spec, task))
    @maybelog ctx timespan_finish(ctx, :datadeps_execute, (;thunk_id=task.uid), (;space=our_space, deps=task_arg_ws, args=remote_args))

    # Update read/write tracking for arguments
    map_or_ntuple(task_arg_ws) do idx
        arg_ws = task_arg_ws[idx]
        arg = arg_ws.arg
        arg_ws.may_alias || return
        arg_ws.inplace_move || return
        for dep in arg_ws.deps
            arg_w = dep.arg_w
            ainfo = aliasing!(state, our_space, arg_w)
            dep_mod = arg_w.dep_mod
            if dep.writedep
                @dagdebug tid :spawn_datadeps "($(repr(value(f))))[$(idx-1)][$dep_mod] Task set as writer"
                add_writer!(state, arg_w, our_space, ainfo, task, write_num)
            else
                add_reader!(state, arg_w, our_space, ainfo, task, write_num)
            end
        end
        return
    end

    # Hierarchical scheduling only: publish this task as the new authoritative
    # owner of each shared backing chunk it writes, so later cross-partition
    # consumers pull the up-to-date version from here.
    if ownership !== nothing
        _commit_ownership!(state, ownership, our_space, task, task_arg_ws)
    end

    write_num += 1

    DATADEPS_CURRENT_TASK[] = nothing

    return write_num
end
