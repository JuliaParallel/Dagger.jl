struct DataDepsTaskQueue <: AbstractTaskQueue
    # The queue above us
    upper_queue::AbstractTaskQueue
    # The set of tasks that have already been seen
    seen_tasks::Union{Vector{Pair{DTaskSpec,DTask}},Nothing}
    # The data-dependency graph of all tasks
    g::Union{SimpleDiGraph{Int},Nothing}
    # The mapping from task to graph ID
    task_to_id::Union{Dict{DTask,Int},Nothing}
    # How to traverse the dependency graph when launching tasks
    traversal::Symbol
    # Which scheduler to use to assign tasks to processors
    scheduler::Symbol

    # Whether aliasing across arguments is possible
    # The fields following only apply when aliasing==true
    aliasing::Bool

    function DataDepsTaskQueue(upper_queue;
                               traversal::Symbol=:inorder,
                               scheduler::Symbol=:naive,
                               aliasing::Bool=true)
        seen_tasks = Pair{DTaskSpec,DTask}[]
        g = SimpleDiGraph()
        task_to_id = Dict{DTask,Int}()
        return new(upper_queue, seen_tasks, g, task_to_id, traversal, scheduler,
                   aliasing)
    end
end

function enqueue!(queue::DataDepsTaskQueue, spec::Pair{DTaskSpec,DTask})
    push!(queue.seen_tasks, spec)
end
function enqueue!(queue::DataDepsTaskQueue, specs::Vector{Pair{DTaskSpec,DTask}})
    append!(queue.seen_tasks, specs)
end

"""
    spawn_datadeps(f::Base.Callable; traversal::Symbol=:inorder)

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

The keyword argument `traversal` controls the order that tasks are launched by
the scheduler, and may be set to `:bfs` or `:dfs` for Breadth-First Scheduling
or Depth-First Scheduling, respectively. All traversal orders respect the
dependencies and ordering of the launched tasks, but may provide better or
worse performance for a given set of datadeps tasks. This argument is
experimental and subject to change.
"""
function spawn_datadeps(f::Base.Callable; static::Bool=true,
                        traversal::Symbol=:inorder,
                        scheduler::Union{Symbol,Nothing}=nothing,
                        aliasing::Bool=true,
                        launch_wait::Union{Bool,Nothing}=nothing)
    if !static
        throw(ArgumentError("Dynamic scheduling is no longer available"))
    end
    wait_all(; check_errors=true) do
        scheduler = something(scheduler, DATADEPS_SCHEDULER[], :roundrobin)::Symbol
        launch_wait = something(launch_wait, DATADEPS_LAUNCH_WAIT[], false)::Bool
        if launch_wait
            result = spawn_bulk() do
                queue = DataDepsTaskQueue(get_options(:task_queue);
                                          traversal, scheduler, aliasing)
                with_options(f; task_queue=queue)
                distribute_tasks!(queue)
            end
        else
            queue = DataDepsTaskQueue(get_options(:task_queue);
                                      traversal, scheduler, aliasing)
            result = with_options(f; task_queue=queue)
            distribute_tasks!(queue)
        end
        return result
    end
end
const DATADEPS_SCHEDULER = ScopedValue{Union{Symbol,Nothing}}(nothing)
const DATADEPS_LAUNCH_WAIT = ScopedValue{Union{Bool,Nothing}}(nothing)

function distribute_tasks!(queue::DataDepsTaskQueue)
    #= TODO: Improvements to be made:
    # - Support for copying non-AbstractArray arguments
    # - Parallelize read copies
    # - Unreference unused slots
    # - Reuse memory when possible
    # - Account for differently-sized data
    =#

    # Get the set of all processors to be scheduled on
    all_procs = Processor[]
    scope = get_options(:scope, DefaultScope())
    for w in procs()
        append!(all_procs, get_processors(OSProc(w)))
    end
    filter!(proc->!isa(constrain(ExactScope(proc), scope),
                       InvalidScope),
            all_procs)
    if isempty(all_procs)
        throw(Sch.SchedulingException("No processors available, try widening scope"))
    end
    exec_spaces = unique(vcat(map(proc->collect(memory_spaces(proc)), all_procs)...))
    if !all(space->space isa CPURAMMemorySpace, exec_spaces) && !all(space->root_worker_id(space) == myid(), exec_spaces)
        @warn "Datadeps support for multi-GPU, multi-worker is currently broken\nPlease be prepared for incorrect results or errors" maxlog=1
    end

    # Round-robin assign tasks to processors
    upper_queue = get_options(:task_queue)

    traversal = queue.traversal
    if traversal == :inorder
        # As-is
        task_order = Colon()
    elseif traversal == :bfs
        # BFS
        task_order = Int[1]
        to_walk = Int[1]
        seen = Set{Int}([1])
        while !isempty(to_walk)
            # N.B. next_root has already been seen
            next_root = popfirst!(to_walk)
            for v in outneighbors(queue.g, next_root)
                if !(v in seen)
                    push!(task_order, v)
                    push!(seen, v)
                    push!(to_walk, v)
                end
            end
        end
    elseif traversal == :dfs
        # DFS (modified with backtracking)
        task_order = Int[]
        to_walk = Int[1]
        seen = Set{Int}()
        while length(task_order) < length(queue.seen_tasks) && !isempty(to_walk)
            next_root = popfirst!(to_walk)
            if !(next_root in seen)
                iv = inneighbors(queue.g, next_root)
                if all(v->v in seen, iv)
                    push!(task_order, next_root)
                    push!(seen, next_root)
                    ov = outneighbors(queue.g, next_root)
                    prepend!(to_walk, ov)
                else
                    push!(to_walk, next_root)
                end
            end
        end
    else
        throw(ArgumentError("Invalid traversal mode: $traversal"))
    end

    state = DataDepsState(queue.aliasing)
    sstate = DataDepsSchedulerState()
    for proc in all_procs
        space = only(memory_spaces(proc))
        get!(()->0, sstate.capacities, space)
        sstate.capacities[space] += 1
    end

    # Start launching tasks and necessary copies
    write_num = 1
    proc_idx = 1
    pressures = Dict{Processor,Int}()
    for (spec, task) in queue.seen_tasks[task_order]
        # Populate all task dependencies
        populate_task_info!(state, spec, task)

        scheduler = queue.scheduler
        if scheduler == :naive
            raw_args = map(arg->tochunk(last(arg)), spec.args)
            our_proc = remotecall_fetch(1, all_procs, raw_args) do all_procs, raw_args
                Sch.init_eager()
                sch_state = Sch.EAGER_STATE[]

                @lock sch_state.lock begin
                    # Calculate costs per processor and select the most optimal
                    # FIXME: This should consider any already-allocated slots,
                    # whether they are up-to-date, and if not, the cost of moving
                    # data to them
                    procs, costs = Sch.estimate_task_costs(sch_state, all_procs, nothing, raw_args)
                    return first(procs)
                end
            end
        elseif scheduler == :smart
            raw_args = map(filter(arg->haskey(state.data_locality, arg), spec.args)) do arg
                arg_chunk = tochunk(last(arg))
                # Only the owned slot is valid
                # FIXME: Track up-to-date copies and pass all of those
                return arg_chunk => data_locality[arg]
            end
            f_chunk = tochunk(spec.f)
            our_proc, task_pressure = remotecall_fetch(1, all_procs, pressures, f_chunk, raw_args) do all_procs, pressures, f, chunks_locality
                Sch.init_eager()
                sch_state = Sch.EAGER_STATE[]

                @lock sch_state.lock begin
                    tx_rate = sch_state.transfer_rate[]

                    costs = Dict{Processor,Float64}()
                    for proc in all_procs
                        # Filter out chunks that are already local
                        chunks_filt = Iterators.filter(((chunk, space)=chunk_locality)->!(proc in processors(space)), chunks_locality)

                        # Estimate network transfer costs based on data size
                        # N.B. `affinity(x)` really means "data size of `x`"
                        # N.B. We treat same-worker transfers as having zero transfer cost
                        tx_cost = Sch.impute_sum(affinity(chunk)[2] for chunk in chunks_filt)

                        # Estimate total cost to move data and get task running after currently-scheduled tasks
                        est_time_util = get(pressures, proc, UInt64(0))
                        costs[proc] = est_time_util + (tx_cost/tx_rate)
                    end

                    # Look up estimated task cost
                    sig = Sch.signature(sch_state, f, map(first, chunks_locality))
                    task_pressure = get(sch_state.signature_time_cost, sig, 1000^3)

                    # Shuffle procs around, so equally-costly procs are equally considered
                    P = randperm(length(all_procs))
                    procs = getindex.(Ref(all_procs), P)

                    # Sort by lowest cost first
                    sort!(procs, by=p->costs[p])

                    best_proc = first(procs)
                    return best_proc, task_pressure
                end
            end
            # FIXME: Pressure should be decreased by pressure of syncdeps on same processor
            pressures[our_proc] = get(pressures, our_proc, UInt64(0)) + task_pressure
        elseif scheduler == :ultra
            args = Base.mapany(spec.args) do arg
                pos, data = arg
                data, _ = unwrap_inout(data)
                if data isa DTask
                    data = fetch(data; raw=true)
                end
                return pos => tochunk(data)
            end
            f_chunk = tochunk(spec.f)
            task_time = remotecall_fetch(1, f_chunk, args) do f, args
                Sch.init_eager()
                sch_state = Sch.EAGER_STATE[]
                return @lock sch_state.lock begin
                    sig = Sch.signature(sch_state, f, args)
                    return get(sch_state.signature_time_cost, sig, 1000^3)
                end
            end

            # FIXME: Copy deps are computed eagerly
            deps = get(Set{Any}, spec.options, :syncdeps)

            # Find latest time-to-completion of all syncdeps
            deps_completed = UInt64(0)
            for dep in deps
                haskey(sstate.task_completions, dep) || continue # copy deps aren't recorded
                deps_completed = max(deps_completed, sstate.task_completions[dep])
            end

            # Find latest time-to-completion of each memory space
            # FIXME: Figure out space completions based on optimal packing
            spaces_completed = Dict{MemorySpace,UInt64}()
            for space in exec_spaces
                completed = UInt64(0)
                for (task, other_space) in sstate.assignments
                    space == other_space || continue
                    completed = max(completed, sstate.task_completions[task])
                end
                spaces_completed[space] = completed
            end

            # Choose the earliest-available memory space and processor
            # FIXME: Consider move time
            move_time = UInt64(0)
            local our_space_completed
            while true
                our_space_completed, our_space = findmin(spaces_completed)
                our_space_procs = filter(proc->proc in all_procs, processors(our_space))
                if isempty(our_space_procs)
                    delete!(spaces_completed, our_space)
                    continue
                end
                our_proc = rand(our_space_procs)
                break
            end

            sstate.task_to_spec[task] = spec
            sstate.assignments[task] = our_space
            sstate.task_completions[task] = our_space_completed + move_time + task_time
        elseif scheduler == :roundrobin
            our_proc = all_procs[proc_idx]
        else
            error("Invalid scheduler: $sched")
        end
        @assert our_proc in all_procs
        our_space = only(memory_spaces(our_proc))
        our_procs = filter(proc->proc in all_procs, collect(processors(our_space)))
        task_scope = get(spec.options, :scope, AnyScope())
        our_scope = constrain(UnionScope(map(ExactScope, our_procs)...), task_scope)
        if our_scope isa InvalidScope
            throw(Sch.SchedulingException("Scopes are not compatible: $(our_scope.x), $(our_scope.y)"))
        end

        spec.f = move(ThreadProc(myid(), 1), our_proc, spec.f)
        @dagdebug nothing :spawn_datadeps "($(repr(spec.f))) Scheduling: $our_proc ($our_space)"

        # Copy raw task arguments for analysis
        task_args = copy(spec.args)

        # Copy args from local to remote
        for (idx, (pos, arg)) in enumerate(task_args)
            # Is the data written previously or now?
            arg, deps = unwrap_inout(arg)
            arg = arg isa DTask ? fetch(arg; raw=true) : arg
            if !type_may_alias(typeof(arg))
                @dagdebug nothing :spawn_datadeps "($(repr(spec.f)))[$idx] Skipped copy-to (immutable)"
                spec.args[idx] = pos => arg
                continue
            end

            # Is the data writeable?
            if !supports_inplace_move(state, arg)
                @dagdebug nothing :spawn_datadeps "($(repr(spec.f)))[$idx] Skipped copy-to (non-writeable)"
                spec.args[idx] = pos => arg
                continue
            end

            # Is the source of truth elsewhere?
            get_or_generate_slot!(state, our_space, arg, deps)
            #record_argument_dep!(state, task, arg, deps, our_space)
            for (dep_mod, _, _) in deps
                arg_remote = state.remote_args[our_space][arg]
                ainfo_remote = aliasing(state, arg_remote, dep_mod)

                # Check if we need a remainder copy before the regular copy
                remainder = compute_remainder_for_task!(state, our_space, arg, dep_mod, ainfo_remote)
                @warn "Use simple copy-to if possible" maxlog=1
                if !(remainder isa NoAliasing)
                    enqueue_remainder_copy_to!(state, spec.f, remainder, arg, deps, idx, our_space, our_scope, task, write_num)
                    state.arg_owner[arg] = our_space
                else
                    @dagdebug nothing :spawn_datadeps "($(repr(spec.f)))[$idx] Skipped copy-to (up-to-date): $our_space"
                    #enqueue_copy_to!(state, spec.f, dep_mod, arg, arg, idx, 0, our_space, our_scope, task, write_num)
                end
            end
            spec.args[idx] = pos => state.remote_args[our_space][arg]
            state.arg_owner[arg] = our_space
        end
        write_num += 1

        # Validate that we're not accidentally performing a copy
        for (idx, (_, arg)) in enumerate(spec.args)
            _, deps = unwrap_inout(task_args[idx][2])
            # N.B. We only do this check when the argument supports in-place
            # moves, because for the moment, we are not guaranteeing updates or
            # write-back of results
            if is_writedep(arg, deps, task) && supports_inplace_move(state, arg)
                arg_space = memory_space(arg)
                @assert arg_space == our_space "($(repr(spec.f)))[$idx] Tried to pass $(typeof(arg)) from $arg_space to $our_space"
            end
        end

        # Calculate this task's syncdeps
        syncdeps = get(Set{Any}, spec.options, :syncdeps)
        for (idx, (_, arg)) in enumerate(task_args)
            arg, deps = unwrap_inout(arg)
            arg = arg isa DTask ? fetch(arg; raw=true) : arg
            type_may_alias(typeof(arg)) || continue
            supports_inplace_move(state, arg) || continue
            for (dep_mod, _, writedep) in deps
                current_arg = state.remote_args[our_space][arg]
                ainfo = aliasing(state, current_arg, dep_mod)
                if writedep
                    @dagdebug nothing :spawn_datadeps "($(repr(spec.f)))[$idx][$dep_mod] Syncing as writer"
                    get_write_deps!(state, our_space, ainfo, task, write_num, syncdeps)
                else
                    @dagdebug nothing :spawn_datadeps "($(repr(spec.f)))[$idx][$dep_mod] Syncing as reader"
                    get_read_deps!(state, our_space, ainfo, task, write_num, syncdeps)
                end
            end
        end
        @dagdebug nothing :spawn_datadeps "($(repr(spec.f))) $(length(syncdeps)) syncdeps"

        # Launch user's task
        task_scope = our_scope
        spec.options = merge(spec.options, (;syncdeps, scope=task_scope))
        enqueue!(upper_queue, spec=>task)

        # Update read/write tracking for arguments
        for (idx, (_, arg)) in enumerate(task_args)
            arg, deps = unwrap_inout(arg)
            arg = arg isa DTask ? fetch(arg; raw=true) : arg
            type_may_alias(typeof(arg)) || continue
            for (dep_mod, _, writedep) in deps
                current_arg = state.remote_args[our_space][arg]
                ainfo = aliasing(state, current_arg, dep_mod)
                if writedep
                    @dagdebug nothing :spawn_datadeps "($(repr(spec.f)))[$idx][$dep_mod] Set as owner"
                    add_writer!(state, ainfo, task, write_num)

                    # Track this as a region update for remainder computation
                    # Find the base aliasing object (the full array/object)
                    base_ainfo = aliasing(state, arg, identity)
                    if ainfo != base_ainfo
                        track_region_update!(state, ainfo, base_ainfo)
                    end
                else
                    add_reader!(state, ainfo, task, write_num)
                end
            end
        end

        # Update tracking for return value
        populate_return_info!(state, task, our_space)

        write_num += 1
        proc_idx = mod1(proc_idx + 1, length(all_procs))
    end

    # Copy args from remote to local
    # We need to replay the writes from all tasks in-order (skipping any
    # outdated write owners), to ensure that overlapping writes are applied
    # in the correct order
    for (arg, arg_space) in state.arg_owner
        for (dep_mod, _, writedep) in deps
            writedep || continue
            ainfo = aliasing(state, arg, dep_mod)
            data_local_space = state.data_origin[ainfo]
            arg_remote = state.remote_args[arg_space][arg]
            @show typeof(arg) dep_mod
            ainfo_remote = aliasing(state, arg_remote, dep_mod)
            remainder = compute_remainder_for_task!(state, data_local_space, arg, dep_mod, ainfo_remote)
            @show ainfo
            if !(remainder isa NoAliasing)
                data_local_scope = UnionScope(map(ExactScope, collect(processors(data_local_space)))...)
                enqueue_remainder_copy_from!(state, ainfo, arg, remainder, data_local_space, data_local_scope, write_num)
            end
        end
    end
    return

    # First, find the latest owners of each live ainfo
    error("Don't look at state.dependencies, look at state.arg_owner and state.arg_history")
    arg_writes = IdDict{Any,Vector{Tuple{AliasingWrapper,<:Any}}}()
    for (task, taskdeps) in state.dependencies
        for (_, writedep, ainfo, dep_mod, arg) in taskdeps
            writedep || continue
            haskey(state.data_locality, ainfo) || continue
            @assert haskey(state.ainfos_owner, ainfo) "Missing ainfo: $ainfo ($dep_mod($(typeof(arg))))"

            # Skip virtual writes from task result aliasing
            # FIXME: Make this less bad
            if arg isa DTask && dep_mod === identity && ainfo.inner isa UnknownAliasing
                continue
            end

            # Skip non-writeable arguments
            if !supports_inplace_move(state, arg)
                @dagdebug nothing :spawn_datadeps "Skipped copy-from (non-writeable)"
                continue
            end

            # Get the set of writers
            ainfo_writes = get!(Vector{Tuple{AliasingWrapper,<:Any}}, arg_writes, arg)

            #= FIXME: If we fully overlap any writer, evict them
            idxs = findall(ainfo_write->overlaps_all(ainfo, ainfo_write[1]), ainfo_writes)
            deleteat!(ainfo_writes, idxs)
            =#

            # Make ourselves the latest writer
            push!(ainfo_writes, (ainfo, dep_mod))
        end
    end

    # Then, replay the writes from each owner in-order
    # FIXME: write_num should advance across overlapping ainfo's, as
    # writes must be ordered sequentially
    for (arg, ainfo_writes) in arg_writes
        if length(ainfo_writes) > 1
            # FIXME: Remove me
            deleteat!(ainfo_writes, 1:length(ainfo_writes)-1)
        end
        for (ainfo, dep_mod) in ainfo_writes
            # Copy from the current location(s) back to original location
            data_local_space = state.data_origin[ainfo] # Where we want to copy TO (original location)
            remainder = compute_remainder_for_task!(state, ainfo)
            @show ainfo
            if !(remainder isa NoAliasing)
                data_local_scope = UnionScope(map(ExactScope, collect(processors(data_local_space)))...)
                enqueue_remainder_copy_from!(state, ainfo, arg, remainder, data_local_space, data_local_scope, write_num)
                state.arg_owner[arg] = data_local_space
                #=
                # Add copy-from operation: copy from remote space to local/original space
                @dagdebug nothing :spawn_datadeps "[$dep_mod] Enqueueing copy-from: $data_remote_space => $data_local_space"
                arg_local = get!(get!(IdDict{Any,Any}, state.remote_args, data_local_space), arg) do
                    generate_slot!(state, data_local_space, arg)
                end
                arg_remote = state.remote_args[data_remote_space][arg]
                @assert arg_remote !== arg_local
                data_local_proc = first(processors(data_local_space))
                copy_from_scope = UnionScope(map(ExactScope, collect(processors(data_local_space)))...)
                copy_from_syncdeps = Set()
                get_write_deps!(state, data_local_space, ainfo, nothing, write_num, copy_from_syncdeps)
                @dagdebug nothing :spawn_datadeps "$(length(copy_from_syncdeps)) syncdeps"

                # For copy-from, we need to consider if we need to copy remainder regions first
                # to avoid overwriting more recent partial updates
                base_ainfo = aliasing(state, arg, identity)
                if ainfo != base_ainfo && haskey(state.updated_regions, base_ainfo)
                    # Check if there are other updated regions that need to be preserved
                    other_updated_regions = filter(x -> x != ainfo, state.updated_regions[base_ainfo])
                    if !isempty(other_updated_regions)
                        @dagdebug nothing :spawn_datadeps "[$dep_mod] Need to copy other updated regions first"
                        # Create remainder copy tasks for the other regions
                        for other_ainfo in other_updated_regions
                            if haskey(state.data_locality, other_ainfo)
                                other_remote_space = state.data_locality[other_ainfo]
                                if other_remote_space != data_local_space
                                    other_copy_from = Dagger.@spawn scope=copy_from_scope syncdeps=copy_from_syncdeps meta=true Dagger.move!(other_ainfo.inner, data_local_space, other_remote_space, arg_local, state.remote_args[other_remote_space][arg])
                                    push!(copy_from_syncdeps, other_copy_from)
                                end
                            end
                        end
                    end
                end

                copy_from = Dagger.@spawn scope=copy_from_scope syncdeps=copy_from_syncdeps meta=true Dagger.move!(dep_mod, data_local_space, data_remote_space, arg_local, arg_remote)
                =#
            else
                @dagdebug nothing :spawn_datadeps "[$dep_mod] Skipped copy-from (up-to-date): $data_local_space"
            end
        end
    end

    empty!(ALIASED_OBJECTS[])
    empty!(ALIASED_OBJECT_CACHE[])
    STATE[] = nothing
end