
const TAG_WAITING = Base.Lockable(Ref{UInt32}(1))
function to_tag()
    intask = Dagger.in_task()
    opts = Dagger.get_options()
    if intask
        return Dagger.get_tls().task_spec.options.tag::UInt32
    end
    lock(TAG_WAITING) do counter_ref
        tag = counter_ref[]
        counter_ref[] = tag + 1 > MPI.tag_ub() ? 1 : tag + 1
        return tag
    end
end

struct DataDepsTaskQueue <: AbstractTaskQueue
    # The queue above us
    upper_queue::AbstractTaskQueue
    # The set of tasks that have already been seen
    seen_tasks::Union{Vector{DTaskPair},Nothing}
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
        seen_tasks = DTaskPair[]
        g = SimpleDiGraph()
        task_to_id = Dict{DTask,Int}()
        return new(upper_queue, seen_tasks, g, task_to_id, traversal, scheduler,
                   aliasing)
    end
end

function enqueue!(queue::DataDepsTaskQueue, pair::DTaskPair)
    push!(queue.seen_tasks, pair)
end
function enqueue!(queue::DataDepsTaskQueue, pairs::Vector{DTaskPair})
    append!(queue.seen_tasks, pairs)
end

const DATADEPS_CURRENT_TASK = TaskLocalValue{Union{DTask,Nothing}}(Returns(nothing))

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
        DATADEPS_CURRENT_TASK[] = nothing
        return result
    end
end
const DATADEPS_SCHEDULER = ScopedValue{Union{Symbol,Nothing}}(nothing)
const DATADEPS_LAUNCH_WAIT = ScopedValue{Union{Bool,Nothing}}(nothing)

@warn "Don't blindly set occupancy=0, only do for MPI" maxlog=1
function distribute_tasks!(queue::DataDepsTaskQueue)
    #= TODO: Improvements to be made:
    # - Support for copying non-AbstractArray arguments
    # - Parallelize read copies
    # - Unreference unused slots
    # - Reuse memory when possible
    # - Account for differently-sized data
    =#

    # Get the set of all processors to be scheduled on
    scope = get_compute_scope()
    accel = current_acceleration()
    accel_procs = filter(procs(Dagger.Sch.eager_context())) do proc
        Dagger.accel_matches_proc(accel, proc)
    end
    all_procs = unique(vcat([collect(Dagger.get_processors(gp)) for gp in accel_procs]...))
    # FIXME: This is an unreliable way to ensure processor uniformity
    sort!(all_procs, by=short_name)
    filter!(proc->proc_in_scope(proc, scope), all_procs)
    if isempty(all_procs)
        throw(Sch.SchedulingException("No processors available, try widening scope"))
    end
    exec_spaces = unique(vcat(map(proc->collect(memory_spaces(proc)), all_procs)...))
    #=if !all(space->space isa CPURAMMemorySpace, exec_spaces) && !all(space->root_worker_id(space) == myid(), exec_spaces)
        @warn "Datadeps support for multi-GPU, multi-worker is currently broken\nPlease be prepared for incorrect results or errors" maxlog=1
    end=#
    for proc in all_procs
        check_uniform(proc)
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
    #pressures = Dict{Processor,Int}()
    proc_to_scope_lfu = BasicLFUCache{Processor,AbstractScope}(1024)
    for pair in queue.seen_tasks[task_order]
        spec = pair.spec
        task = pair.task
        write_num, proc_idx = distribute_task!(queue, state, all_procs, spec, task, spec.fargs, proc_to_scope_lfu, write_num, proc_idx)
    end

    # Copy args from remote to local
    # N.B. We sort the keys to ensure a deterministic order for uniformity
    check_uniform(length(state.arg_owner))
    for arg_w in sort(collect(keys(state.arg_owner)); by=arg_w->arg_w.hash)
        check_uniform(arg_w)
        arg = arg_w.arg
        origin_space = state.arg_origin[arg]
        remainder, _ = compute_remainder_for_arg!(state, origin_space, arg_w, write_num)
        if remainder isa MultiRemainderAliasing
            origin_scope = UnionScope(map(ExactScope, collect(processors(origin_space)))...)
            enqueue_remainder_copy_from!(state, origin_space, arg_w, remainder, origin_scope, write_num)
        elseif remainder isa FullCopy
            origin_scope = UnionScope(map(ExactScope, collect(processors(origin_space)))...)
            enqueue_copy_from!(state, origin_space, arg_w, origin_scope, write_num)
        else
            @assert remainder isa NoAliasing "Expected NoAliasing, got $(typeof(remainder))"
            @dagdebug nothing :spawn_datadeps "Skipped copy-from (up-to-date): $origin_space"
        end
    end
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
map_or_ntuple(f, xs::Tuple) = ntuple(f, length(xs))
function distribute_task!(queue::DataDepsTaskQueue, state::DataDepsState, all_procs, spec::DTaskSpec{typed}, task::DTask, fargs, proc_to_scope_lfu, write_num::Int, proc_idx::Int) where typed
    @specialize spec fargs

    DATADEPS_CURRENT_TASK[] = task

    if typed
        fargs::Tuple
    else
        fargs::Vector{Argument}
    end

    scheduler = queue.scheduler
    if scheduler == :naive
        raw_args = map(arg->tochunk(value(arg)), spec.fargs)
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
        raw_args = map(filter(arg->haskey(state.data_locality, value(arg)), spec.fargs)) do arg
            arg_chunk = tochunk(value(arg))
            # Only the owned slot is valid
            # FIXME: Track up-to-date copies and pass all of those
            return arg_chunk => data_locality[arg]
        end
        f_chunk = tochunk(value(spec.fargs[1]))
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
        args = Base.mapany(spec.fargs) do arg
            pos, data = arg
            data, _ = unwrap_inout(data)
            if data isa DTask
                data = fetch(data; move_value=false, unwrap=false)
            end
            return pos => tochunk(data)
        end
        f_chunk = tochunk(value(spec.fargs[1]))
        task_time = remotecall_fetch(1, f_chunk, args) do f, args
            Sch.init_eager()
            sch_state = Sch.EAGER_STATE[]
            return @lock sch_state.lock begin
                sig = Sch.signature(sch_state, f, args)
                return get(sch_state.signature_time_cost, sig, 1000^3)
            end
        end

        # FIXME: Copy deps are computed eagerly
        deps = @something(spec.options.syncdeps, Set{Any}())

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

    # Find the scope for this task (and its copies)
    task_scope = @something(spec.options.compute_scope, spec.options.scope, DefaultScope())
    if task_scope == scope
        # Optimize for the common case, cache the proc=>scope mapping
        our_scope = get!(proc_to_scope_lfu, our_proc) do
            our_procs = filter(proc->proc in all_procs, collect(processors(our_space)))
            return constrain(UnionScope(map(ExactScope, our_procs)...), scope)
        end
    else
        # Use the provided scope and constrain it to the available processors
        our_procs = filter(proc->proc in all_procs, collect(processors(our_space)))
        our_scope = constrain(UnionScope(map(ExactScope, our_procs)...), task_scope)
    end
    if our_scope isa InvalidScope
        throw(Sch.SchedulingException("Scopes are not compatible: $(our_scope.x), $(our_scope.y)"))
    end
    check_uniform(our_proc)
    check_uniform(our_space)

    f = spec.fargs[1]
    # FIXME: May not be correct to move this under uniformity
    #f.value = move(default_processor(), our_proc, value(f))
    @dagdebug nothing :spawn_datadeps "($(repr(value(f)))) Scheduling: $our_proc ($our_space)"

    # Copy raw task arguments for analysis
    # N.B. Used later for checking dependencies
    task_args = map_or_ntuple(idx->copy(spec.fargs[idx]), spec.fargs)

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

    # Copy args from local to remote
    remote_args = map_or_ntuple(task_arg_ws) do idx
        arg_ws = task_arg_ws[idx]
        arg = arg_ws.arg
        pos = raw_position(arg_ws.pos)

        # Is the data written previously or now?
        if !arg_ws.may_alias
            @dagdebug nothing :spawn_datadeps "($(repr(value(f))))[$(idx-1)] Skipped copy-to (immutable)"
            return arg
        end

        # Is the data writeable?
        if !arg_ws.inplace_move
            @dagdebug nothing :spawn_datadeps "($(repr(value(f))))[$(idx-1)] Skipped copy-to (non-writeable)"
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
                @dagdebug nothing :spawn_datadeps "($(repr(value(f))))[$(idx-1)][$dep_mod] Skipped copy-to (up-to-date): $our_space"
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
        if is_writedep(arg, deps, task) && arg_ws.may_alias && arg_ws.inplace_move
            arg_space = memory_space(arg)
            @assert arg_space == our_space "($(repr(value(f))))[$(idx-1)] Tried to pass $(typeof(arg)) from $arg_space to $our_space"
        end
    end

    # Calculate this task's syncdeps
    if spec.options.syncdeps === nothing
        spec.options.syncdeps = Set{Any}()
    end
    if spec.options.tag === nothing
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
                @dagdebug nothing :spawn_datadeps "($(repr(value(f))))[$(idx-1)][$dep_mod] Syncing as writer"
                get_write_deps!(state, our_space, ainfo, write_num, syncdeps)
            else
                @dagdebug nothing :spawn_datadeps "($(repr(value(f))))[$(idx-1)][$dep_mod] Syncing as reader"
                get_read_deps!(state, our_space, ainfo, write_num, syncdeps)
            end
        end
        return
    end
    @dagdebug nothing :spawn_datadeps "($(repr(value(f)))) Task has $(length(syncdeps)) syncdeps"

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
    new_spec.options.occupancy = Dict(Any=>0)
    enqueue!(queue.upper_queue, DTaskPair(new_spec, task))

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
                @dagdebug nothing :spawn_datadeps "($(repr(value(f))))[$(idx-1)][$dep_mod] Task set as writer"
                add_writer!(state, arg_w, our_space, ainfo, task, write_num)
            else
                add_reader!(state, arg_w, our_space, ainfo, task, write_num)
            end
        end
        return
    end

    write_num += 1
    proc_idx = mod1(proc_idx + 1, length(all_procs))

    return write_num, proc_idx
end
