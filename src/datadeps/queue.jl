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
                        launch_wait::Union{Bool,Nothing}=nothing)
    if !static
        throw(ArgumentError("Dynamic scheduling is no longer available"))
    end
    if traversal != :inorder
        throw(ArgumentError("Traversal order is no longer configurable, and always :inorder"))
    end
    if !aliasing
        throw(ArgumentError("Aliasing analysis is no longer optional"))
    end
    wait_all(; check_errors=true) do
        scheduler = something(scheduler, DATADEPS_SCHEDULER[], RoundRobinScheduler())
        launch_wait = something(launch_wait, DATADEPS_LAUNCH_WAIT[], false)::Bool
        if launch_wait
            result = spawn_bulk() do
                queue = DataDepsTaskQueue(get_options(:task_queue); scheduler)
                with_options(f; task_queue=queue)
                distribute_tasks!(queue)
            end
        else
            queue = DataDepsTaskQueue(get_options(:task_queue); scheduler)
            result = with_options(f; task_queue=queue)
            distribute_tasks!(queue)
        end
        return result
    end
end
const DATADEPS_SCHEDULER = ScopedValue{Union{DataDepsScheduler,Nothing}}(nothing)
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
    scope = get_compute_scope()
    for w in procs()
        append!(all_procs, get_processors(OSProc(w)))
    end
    filter!(proc->proc_in_scope(proc, scope), all_procs)
    if isempty(all_procs)
        throw(Sch.SchedulingException("No processors available, try widening scope"))
    end
    all_scope = UnionScope(map(ExactScope, all_procs))
    exec_spaces = unique(vcat(map(proc->collect(memory_spaces(proc)), all_procs)...))
    if !all(space->space isa CPURAMMemorySpace, exec_spaces) && !all(space->root_worker_id(space) == myid(), exec_spaces)
        @warn "Datadeps support for multi-GPU, multi-worker is currently broken\nPlease be prepared for incorrect results or errors" maxlog=1
    end

    # Round-robin assign tasks to processors
    upper_queue = get_options(:task_queue)

    # Start launching tasks and necessary copies
    state = DataDepsState()
    write_num = 1
    proc_to_scope_lfu = BasicLFUCache{Processor,AbstractScope}(1024)
    for pair in queue.seen_tasks
        spec = pair.spec
        task = pair.task
        write_num = distribute_task!(queue, state, all_procs, all_scope, spec, task, spec.fargs, proc_to_scope_lfu, write_num)
    end

    # Copy args from remote to local
    # N.B. We sort the keys to ensure a deterministic order for uniformity
    for arg_w in sort(collect(keys(state.arg_owner)); by=arg_w->arg_w.hash)
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
            ctx = Sch.eager_context()
            id = rand(UInt)
            @maybelog ctx timespan_start(ctx, :datadeps_copy_skip, (;id), (;))
            @maybelog ctx timespan_finish(ctx, :datadeps_copy_skip, (;id), (;thunk_id=0, from_space=origin_space, to_space=origin_space, arg_w, from_arg=arg, to_arg=arg))
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
@inline map_or_ntuple(@specialize(f), xs::NTuple{N,T}) where {N,T} = ntuple(f, Val(N))
function distribute_task!(queue::DataDepsTaskQueue, state::DataDepsState, all_procs, all_scope, spec::DTaskSpec{typed}, task::DTask, fargs, proc_to_scope_lfu, write_num::Int) where typed
    @specialize spec fargs

    if typed
        fargs::Tuple
    else
        fargs::Vector{Argument}
    end

    task_scope = @something(spec.options.compute_scope, spec.options.scope, DefaultScope())
    scheduler = queue.scheduler
    our_proc = datadeps_schedule_task(scheduler, state, all_procs, all_scope, task_scope, spec, task)
    @assert our_proc in all_procs
    our_space = only(memory_spaces(our_proc))

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

    f = spec.fargs[1]
    tid = task.uid
    # FIXME: May not be correct to move this under uniformity
    #f.value = move(default_processor(), our_proc, value(f))
    @dagdebug tid :spawn_datadeps "($(repr(value(f)))) Scheduling: $our_proc ($our_space)"

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
    new_spec.options.occupancy = Dict(Any=>0)
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

    write_num += 1

    return write_num
end
