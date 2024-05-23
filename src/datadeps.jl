import Graphs: SimpleDiGraph, add_edge!, add_vertex!, inneighbors, outneighbors, nv

export In, Out, InOut, spawn_datadeps

"Specifies a read-only dependency."
struct In{T}
    x::T
end
"Specifies a write-only dependency."
struct Out{T}
    x::T
end
"Specifies a read-write dependency."
struct InOut{T}
    x::T
end

struct DataDepsTaskQueue <: AbstractTaskQueue
    # The queue above us
    upper_queue::AbstractTaskQueue
    # The mapping of unique objects to previously-launched tasks,
    # and their data dependency on the object (read, write)
    deps::IdDict{Any, Vector{Pair{Tuple{Bool,Bool}, DTask}}}

    # Whether to analyze the DAG statically or eagerly
    # The fields following only apply when static==true
    static::Bool
    # The set of tasks that have already been seen
    seen_tasks::Union{Vector{Pair{DTaskSpec,DTask}},Nothing}
    # The data-dependency graph of all tasks
    g::Union{SimpleDiGraph{Int},Nothing}
    # The mapping from task to graph ID
    task_to_id::Union{Dict{DTask,Int},Nothing}
    # How to traverse the dependency graph when launching tasks
    traversal::Symbol

    function DataDepsTaskQueue(upper_queue; static::Bool=true,
                               traversal::Symbol=:inorder)
        deps = IdDict{Any, Vector{Pair{Tuple{Bool,Bool}, DTask}}}()
        if static
            seen_tasks = Pair{DTaskSpec,DTask}[]
            g = SimpleDiGraph()
            task_to_id = Dict{DTask,Int}()
        else
            seen_tasks = nothing
            g = nothing
            task_to_id = nothing
        end
        return new(upper_queue, deps,
                   static, seen_tasks, g, task_to_id, traversal)
    end
end

function _enqueue!(queue::DataDepsTaskQueue, fullspec::Pair{DTaskSpec,DTask})
    # If static, record this task and its edges in the graph
    if queue.static
        g = queue.g
        task_to_id = queue.task_to_id
    end

    spec, task = fullspec
    if queue.static
        add_vertex!(g)
        task_to_id[task] = our_task_id = nv(g)
    else
        opts = spec.options
        syncdeps = get(Set{Any}, opts, :syncdeps)
        scope = get(DefaultScope, opts, :scope)
        worker_scope = ProcessScope(myid())
        new_scope = constrain(scope, worker_scope)
        if new_scope isa InvalidScope
            throw(SchedulingException("Scopes are not compatible: $scope vs $worker_scope"))
        end
        scope = new_scope
    end
    deps_to_add = Vector{Pair{Any, Tuple{Bool,Bool}}}()
    for (idx, (pos, arg)) in enumerate(spec.args)
        readdep = false
        writedep = false
        if arg isa In
            readdep = true
            arg = arg.x
        elseif arg isa Out
            writedep = true
            arg = arg.x
        elseif arg isa InOut
            readdep = true
            writedep = true
            arg = arg.x
        else
            readdep = true
        end
        spec.args[idx] = pos => arg
        arg_data = arg isa DTask ? fetch(arg; raw=true) : arg

        push!(deps_to_add, arg_data => (readdep, writedep))

        if !haskey(queue.deps, arg_data)
            continue
        end
        argdeps = queue.deps[arg_data]::Vector{Pair{Tuple{Bool,Bool}, DTask}}
        if readdep
            # When you have an in dependency, sync with the previous out
            for ((other_readdep::Bool, other_writedep::Bool),
                 other_task::DTask) in argdeps
                if other_writedep
                    if queue.static
                        other_task_id = task_to_id[other_task]
                        add_edge!(g, other_task_id, our_task_id)
                    else
                        push!(syncdeps, other_task)
                    end
                end
            end
        end
        if writedep
            # When you have an out dependency, sync with the previous in or out
            for ((other_readdep::Bool, other_writedep::Bool),
                other_task::DTask) in argdeps
                if other_readdep || other_writedep
                    if queue.static
                        other_task_id = task_to_id[other_task]
                        add_edge!(g, other_task_id, our_task_id)
                    else
                        push!(syncdeps, other_task)
                    end
                end
           end
        end
    end
    for (arg_data, (readdep, writedep)) in deps_to_add
        argdeps = get!(queue.deps, arg_data) do
            Vector{Pair{Tuple{Bool,Bool}, DTask}}()
        end
        push!(argdeps, (readdep, writedep) => task)
    end

    if !queue.static
        spec.options = merge(opts, (;syncdeps, scope))
    end
end
function enqueue!(queue::DataDepsTaskQueue, spec::Pair{DTaskSpec,DTask})
    _enqueue!(queue, spec)
    if queue.static
        push!(queue.seen_tasks, spec)
    else
        enqueue!(queue.upper_queue, spec)
    end
end
function enqueue!(queue::DataDepsTaskQueue, specs::Vector{Pair{DTaskSpec,DTask}})
    for spec in specs
        _enqueue!(queue, spec)
    end
    if queue.static
        append!(queue.seen_tasks, specs)
    else
        enqueue!(queue.upper_queue, specs)
    end
end

function distribute_tasks!(queue::DataDepsTaskQueue)
    #= TODO: We currently assume:
    # - All data is local to `myid()`
    # - All data is approximately the same size
    # - Copies won't occur automatically (TODO: GPUs)
    # - All tasks take approximately the same amount of time to execute
    # - All data will be written-back at the end of the computation
    # - All tracked data supports `copyto!` (FIXME)
    =#

    # TODO: Don't do round-robin - instead, color the graph

    # Determine which arguments could be written to, and thus need tracking
    arg_has_writedep = IdDict{Any,Bool}(arg=>any(argdep->argdep[1][2], argdeps) for (arg, argdeps) in queue.deps)
    has_writedep(arg) = haskey(arg_has_writedep, arg) && arg_has_writedep[arg]
    function has_writedep(arg, task::DTask)
        haskey(arg_has_writedep, arg) || return false
        any_writedep = false
        for ((readdep, writedep), other_task) in queue.deps[arg]
            any_writedep |= writedep
            if task === other_task
                return any_writedep
            end
        end
        error("Task isn't in argdeps set")
    end
    function is_writedep(arg, task::DTask)
        haskey(arg_has_writedep, arg) || return false
        for ((readdep, writedep), other_task) in queue.deps[arg]
            if task === other_task
                return writedep
            end
        end
        error("Task isn't in argdeps set")
    end

    # Get the set of all processors to be scheduled on
    all_procs = Processor[]
    all_spaces_set = Set{MemorySpace}()
    scope = get_options(:scope, DefaultScope())
    for w in procs()
        append!(all_procs, get_processors(OSProc(w)))
    end
    filter!(proc->!isa(constrain(ExactScope(proc), scope),
                       InvalidScope),
            all_procs)
    if any(proc->!isa(proc, ThreadProc), all_procs)
        @warn "Non-CPU execution not yet supported by `spawn_datadeps`; non-CPU processors will be ignored" maxlog=1
        filter!(proc->!isa(proc, ThreadProc), all_procs)
    end
    for proc in all_procs
        for space in memory_spaces(proc)
            push!(all_spaces_set, space)
        end
    end
    all_spaces = collect(all_spaces_set)

    # Track original and current data locations
    # We track data => space
    data_origin = IdDict{Any,MemorySpace}(data=>memory_space(data) for data in keys(queue.deps))
    data_locality = IdDict{Any,MemorySpace}(data=>memory_space(data) for data in keys(queue.deps))

    # Track writers ("owners") and readers
    args_owner = IdDict{Any,Union{DTask,Nothing}}(arg=>nothing for arg in keys(queue.deps))
    args_readers = IdDict{Any,Vector{DTask}}(arg=>DTask[] for arg in keys(queue.deps))
    function get_write_deps!(arg, syncdeps)
        haskey(args_owner, arg) || return
        if (owner = args_owner[arg]) !== nothing
            push!(syncdeps, owner)
        end
        for reader in args_readers[arg]
            push!(syncdeps, reader)
        end
    end
    function get_read_deps!(arg, syncdeps)
        haskey(args_owner, arg) || return
        if (owner = args_owner[arg]) !== nothing
            push!(syncdeps, owner)
        end
    end
    function add_writer!(arg, task)
        args_owner[arg] = task
        empty!(args_readers[arg])
        # Not necessary, but conceptually it's true
        # It also means we don't need an extra `add_reader!` call
        push!(args_readers[arg], task)
    end
    function add_reader!(arg, task)
        push!(args_readers[arg], task)
    end

    # Make a copy of each piece of data on each worker
    # memory_space => {arg => copy_of_arg}
    remote_args = Dict{MemorySpace,IdDict{Any,Any}}(space=>IdDict{Any,Any}() for space in all_spaces)
    for space in all_spaces
        this_space_args = remote_args[space] = IdDict{Any,Any}()
        for data in keys(queue.deps)
            has_writedep(data) || continue
            data_space = memory_space(data)
            if data_space == space
                this_space_args[data] = Dagger.tochunk(data)
            else
                # TODO: Can't use @mutable with custom Chunk scope
                #remote_args[w][data] = Dagger.@mutable worker=w copy(data)
                to_proc = first(processors(space))
                from_proc = first(processors(data_space))
                w = only(unique(map(get_parent, collect(processors(space))))).pid
                this_space_args[data] = remotecall_fetch(w, from_proc, to_proc, data) do from_proc, to_proc, data
                    data_raw = fetch(data)
                    data_converted = move(from_proc, to_proc, data_raw)
                    return Dagger.tochunk(data_converted)
                end
            end
        end
    end

    # Round-robin assign tasks to processors
    proc_idx = 1
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

    for (spec, task) in queue.seen_tasks[task_order]
        our_proc = all_procs[proc_idx]
        our_space = only(memory_spaces(our_proc))

        # Spawn copies before user's task, as necessary
        @dagdebug nothing :spawn_datadeps "($(spec.f)) Scheduling: $our_proc ($our_space)"
        task_args = map(((pos, arg)=_arg,)->pos=>(arg isa DTask ? fetch(arg; raw=true) : arg), copy(spec.args))

        # Copy args from local to remote
        for (idx, (pos, arg)) in enumerate(task_args)
            # Is the data written previously or now?
            if !has_writedep(arg, task)
                @dagdebug nothing :spawn_datadeps "($(spec.f))[$idx] Skipped copy-to (unwritten)"
                continue
            end

            data_space = data_locality[arg]

            # Is the source of truth elsewhere?
            arg_remote = remote_args[our_space][arg]
            nonlocal = our_space != data_space
            if nonlocal
                # Add copy-to operation (depends on latest owner of arg)
                @dagdebug nothing :spawn_datadeps "($(spec.f))[$idx] Enqueueing copy-to: $data_space => $our_space"
                arg_local = remote_args[data_space][arg]
                copy_to_scope = ExactScope(our_proc)
                copy_to_syncdeps = Set{Any}()
                get_write_deps!(arg, copy_to_syncdeps)
                @dagdebug nothing :spawn_datadeps "($(spec.f))[$idx] $(length(copy_to_syncdeps)) syncdeps"
                # TODO: copy_to = Dagger.@spawn scope=copy_to_scope syncdeps=copy_to_syncdeps Dagger.move!(our_space, data_space, arg_remote, arg_local)
                copy_to = Dagger.@spawn scope=copy_to_scope syncdeps=copy_to_syncdeps copyto!(arg_remote, arg_local)
                add_writer!(arg, copy_to)

                data_locality[arg] = our_space
            else
                @dagdebug nothing :spawn_datadeps "($(spec.f))[$idx] Skipped copy-to (local): $data_space"
            end
            spec.args[idx] = pos => arg_remote
        end

        # Validate that we're not accidentally performing a copy
        for (idx, (_, arg)) in enumerate(spec.args)
            if is_writedep(arg, task)
                arg_space = memory_space(arg)
                @assert arg_space == our_space "($(spec.f))[$idx] Tried to pass $(typeof(arg)) from $arg_space to $our_space"
            end
        end

        # Launch user's task
        spec.f = move(ThreadProc(myid(), 1), our_proc, spec.f)
        syncdeps = get(Set{Any}, spec.options, :syncdeps)
        for (_, arg) in task_args
            if is_writedep(arg, task)
                get_write_deps!(arg, syncdeps)
            else
                get_read_deps!(arg, syncdeps)
            end
        end
        @dagdebug nothing :spawn_datadeps "($(spec.f)) $(length(syncdeps)) syncdeps"
        task_scope = Dagger.ExactScope(our_proc)
        spec.options = merge(spec.options, (;syncdeps, scope=task_scope))
        enqueue!(upper_queue, spec=>task)
        for (idx, (_, arg)) in enumerate(task_args)
            if is_writedep(arg, task)
                @dagdebug nothing :spawn_datadeps "($(spec.f))[$idx] Set as owner"
                add_writer!(arg, task)
            else
                add_reader!(arg, task)
            end
        end

        # Select the next processor to use
        proc_idx = mod1(proc_idx+1, length(all_procs))
    end

    # Copy args from remote to local
    for arg in keys(queue.deps)
        # Is the data previously written?
        if !has_writedep(arg)
            @dagdebug nothing :spawn_datadeps "Skipped copy-from (unwritten)"
        end

        # Is the source of truth elsewhere?
        data_remote_space = data_locality[arg]
        data_local_space = data_origin[arg]
        if data_local_space != data_remote_space
            # Add copy-from operation
            @dagdebug nothing :spawn_datadeps "Enqueueing copy-from: $data_remote_space => $data_local_space"
            arg_local = remote_args[data_local_space][arg]
            arg_remote = remote_args[data_remote_space][arg]
            @assert arg_remote !== arg_local
            data_local_proc = first(processors(data_local_space))
            copy_from_scope = ExactScope(data_local_proc)
            copy_from_syncdeps = Set()
            get_write_deps!(arg, copy_from_syncdeps)
            @dagdebug nothing :spawn_datadeps "$(length(copy_from_syncdeps)) syncdeps"
            # TODO: copy_from = Dagger.@spawn scope=copy_from_scope syncdeps=copy_from_syncdeps Dagger.move!(data_local_space, data_remote_space, arg_local, arg_remote)
            copy_from = Dagger.@spawn scope=copy_from_scope syncdeps=copy_from_syncdeps copyto!(arg_local, arg_remote)
        else
            @dagdebug nothing :spawn_datadeps "Skipped copy-from (local): $data_remote_space"
        end
    end
end

"""
    spawn_datadeps(f::Base.Callable; static::Bool=true, traversal::Symbol=:inorder)

Constructs a "datadeps" (data dependencies) region and calls `f` within it.
Dagger tasks launched within `f` may wrap their arguments with `In`, `Out`, or
`InOut` to indicate whether the task will read, write, or read+write that
argument, respectively. These argument dependencies will be used to specify
which tasks depend on each other based on the following rules:

- Dependencies across different arguments are independent; only dependencies on the same argument synchronize with each other ("same-ness" is determined based on `isequal`)
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

The keyword argument `static` can be set to `false` to use the simpler dynamic
schedule - its usage is experimental and is subject to change.

The keyword argument `traversal` controls the order that tasks are launched by
the static scheduler, and may be set to `:bfs` or `:dfs` for Breadth-First
Scheduling or Depth-First Scheduling, respectively. All traversal orders
respect the dependencies and ordering of the launched tasks, but may provide
better or worse performance for a given set of datadeps tasks. This argument
is experimental and subject to change.
"""
function spawn_datadeps(f::Base.Callable; static::Bool=true,
                        traversal::Symbol=:inorder)
    wait_all(; check_errors=true) do
        queue = DataDepsTaskQueue(get_options(:task_queue, DefaultTaskQueue());
                                  static, traversal)
        result = with_options(f; task_queue=queue)
        if queue.static
            distribute_tasks!(queue)
        end
        return result
    end
end
