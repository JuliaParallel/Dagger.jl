using Graphs

export In, Out, InOut, Deps, spawn_datadeps

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
"Specifies one or more dependencies."
struct Deps{T,DT<:Tuple}
    x::T
    deps::DT
end
Deps(x, deps...) = Deps(x, deps)

struct DataDepsTaskQueue <: AbstractTaskQueue
    # The queue above us
    upper_queue::AbstractTaskQueue
    # The set of tasks that have already been seen
    seen_tasks::Union{Vector{Pair{EagerTaskSpec,EagerThunk}},Nothing}
    # The data-dependency graph of all tasks
    g::Union{SimpleDiGraph{Int},Nothing}
    # The mapping from task to graph ID
    task_to_id::Union{Dict{EagerThunk,Int},Nothing}
    # How to traverse the dependency graph when launching tasks
    traversal::Symbol

    # Whether aliasing across arguments is possible
    # The fields following only apply when aliasing==true
    aliasing::Bool
    # The ordered list of tasks and their read/write dependencies
    dependencies::Vector{Pair{EagerThunk,Vector{Tuple{Bool,Bool,<:AbstractAliasing,<:Any,<:Any}}}}

    function DataDepsTaskQueue(upper_queue;
                               traversal::Symbol=:inorder, aliasing::Bool=true)
        seen_tasks = Pair{EagerTaskSpec,EagerThunk}[]
        g = SimpleDiGraph()
        task_to_id = Dict{EagerThunk,Int}()
        dependencies = Pair{EagerThunk,Vector{Tuple{Bool,Bool,<:AbstractAliasing,<:Any,<:Any}}}[]
        return new(upper_queue, seen_tasks, g, task_to_id, traversal,
                   aliasing, dependencies)
    end
end

function unwrap_inout(arg)
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
    elseif arg isa Deps
        alldeps = Tuple[]
        for dep in arg.deps
            dep_mod, inner_deps = unwrap_inout(dep)
            for (_, readdep, writedep) in inner_deps
                push!(alldeps, (dep_mod, readdep, writedep))
            end
        end
        arg = arg.x
        return arg, alldeps
    else
        readdep = true
    end
    return arg, Tuple[(identity, readdep, writedep)]
end

function enqueue!(queue::DataDepsTaskQueue, spec::Pair{EagerTaskSpec,EagerThunk})
    push!(queue.seen_tasks, spec)
end
function enqueue!(queue::DataDepsTaskQueue, specs::Vector{Pair{EagerTaskSpec,EagerThunk}})
    append!(queue.seen_tasks, specs)
end

function distribute_tasks!(queue::DataDepsTaskQueue)
    #= TODO: Improvements to be made:
    # - Support for non-CPU processors
    # - Support for copying non-AbstractArray arguments
    # - Use graph coloring for scheduling OR use Dagger's scheduler directly
    # - Generate slots on-the-fly
    # - Parallelize read copies
    # - Unreference unused slots
    # - Reuse memory when possible (SafeTensors)
    # - Account for differently-sized data
    # - Account for different task execution times
    =#

    # Determine which arguments could be written to, and thus need tracking
    if queue.aliasing
        ainfo_has_writedep = Dict{Any,Bool}()
    else
        arg_has_writedep = IdDict{Any,Bool}()
    end
    "Whether `arg` has any writedep in this datadeps region."
    function has_writedep(arg, deps)
        if queue.aliasing
            for (dep_mod, readdep, writedep) in deps
                ainfo = aliasing(arg, dep_mod)
                error("FIXME")
            end
            return false
        else
            return arg_has_writedep[arg]
        end
    end
    """
    Whether `arg` has any writedep at or before executing `task` in this
    datadeps region.
    """
    function has_writedep(arg, deps, task::EagerThunk)
        is_writedep(arg, deps, task) && return true
        if queue.aliasing
            for (other_task, other_taskdeps) in queue.dependencies
                for (readdep, writedep, other_ainfo, _, _) in other_taskdeps
                    writedep || continue
                    for (dep_mod, _, _) in deps
                        ainfo = aliasing(arg, dep_mod)
                        if will_alias(ainfo, other_ainfo)
                            return true
                        end
                    end
                end
                if task === other_task
                    return false
                end
            end
        else
            for (other_task, other_taskdeps) in queue.dependencies
                for (readdep, writedep, _, _, other_arg) in other_taskdeps
                    writedep || continue
                    if arg === other_arg
                        return true
                    end
                end
                if task === other_task
                    return false
                end
            end
        end
        error("Task isn't in argdeps set")
    end
    "Whether `arg` is written to by `task`."
    function is_writedep(arg, deps, task::EagerThunk)
        return any(dep->dep[3], deps)
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
    if queue.aliasing
        data_origin = Dict{AbstractAliasing,MemorySpace}()
        data_locality = Dict{AbstractAliasing,MemorySpace}()
    else
        data_origin = IdDict{Any,MemorySpace}()
        data_locality = IdDict{Any,MemorySpace}()
    end

    # Track writers ("owners") and readers
    args_tracked = IdDict{Any,Bool}()
    if queue.aliasing
        ainfos_owner = Dict{AbstractAliasing,Union{EagerThunk,Nothing}}()
        ainfos_readers = Dict{AbstractAliasing,Vector{EagerThunk}}()
    else
        args_owner = IdDict{Any,Union{EagerThunk,Nothing}}()
        args_readers = IdDict{Any,Vector{EagerThunk}}()
    end
    function populate_task_info!(spec, task)
        # Populate task dependencies
        if queue.aliasing
            dependencies_to_add = Vector{Tuple{Bool,Bool,AbstractAliasing,<:Any,<:Any}}()

            # Track the task's arguments and access patterns
            for (idx, (pos, arg)) in enumerate(spec.args)
                # Unwrap In/InOut/Out wrappers and record dependencies
                arg, deps = unwrap_inout(arg)

                # Unwrap the Chunk underlying any EagerThunk arguments
                arg_data = arg isa EagerThunk ? fetch(arg; raw=true) : arg

                # Add all aliasing dependencies
                for (dep_mod, readdep, writedep) in deps
                    ainfo = aliasing(arg_data, dep_mod)
                    push!(dependencies_to_add, (readdep, writedep, ainfo, dep_mod, arg_data))
                end
            end

            # Track the task result too
            push!(dependencies_to_add, (true, true, UnknownAliasing(), identity, task))

            # Record argument/result dependencies
            push!(queue.dependencies, task => dependencies_to_add)

            # FIXME: Should we call populate_argument_info! here?
        else
            error("FIXME")
        end
    end
    function populate_argument_info!(arg, deps)
        # Populate argument write info
        if queue.aliasing
            for (dep_mod, readdep, writedep) in deps
                ainfo = aliasing(arg, dep_mod)

                # Initialize owner and readers
                if !haskey(ainfos_owner, ainfo)
                    ainfos_owner[ainfo] = nothing
                    ainfos_readers[ainfo] = EagerThunk[]
                end

                # Assign data owner and locality
                if !haskey(data_locality, ainfo)
                    data_locality[ainfo] = memory_space(arg)
                    data_origin[ainfo] = memory_space(arg)
                end

                # Check if another task is writing to this memory
                writedep = false
                for (_, taskdeps) in queue.dependencies
                    for (_, ainfo_writedep, other_ainfo, _, _) in taskdeps
                        ainfo_writedep || continue
                        if will_alias(ainfo, other_ainfo)
                            writedep = true
                            break
                        end
                    end
                    writedep && break
                end
            end
        else
            # Initialize owner and readers
            if !haskey(args_owner, arg)
                args_owner[arg] = nothing
                args_readers[arg] = EagerThunk[]
            end

            # Assign data owner and locality
            if !haskey(data_locality, arg)
                data_locality[arg] = memory_space(arg)
                data_origin[arg] = memory_space(arg)
            end

            if haskey(arg_has_writedep, arg) && arg_has_writedep[arg]
                return
            end

            # Check if we are writing to this memory
            writedep = any(dep->dep[3], deps)
            if writedep
                arg_has_writedep[arg] = true
                return
            end

            # Check if another task is writing to this memory
            writedep = false
            for (_, taskdeps) in queue.dependencies
                for (_, other_arg_writedep, _, _, other_arg) in taskdeps
                    other_arg_writedep || continue
                    if arg === other_arg
                        writedep = true
                        break
                    end
                end
                writedep && break
            end
            arg_has_writedep[arg] = writedep
        end

        @label owner_readers
        # Populate owner/readers
        if queue.aliasing
            for (dep_mod, _, _) in deps
                ainfo = aliasing(arg, dep_mod)
                ainfos_owner[ainfo] = nothing
                ainfos_readers[ainfo] = EagerThunk[]
            end
        else
            args_owner[arg] = nothing
            args_readers[arg] = EagerThunk[]
        end
    end

    # Aliasing
    function get_write_deps!(ainfo::AbstractAliasing, task, syncdeps)
        for (other_ainfo, other_task) in ainfos_owner
            will_alias(ainfo, other_ainfo) || continue
            if other_task !== nothing && other_task !== task
                push!(syncdeps, other_task)
            end
        end
        get_read_deps!(ainfo, task, syncdeps)
    end
    function get_read_deps!(ainfo::AbstractAliasing, task, syncdeps)
        for (other_ainfo, other_tasks) in ainfos_readers
            will_alias(ainfo, other_ainfo) || continue
            for other_task in other_tasks
                if other_task !== task
                    push!(syncdeps, other_task)
                end
            end
        end
    end
    function add_writer!(ainfo::AbstractAliasing, task)
        ainfos_owner[ainfo] = task
        filter!(other_task->other_task === task, ainfos_readers[ainfo])
        # Not necessary to assert a read, but conceptually it's true
        add_reader!(ainfo, task)
    end
    function add_reader!(ainfo::AbstractAliasing, task)
        push!(ainfos_readers[ainfo], task)
    end

    # Non-Aliasing
    function get_write_deps!(arg, syncdeps)
        haskey(args_tracked, arg) || return
        if (owner = args_owner[arg]) !== nothing
            push!(syncdeps, owner)
        end
        get_read_deps!(arg, syncdeps)
    end
    function get_read_deps!(arg, syncdeps)
        haskey(args_tracked, arg) || return
        if (owner = args_owner[arg]) !== nothing
            push!(syncdeps, owner)
        end
    end
    function add_writer!(arg, task)
        args_owner[arg] = task
        empty!(args_readers[arg])
        # Not necessary to assert a read, but conceptually it's true
        add_reader!(arg, task)
    end
    function add_reader!(arg, task)
        push!(args_readers[arg], task)
    end

    # Make a copy of each piece of data on each worker
    # memory_space => {arg => copy_of_arg}
    remote_args = Dict{MemorySpace,IdDict{Any,Any}}(space=>IdDict{Any,Any}() for space in all_spaces)
    function generate_slot!(space, data)
        if data isa EagerThunk
            data = fetch(data; raw=true)
        end
        data_space = memory_space(data)
        if data_space == space
            return Dagger.tochunk(data)
        else
            # TODO: Can't use @mutable with custom Chunk scope
            #remote_args[w][data] = Dagger.@mutable worker=w copy(data)
            to_proc = first(processors(space))
            from_proc = first(processors(data_space))
            w = only(unique(map(get_parent, collect(processors(space))))).pid
            return remotecall_fetch(w, from_proc, to_proc, data) do from_proc, to_proc, data
                data_raw = fetch(data)
                data_converted = move(from_proc, to_proc, data_raw)
                return Dagger.tochunk(data_converted)
            end
        end
    end
    #=
    for space in all_spaces
        this_space_args = remote_args[space] = IdDict{Any,Any}()
        for data in keys(queue.deps)
            data isa EagerThunk && !istaskstarted(data) && continue
            has_writedep(data) || continue
            this_space_args[data] = generate_slot!(space, data)
        end
    end
    =#

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

    # Start launching tasks and necessary copies
    for (spec, task) in queue.seen_tasks[task_order]
        our_proc = all_procs[proc_idx]
        our_space = only(memory_spaces(our_proc))

        @dagdebug nothing :spawn_datadeps "($(spec.f)) Scheduling: $our_proc ($our_space)"

        # Copy raw task arguments for analysis
        task_args = copy(spec.args)

        # Populate all task dependencies
        populate_task_info!(spec, task)

        # Copy args from local to remote
        for (idx, (pos, arg)) in enumerate(task_args)
            # Is the data written previously or now?
            arg, deps = unwrap_inout(arg)
            arg = arg isa EagerThunk ? fetch(arg; raw=true) : arg
            populate_argument_info!(arg, deps)
            if !has_writedep(arg, deps, task)
                @dagdebug nothing :spawn_datadeps "($(spec.f))[$idx] Skipped copy-to (unwritten)"
                spec.args[idx] = pos => arg
                continue
            end

            # Is the source of truth elsewhere?
            arg_remote = get!(remote_args[our_space], arg) do
                generate_slot!(our_space, arg)
            end
            if queue.aliasing
                for (dep_mod, _, _) in deps
                    ainfo = aliasing(arg, dep_mod)
                    data_space = data_locality[ainfo]
                    nonlocal = our_space != data_space
                    if nonlocal
                        # Add copy-to operation (depends on latest owner of arg)
                        @dagdebug nothing :spawn_datadeps "($(spec.f))[$idx][$dep_mod] Enqueueing copy-to: $data_space => $our_space"
                        arg_local = get!(remote_args[data_space], arg) do
                            generate_slot!(data_space, arg)
                        end
                        copy_to_scope = ExactScope(our_proc)
                        copy_to_syncdeps = Set{Any}()
                        get_write_deps!(ainfo, task, copy_to_syncdeps)
                        @dagdebug nothing :spawn_datadeps "($(spec.f))[$idx][$dep_mod] $(length(copy_to_syncdeps)) syncdeps"
                        copy_to = Dagger.@spawn scope=copy_to_scope syncdeps=copy_to_syncdeps meta=true Dagger.move!(dep_mod, our_space, data_space, arg_remote, arg_local)
                        add_writer!(ainfo, copy_to)

                        data_locality[ainfo] = our_space
                    else
                        @dagdebug nothing :spawn_datadeps "($(spec.f))[$idx][$dep_mod] Skipped copy-to (local): $data_space"
                    end
                end
            else
                data_space = data_locality[arg]
                nonlocal = our_space != data_space
                if nonlocal
                    # Add copy-to operation (depends on latest owner of arg)
                    @dagdebug nothing :spawn_datadeps "($(spec.f))[$idx] Enqueueing copy-to: $data_space => $our_space"
                    arg_local = get!(remote_args[data_space], arg) do
                        generate_slot!(data_space, arg)
                    end
                    copy_to_scope = ExactScope(our_proc)
                    copy_to_syncdeps = Set{Any}()
                    get_write_deps!(arg, copy_to_syncdeps)
                    @dagdebug nothing :spawn_datadeps "($(spec.f))[$idx] $(length(copy_to_syncdeps)) syncdeps"
                    copy_to = Dagger.@spawn scope=copy_to_scope syncdeps=copy_to_syncdeps Dagger.move!(identity, our_space, data_space, arg_remote, arg_local)
                    add_writer!(arg, copy_to)

                    data_locality[arg] = our_space
                else
                    @dagdebug nothing :spawn_datadeps "($(spec.f))[$idx] Skipped copy-to (local): $data_space"
                end
            end
            spec.args[idx] = pos => arg_remote
        end

        # Validate that we're not accidentally performing a copy
        for (idx, (_, arg)) in enumerate(spec.args)
            _, deps = unwrap_inout(task_args[idx][2])
            if is_writedep(arg, deps, task)
                arg_space = memory_space(arg)
                @assert arg_space == our_space "($(spec.f))[$idx] Tried to pass $(typeof(arg)) from $arg_space to $our_space"
            end
        end

        # Launch user's task
        spec.f = move(ThreadProc(myid(), 1), our_proc, spec.f)
        syncdeps = get(Set{Any}, spec.options, :syncdeps)
        for (idx, (_, arg)) in enumerate(task_args)
            arg, deps = unwrap_inout(arg)
            if queue.aliasing
                for (dep_mod, _, writedep) in deps
                    ainfo = aliasing(arg, dep_mod)
                    if writedep
                        @dagdebug nothing :spawn_datadeps "($(spec.f))[$idx][$dep_mod] Sync with owner/readers"
                        get_write_deps!(ainfo, task, syncdeps)
                    else
                        get_read_deps!(ainfo, task, syncdeps)
                    end
                end
            else
                if is_writedep(arg, deps, task)
                    @dagdebug nothing :spawn_datadeps "($(spec.f))[$idx] Sync with owner/readers"
                    get_write_deps!(arg, syncdeps)
                else
                    get_read_deps!(arg, syncdeps)
                end
            end
        end
        @dagdebug nothing :spawn_datadeps "($(spec.f)) $(length(syncdeps)) syncdeps"
        task_scope = Dagger.ExactScope(our_proc)
        spec.options = merge(spec.options, (;syncdeps, scope=task_scope))
        enqueue!(upper_queue, spec=>task)

        # Update read/write tracking for arguments
        for (idx, (_, arg)) in enumerate(task_args)
            arg, deps = unwrap_inout(arg)
            if queue.aliasing
                for (dep_mod, _, writedep) in deps
                    ainfo = aliasing(arg, dep_mod)
                    if writedep
                        @dagdebug nothing :spawn_datadeps "($(spec.f))[$idx][$dep_mod] Set as owner"
                        add_writer!(ainfo, task)
                    else
                        add_reader!(ainfo, task)
                    end
                end
            else
                if is_writedep(arg, deps, task)
                    @dagdebug nothing :spawn_datadeps "($(spec.f))[$idx] Set as owner"
                    add_writer!(arg, task)
                else
                    add_reader!(arg, task)
                end
            end
        end

        # Select the next processor to use
        proc_idx = mod1(proc_idx+1, length(all_procs))
    end

    # Copy args from remote to local
    if queue.aliasing
        # We need to replay the writes from all tasks in-order (skipping any
        # outdated write owners), to ensure that overlapping writes are applied
        # in the correct order

        # First, find the latest owners of each live ainfo
        arg_writes = IdDict{Any,Vector{Tuple{AbstractAliasing,<:Any,MemorySpace}}}()
        for (task, taskdeps) in queue.dependencies
            for (_, writedep, ainfo, dep_mod, arg) in taskdeps
                writedep || continue
                haskey(data_locality, ainfo) || continue
                @assert haskey(ainfos_owner, ainfo) "Missing ainfo: $ainfo ($dep_mod($(typeof(arg))))"

                # Get the set of writers
                ainfo_writes = get!(Vector{Tuple{AbstractAliasing,<:Any,MemorySpace}}, arg_writes, arg)

                #=FIXME If we fully overlap any writer, evict them
                idxs = findall(ainfo_write->overlaps_all(ainfo, ainfo_write[1]), ainfo_writes)
                deleteat!(ainfo_writes, idxs)
                =#

                # Make ourselves the latest writer
                push!(ainfo_writes, (ainfo, dep_mod, data_locality[ainfo]))
            end
        end

        # Then, replay the writes from each owner in-order
        for (arg, ainfo_writes) in arg_writes
            for (ainfo, dep_mod, data_remote_space) in ainfo_writes
                # Is the source of truth elsewhere?
                data_local_space = data_origin[ainfo]
                if data_local_space != data_remote_space
                    # Add copy-from operation
                    @dagdebug nothing :spawn_datadeps "[$dep_mod] Enqueueing copy-from: $data_remote_space => $data_local_space"
                    arg_local = remote_args[data_local_space][arg]
                    arg_remote = remote_args[data_remote_space][arg]
                    @assert arg_remote !== arg_local
                    data_local_proc = first(processors(data_local_space))
                    copy_from_scope = ExactScope(data_local_proc)
                    copy_from_syncdeps = Set()
                    get_write_deps!(ainfo, copy_from_syncdeps)
                    @dagdebug nothing :spawn_datadeps "$(length(copy_from_syncdeps)) syncdeps"
                    copy_from = Dagger.@spawn scope=copy_from_scope syncdeps=copy_from_syncdeps meta=true Dagger.move!(dep_mod, data_local_space, data_remote_space, arg_local, arg_remote)
                else
                    @dagdebug nothing :spawn_datadeps "[$dep_mod] Skipped copy-from (local): $data_remote_space"
                end
            end
        end
    else
        for arg in keys(remote_args[our_space])
            # Is the data previously written?
            arg, deps = unwrap_inout(arg)
            if !has_writedep(arg, deps)
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
                copy_from = Dagger.@spawn scope=copy_from_scope syncdeps=copy_from_syncdeps meta=true Dagger.move!(identity, data_local_space, data_remote_space, arg_local, arg_remote)
            else
                @dagdebug nothing :spawn_datadeps "Skipped copy-from (local): $data_remote_space"
            end
        end
    end
end

"""
    spawn_datadeps(f::Base.Callable; traversal::Symbol=:inorder)

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

The keyword argument `traversal` controls the order that tasks are launched by
the scheduler, and may be set to `:bfs` or `:dfs` for Breadth-First Scheduling
or Depth-First Scheduling, respectively. All traversal orders respect the
dependencies and ordering of the launched tasks, but may provide better or
worse performance for a given set of datadeps tasks. This argument is
experimental and subject to change.
"""
function spawn_datadeps(f::Base.Callable; static::Bool=true,
                        traversal::Symbol=:inorder, aliasing::Bool=true)
    if !static
        throw(ArgumentError("Dynamic scheduling is no longer available"))
    end
    wait_all(; check_errors=true) do
        queue = DataDepsTaskQueue(get_options(:task_queue, EagerTaskQueue());
                                  traversal, aliasing)
        result = with_options(f; task_queue=queue)
        distribute_tasks!(queue)
        return result
    end
end
