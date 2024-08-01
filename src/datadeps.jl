import Graphs: SimpleDiGraph, add_edge!, add_vertex!, inneighbors, outneighbors, nv

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

function enqueue!(queue::DataDepsTaskQueue, spec::Pair{DTaskSpec,DTask})
    push!(queue.seen_tasks, spec)
end
function enqueue!(queue::DataDepsTaskQueue, specs::Vector{Pair{DTaskSpec,DTask}})
    append!(queue.seen_tasks, specs)
end

struct DataDepsAliasingState
    # Track original and current data locations
    # We track data => space
    data_origin::Dict{AbstractAliasing,MemorySpace}
    data_locality::Dict{AbstractAliasing,MemorySpace}

    # Track writers ("owners") and readers
    ainfos_owner::Dict{AbstractAliasing,Union{Pair{DTask,Int},Nothing}}
    ainfos_readers::Dict{AbstractAliasing,Vector{Pair{DTask,Int}}}
    ainfos_overlaps::Dict{AbstractAliasing,Set{AbstractAliasing}}

    # Cache ainfo lookups
    ainfo_cache::Dict{Tuple{Any,Any},AbstractAliasing}

    function DataDepsAliasingState()
        data_origin = Dict{AbstractAliasing,MemorySpace}()
        data_locality = Dict{AbstractAliasing,MemorySpace}()

        ainfos_owner = Dict{AbstractAliasing,Union{Pair{DTask,Int},Nothing}}()
        ainfos_readers = Dict{AbstractAliasing,Vector{Pair{DTask,Int}}}()
        ainfos_overlaps = Dict{AbstractAliasing,Set{AbstractAliasing}}()

        ainfo_cache = Dict{Tuple{Any,Any},AbstractAliasing}()

        return new(data_origin, data_locality,
                   ainfos_owner, ainfos_readers, ainfos_overlaps,
                   ainfo_cache)
    end
end
struct DataDepsNonAliasingState
    # Track original and current data locations
    # We track data => space
    data_origin::IdDict{Any,MemorySpace}
    data_locality::IdDict{Any,MemorySpace}

    # Track writers ("owners") and readers
    args_owner::IdDict{Any,Union{Pair{DTask,Int},Nothing}}
    args_readers::IdDict{Any,Vector{Pair{DTask,Int}}}

    function DataDepsNonAliasingState()
        data_origin = IdDict{Any,MemorySpace}()
        data_locality = IdDict{Any,MemorySpace}()

        args_owner = IdDict{Any,Union{Pair{DTask,Int},Nothing}}()
        args_readers = IdDict{Any,Vector{Pair{DTask,Int}}}()

        return new(data_origin, data_locality,
                   args_owner, args_readers)
    end
end
struct DataDepsState{State<:Union{DataDepsAliasingState,DataDepsNonAliasingState}}
    # Whether aliasing is being analyzed
    aliasing::Bool

    # The ordered list of tasks and their read/write dependencies
    dependencies::Vector{Pair{DTask,Vector{Tuple{Bool,Bool,<:AbstractAliasing,<:Any,<:Any}}}}

    # The mapping of memory space to remote argument copies
    remote_args::Dict{MemorySpace,IdDict{Any,Any}}

    # The aliasing analysis state
    alias_state::State

    function DataDepsState(aliasing::Bool)
        dependencies = Pair{DTask,Vector{Tuple{Bool,Bool,<:AbstractAliasing,<:Any,<:Any}}}[]
        remote_args = Dict{MemorySpace,IdDict{Any,Any}}()
        if aliasing
            state = DataDepsAliasingState()
        else
            state = DataDepsNonAliasingState()
        end
        return new{typeof(state)}(aliasing, dependencies, remote_args, state)
    end
end

function aliasing(astate::DataDepsAliasingState, arg, dep_mod)
    return get!(astate.ainfo_cache, (arg, dep_mod)) do
        return aliasing(arg, dep_mod)
    end
end

# Determine which arguments could be written to, and thus need tracking

"Whether `arg` has any writedep in this datadeps region."
function has_writedep(state::DataDepsState{DataDepsNonAliasingState}, arg, deps)
    # Check if we are writing to this memory
    writedep = any(dep->dep[3], deps)
    if writedep
        arg_has_writedep[arg] = true
        return true
    end

    # Check if another task is writing to this memory
    for (_, taskdeps) in state.dependencies
        for (_, other_arg_writedep, _, _, other_arg) in taskdeps
            other_arg_writedep || continue
            if arg === other_arg
                return true
            end
        end
    end

    return false
end
"""
Whether `arg` has any writedep at or before executing `task` in this
datadeps region.
"""
function has_writedep(state::DataDepsState, arg, deps, task::DTask)
    is_writedep(arg, deps, task) && return true
    if state.aliasing
        for (other_task, other_taskdeps) in state.dependencies
            for (readdep, writedep, other_ainfo, _, _) in other_taskdeps
                writedep || continue
                for (dep_mod, _, _) in deps
                    ainfo = aliasing(state.alias_state, arg, dep_mod)
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
        for (other_task, other_taskdeps) in state.dependencies
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
function is_writedep(arg, deps, task::DTask)
    return any(dep->dep[3], deps)
end

# Aliasing state setup
function populate_task_info!(state::DataDepsState, spec::DTaskSpec, task::DTask)
    # Populate task dependencies
    dependencies_to_add = Vector{Tuple{Bool,Bool,AbstractAliasing,<:Any,<:Any}}()

    # Track the task's arguments and access patterns
    for (idx, (pos, arg)) in enumerate(spec.args)
        # Unwrap In/InOut/Out wrappers and record dependencies
        arg, deps = unwrap_inout(arg)

        # Unwrap the Chunk underlying any DTask arguments
        arg = arg isa DTask ? fetch(arg; raw=true) : arg

        # Skip non-aliasing arguments
        type_may_alias(typeof(arg)) || continue

        # Add all aliasing dependencies
        for (dep_mod, readdep, writedep) in deps
            if state.aliasing
                ainfo = aliasing(state.alias_state, arg, dep_mod)
            else
                ainfo = UnknownAliasing()
            end
            push!(dependencies_to_add, (readdep, writedep, ainfo, dep_mod, arg))
        end

        # Populate argument write info
        populate_argument_info!(state, arg, deps)
    end

    # Track the task result too
    # N.B. We state no readdep/writedep because, while we can't model the aliasing info for the task result yet, we don't want to synchronize because of this
    push!(dependencies_to_add, (false, false, UnknownAliasing(), identity, task))

    # Record argument/result dependencies
    push!(state.dependencies, task => dependencies_to_add)
end
function populate_argument_info!(state::DataDepsState{DataDepsAliasingState}, arg, deps)
    astate = state.alias_state
    for (dep_mod, readdep, writedep) in deps
        ainfo = aliasing(astate, arg, dep_mod)

        # Initialize owner and readers
        if !haskey(astate.ainfos_owner, ainfo)
            overlaps = Set{AbstractAliasing}()
            push!(overlaps, ainfo)
            for other_ainfo in keys(astate.ainfos_owner)
                ainfo == other_ainfo && continue
                if will_alias(ainfo, other_ainfo)
                    push!(overlaps, other_ainfo)
                    push!(astate.ainfos_overlaps[other_ainfo], ainfo)
                end
            end
            astate.ainfos_overlaps[ainfo] = overlaps
            astate.ainfos_owner[ainfo] = nothing
            astate.ainfos_readers[ainfo] = Pair{DTask,Int}[]
        end

        # Assign data owner and locality
        if !haskey(astate.data_locality, ainfo)
            astate.data_locality[ainfo] = memory_space(arg)
            astate.data_origin[ainfo] = memory_space(arg)
        end
    end
end
function populate_argument_info!(state::DataDepsState{DataDepsNonAliasingState}, arg, deps)
    astate = state.alias_state
    # Initialize owner and readers
    if !haskey(astate.args_owner, arg)
        astate.args_owner[arg] = nothing
        astate.args_readers[arg] = DTask[]
    end

    # Assign data owner and locality
    if !haskey(astate.data_locality, arg)
        astate.data_locality[arg] = memory_space(arg)
        astate.data_origin[arg] = memory_space(arg)
    end
end
function populate_return_info!(state::DataDepsState{DataDepsAliasingState}, task, space)
    astate = state.alias_state
    @assert !haskey(astate.data_locality, task)
    # FIXME: We don't yet know about ainfos for this task
end
function populate_return_info!(state::DataDepsState{DataDepsNonAliasingState}, task, space)
    astate = state.alias_state
    @assert !haskey(astate.data_locality, task)
    astate.data_locality[task] = space
    astate.data_origin[task] = space
end

# Read/write dependency management
function get_write_deps!(state::DataDepsState, ainfo_or_arg, task, write_num, syncdeps)
    _get_write_deps!(state, ainfo_or_arg, task, write_num, syncdeps)
    _get_read_deps!(state, ainfo_or_arg, task, write_num, syncdeps)
end
function get_read_deps!(state::DataDepsState, ainfo_or_arg, task, write_num, syncdeps)
    _get_write_deps!(state, ainfo_or_arg, task, write_num, syncdeps)
end

function _get_write_deps!(state::DataDepsState{DataDepsAliasingState}, ainfo::AbstractAliasing, task, write_num, syncdeps)
    astate = state.alias_state
    ainfo isa NoAliasing && return
    for other_ainfo in astate.ainfos_overlaps[ainfo]
        other_task_write_num = astate.ainfos_owner[other_ainfo]
        @dagdebug nothing :spawn_datadeps "Considering sync with writer via $ainfo -> $other_ainfo"
        other_task_write_num === nothing && continue
        other_task, other_write_num = other_task_write_num
        write_num == other_write_num && continue
        @dagdebug nothing :spawn_datadeps "Sync with writer via $ainfo -> $other_ainfo"
        push!(syncdeps, other_task)
    end
end
function _get_read_deps!(state::DataDepsState{DataDepsAliasingState}, ainfo::AbstractAliasing, task, write_num, syncdeps)
    astate = state.alias_state
    ainfo isa NoAliasing && return
    for other_ainfo in astate.ainfos_overlaps[ainfo]
        @dagdebug nothing :spawn_datadeps "Considering sync with reader via $ainfo -> $other_ainfo"
        other_tasks = astate.ainfos_readers[other_ainfo]
        for (other_task, other_write_num) in other_tasks
            write_num == other_write_num && continue
            @dagdebug nothing :spawn_datadeps "Sync with reader via $ainfo -> $other_ainfo"
            push!(syncdeps, other_task)
        end
    end
end
function add_writer!(state::DataDepsState{DataDepsAliasingState}, ainfo::AbstractAliasing, task, write_num)
    state.alias_state.ainfos_owner[ainfo] = task=>write_num
    empty!(state.alias_state.ainfos_readers[ainfo])
    # Not necessary to assert a read, but conceptually it's true
    add_reader!(state, ainfo, task, write_num)
end
function add_reader!(state::DataDepsState{DataDepsAliasingState}, ainfo::AbstractAliasing, task, write_num)
    push!(state.alias_state.ainfos_readers[ainfo], task=>write_num)
end

function _get_write_deps!(state::DataDepsState{DataDepsNonAliasingState}, arg, task, write_num, syncdeps)
    other_task_write_num = state.alias_state.args_owner[arg]
    if other_task_write_num !== nothing
        other_task, other_write_num = other_task_write_num
        if write_num != other_write_num
            push!(syncdeps, other_task)
        end
    end
end
function _get_read_deps!(state::DataDepsState{DataDepsNonAliasingState}, arg, task, write_num, syncdeps)
    for (other_task, other_write_num) in state.alias_state.args_readers[arg]
        if write_num != other_write_num
            push!(syncdeps, other_task)
        end
    end
end
function add_writer!(state::DataDepsState{DataDepsNonAliasingState}, arg, task, write_num)
    state.alias_state.args_owner[arg] = task=>write_num
    empty!(state.alias_state.args_readers[arg])
    # Not necessary to assert a read, but conceptually it's true
    add_reader!(state, arg, task, write_num)
end
function add_reader!(state::DataDepsState{DataDepsNonAliasingState}, arg, task, write_num)
    push!(state.alias_state.args_readers[arg], task=>write_num)
end

# Make a copy of each piece of data on each worker
# memory_space => {arg => copy_of_arg}
function generate_slot!(state::DataDepsState, dest_space, data)
    if data isa DTask
        data = fetch(data; raw=true)
    end
    orig_space = memory_space(data)
    to_proc = first(processors(dest_space))
    from_proc = first(processors(orig_space))
    dest_space_args = get!(IdDict{Any,Any}, state.remote_args, dest_space)
    if orig_space == dest_space
        data_chunk = tochunk(data, from_proc)
        dest_space_args[data] = data_chunk
        @assert processor(data_chunk) in processors(dest_space) || data isa Chunk && processor(data) isa Dagger.OSProc
        @assert memory_space(data_chunk) == orig_space
    else
        w = only(unique(map(get_parent, collect(processors(dest_space))))).pid
        ctx = Sch.eager_context()
        id = rand(Int)
        timespan_start(ctx, :move, (;thunk_id=0, id, processor=to_proc), (;f=nothing, data))
        dest_space_args[data] = remotecall_fetch(w, from_proc, to_proc, data) do from_proc, to_proc, data
            data_converted = move(from_proc, to_proc, data)
            data_chunk = tochunk(data_converted, to_proc)
            @assert processor(data_chunk) in processors(dest_space)
            @assert memory_space(data_converted) == memory_space(data_chunk) "space mismatch! $(memory_space(data_converted)) != $(memory_space(data_chunk)) ($(typeof(data_converted)) vs. $(typeof(data_chunk))), spaces ($orig_space -> $dest_space)"
            @assert orig_space != memory_space(data_chunk) "space preserved! $orig_space != $(memory_space(data_chunk)) ($(typeof(data)) vs. $(typeof(data_chunk))), spaces ($orig_space -> $dest_space)"
            return data_chunk
        end
        timespan_finish(ctx, :move, (;thunk_id=0, id, processor=to_proc), (;f=nothing, data=dest_space_args[data]))
    end
    return dest_space_args[data]
end

struct DataDepsSchedulerState
    task_to_spec::Dict{DTask,DTaskSpec}
    assignments::Dict{DTask,MemorySpace}
    dependencies::Dict{DTask,Set{DTask}}
    task_completions::Dict{DTask,UInt64}
    space_completions::Dict{MemorySpace,UInt64}
    capacities::Dict{MemorySpace,Int}

    function DataDepsSchedulerState()
        return new(Dict{DTask,DTaskSpec}(),
                   Dict{DTask,MemorySpace}(),
                   Dict{DTask,Set{DTask}}(),
                   Dict{DTask,UInt64}(),
                   Dict{MemorySpace,UInt64}(),
                   Dict{MemorySpace,Int}())
    end
end

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
    astate = state.alias_state
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
            raw_args = map(filter(arg->haskey(astate.data_locality, arg), spec.args)) do arg
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
        our_scope = UnionScope(map(ExactScope, our_procs)...)

        spec.f = move(ThreadProc(myid(), 1), our_proc, spec.f)
        @dagdebug nothing :spawn_datadeps "($(repr(spec.f))) Scheduling: $our_proc ($our_space)"

        # Copy raw task arguments for analysis
        task_args = copy(spec.args)

        # Copy args from local to remote
        for (idx, (pos, arg)) in enumerate(task_args)
            # Is the data written previously or now?
            arg, deps = unwrap_inout(arg)
            arg = arg isa DTask ? fetch(arg; raw=true) : arg
            if !type_may_alias(typeof(arg)) || !has_writedep(state, arg, deps, task)
                @dagdebug nothing :spawn_datadeps "($(repr(spec.f)))[$idx] Skipped copy-to (unwritten)"
                spec.args[idx] = pos => arg
                continue
            end

            # Is the source of truth elsewhere?
            arg_remote = get!(get!(IdDict{Any,Any}, state.remote_args, our_space), arg) do
                generate_slot!(state, our_space, arg)
            end
            if queue.aliasing
                for (dep_mod, _, _) in deps
                    ainfo = aliasing(astate, arg, dep_mod)
                    data_space = astate.data_locality[ainfo]
                    nonlocal = our_space != data_space
                    if nonlocal
                        # Add copy-to operation (depends on latest owner of arg)
                        @dagdebug nothing :spawn_datadeps "($(repr(spec.f)))[$idx][$dep_mod] Enqueueing copy-to: $data_space => $our_space"
                        arg_local = get!(get!(IdDict{Any,Any}, state.remote_args, data_space), arg) do
                            generate_slot!(state, data_space, arg)
                        end
                        copy_to_scope = our_scope
                        copy_to_syncdeps = Set{Any}()
                        get_write_deps!(state, ainfo, task, write_num, copy_to_syncdeps)
                        @dagdebug nothing :spawn_datadeps "($(repr(spec.f)))[$idx][$dep_mod] $(length(copy_to_syncdeps)) syncdeps"
                        copy_to = Dagger.@spawn scope=copy_to_scope syncdeps=copy_to_syncdeps meta=true Dagger.move!(dep_mod, our_space, data_space, arg_remote, arg_local)
                        add_writer!(state, ainfo, copy_to, write_num)

                        astate.data_locality[ainfo] = our_space
                    else
                        @dagdebug nothing :spawn_datadeps "($(repr(spec.f)))[$idx][$dep_mod] Skipped copy-to (local): $data_space"
                    end
                end
            else
                data_space = astate.data_locality[arg]
                nonlocal = our_space != data_space
                if nonlocal
                    # Add copy-to operation (depends on latest owner of arg)
                    @dagdebug nothing :spawn_datadeps "($(repr(spec.f)))[$idx] Enqueueing copy-to: $data_space => $our_space"
                    arg_local = get!(get!(IdDict{Any,Any}, state.remote_args, data_space), arg) do
                        generate_slot!(state, data_space, arg)
                    end
                    copy_to_scope = our_scope
                    copy_to_syncdeps = Set{Any}()
                    get_write_deps!(state, arg, task, write_num, copy_to_syncdeps)
                    @dagdebug nothing :spawn_datadeps "($(repr(spec.f)))[$idx] $(length(copy_to_syncdeps)) syncdeps"
                    copy_to = Dagger.@spawn scope=copy_to_scope syncdeps=copy_to_syncdeps Dagger.move!(identity, our_space, data_space, arg_remote, arg_local)
                    add_writer!(state, arg, copy_to, write_num)

                    astate.data_locality[arg] = our_space
                else
                    @dagdebug nothing :spawn_datadeps "($(repr(spec.f)))[$idx] Skipped copy-to (local): $data_space"
                end
            end
            spec.args[idx] = pos => arg_remote
        end
        write_num += 1

        # Validate that we're not accidentally performing a copy
        for (idx, (_, arg)) in enumerate(spec.args)
            _, deps = unwrap_inout(task_args[idx][2])
            if is_writedep(arg, deps, task)
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
            if queue.aliasing
                for (dep_mod, _, writedep) in deps
                    ainfo = aliasing(astate, arg, dep_mod)
                    if writedep
                        @dagdebug nothing :spawn_datadeps "($(repr(spec.f)))[$idx][$dep_mod] Syncing as writer"
                        get_write_deps!(state, ainfo, task, write_num, syncdeps)
                    else
                        @dagdebug nothing :spawn_datadeps "($(repr(spec.f)))[$idx][$dep_mod] Syncing as reader"
                        get_read_deps!(state, ainfo, task, write_num, syncdeps)
                    end
                end
            else
                if is_writedep(arg, deps, task)
                    @dagdebug nothing :spawn_datadeps "($(repr(spec.f)))[$idx] Syncing as writer"
                    get_write_deps!(state, arg, task, write_num, syncdeps)
                else
                    @dagdebug nothing :spawn_datadeps "($(repr(spec.f)))[$idx] Syncing as reader"
                    get_read_deps!(state, arg, task, write_num, syncdeps)
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
            if queue.aliasing
                for (dep_mod, _, writedep) in deps
                    ainfo = aliasing(astate, arg, dep_mod)
                    if writedep
                        @dagdebug nothing :spawn_datadeps "($(repr(spec.f)))[$idx][$dep_mod] Set as owner"
                        add_writer!(state, ainfo, task, write_num)
                    else
                        add_reader!(state, ainfo, task, write_num)
                    end
                end
            else
                if is_writedep(arg, deps, task)
                    @dagdebug nothing :spawn_datadeps "($(repr(spec.f)))[$idx] Set as owner"
                    add_writer!(state, arg, task, write_num)
                else
                    add_reader!(state, arg, task, write_num)
                end
            end
        end

        # Update tracking for return value
        populate_return_info!(state, task, our_space)

        write_num += 1
        proc_idx = mod1(proc_idx + 1, length(all_procs))
    end

    # Copy args from remote to local
    if queue.aliasing
        # We need to replay the writes from all tasks in-order (skipping any
        # outdated write owners), to ensure that overlapping writes are applied
        # in the correct order

        # First, find the latest owners of each live ainfo
        arg_writes = IdDict{Any,Vector{Tuple{AbstractAliasing,<:Any,MemorySpace}}}()
        for (task, taskdeps) in state.dependencies
            for (_, writedep, ainfo, dep_mod, arg) in taskdeps
                writedep || continue
                haskey(astate.data_locality, ainfo) || continue
                @assert haskey(astate.ainfos_owner, ainfo) "Missing ainfo: $ainfo ($dep_mod($(typeof(arg))))"

                # Skip virtual writes from task result aliasing
                # FIXME: Make this less bad
                if arg isa DTask && dep_mod === identity && ainfo isa UnknownAliasing
                    continue
                end

                # Get the set of writers
                ainfo_writes = get!(Vector{Tuple{AbstractAliasing,<:Any,MemorySpace}}, arg_writes, arg)

                #= FIXME: If we fully overlap any writer, evict them
                idxs = findall(ainfo_write->overlaps_all(ainfo, ainfo_write[1]), ainfo_writes)
                deleteat!(ainfo_writes, idxs)
                =#

                # Make ourselves the latest writer
                push!(ainfo_writes, (ainfo, dep_mod, astate.data_locality[ainfo]))
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
            for (ainfo, dep_mod, data_remote_space) in ainfo_writes
                # Is the source of truth elsewhere?
                data_local_space = astate.data_origin[ainfo]
                if data_local_space != data_remote_space
                    # Add copy-from operation
                    @dagdebug nothing :spawn_datadeps "[$dep_mod] Enqueueing copy-from: $data_remote_space => $data_local_space"
                    arg_local = get!(get!(IdDict{Any,Any}, state.remote_args, data_local_space), arg) do
                        generate_slot!(state, data_local_space, arg)
                    end
                    arg_remote = state.remote_args[data_remote_space][arg]
                    @assert arg_remote !== arg_local
                    data_local_proc = first(processors(data_local_space))
                    copy_from_scope = UnionScope(map(ExactScope, collect(processors(data_local_space)))...)
                    copy_from_syncdeps = Set()
                    get_write_deps!(state, ainfo, nothing, write_num, copy_from_syncdeps)
                    @dagdebug nothing :spawn_datadeps "$(length(copy_from_syncdeps)) syncdeps"
                    copy_from = Dagger.@spawn scope=copy_from_scope syncdeps=copy_from_syncdeps meta=true Dagger.move!(dep_mod, data_local_space, data_remote_space, arg_local, arg_remote)
                else
                    @dagdebug nothing :spawn_datadeps "[$dep_mod] Skipped copy-from (local): $data_remote_space"
                end
            end
        end
    else
        for arg in keys(astate.data_origin)
            # Is the data previously written?
            arg, deps = unwrap_inout(arg)
            if !type_may_alias(typeof(arg)) || !has_writedep(state, arg, deps)
                @dagdebug nothing :spawn_datadeps "Skipped copy-from (unwritten)"
            end

            # Is the source of truth elsewhere?
            data_remote_space = astate.data_locality[arg]
            data_local_space = astate.data_origin[arg]
            if data_local_space != data_remote_space
                # Add copy-from operation
                @dagdebug nothing :spawn_datadeps "Enqueueing copy-from: $data_remote_space => $data_local_space"
                arg_local = state.remote_args[data_local_space][arg]
                arg_remote = state.remote_args[data_remote_space][arg]
                @assert arg_remote !== arg_local
                data_local_proc = first(processors(data_local_space))
                copy_from_scope = ExactScope(data_local_proc)
                copy_from_syncdeps = Set()
                get_write_deps!(state, arg, nothing, write_num, copy_from_syncdeps)
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
