import Graphs: SimpleDiGraph, add_edge!, add_vertex!, inneighbors, outneighbors, nv, ne

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
"Specifies a data format modifier."
struct Modify{T,M}
    x::T
    mod::M
end

# FIXME: Less bad name
abstract type AbstractFormatModifier end
modify_data(x, mod::AbstractFormatModifier) =
    error("Unsupported format modifier $(mod) for data of type $(typeof(x))")
modify_data_unwrap(x, mod::AbstractFormatModifier) = modify_data(x, mod)
modify_data_unwrap(x::Chunk, mod::AbstractFormatModifier) =
    modify_data(fetch(x), mod)

struct NoModifier <: AbstractFormatModifier end
modify_data(x, ::NoModifier) = x

struct FloatingPointModifier{T<:AbstractFloat} <: AbstractFormatModifier end
FloatingPointModifier(T) = FloatingPointModifier{T}()
modify_data(A::AbstractArray, mod::FloatingPointModifier{T}) where T =
    convert.(T, A)
modify_data(x::Real, mod::FloatingPointModifier{T}) where T = T(x)

struct DataDepsTaskQueue <: AbstractTaskQueue
    # The queue above us
    upper_queue::AbstractTaskQueue
    # The set of tasks that have already been seen
    seen_tasks::Union{Vector{Pair{DTaskSpec,DTask}},Nothing}
    # Which scheduler to use to assign tasks to processors
    scheduler::Any

    # Whether aliasing across arguments is possible
    # The fields following only apply when aliasing==true
    aliasing::Bool

    function DataDepsTaskQueue(upper_queue;
                               scheduler=RoundRobinScheduler(),
                               aliasing::Bool=true)
        seen_tasks = Pair{DTaskSpec,DTask}[]
        return new(upper_queue, seen_tasks, scheduler, aliasing)
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
        arg, mod = unwrap_format_mod(arg.x)
        return arg, alldeps, mod
    else
        readdep = true
    end
    arg, mod = unwrap_format_mod(arg)
    return arg, Tuple[(identity, readdep, writedep)], mod
end
unwrap_inout_value(arg) = first(unwrap_inout(arg))
function unwrap_format_mod(arg)
    if arg isa Modify
        return arg.x, arg.mod
    end
    return arg, NoModifier()
end

function enqueue!(queue::DataDepsTaskQueue, spec::Pair{DTaskSpec,DTask})
    push!(queue.seen_tasks, spec)
end
function enqueue!(queue::DataDepsTaskQueue, specs::Vector{Pair{DTaskSpec,DTask}})
    append!(queue.seen_tasks, specs)
end

struct DatadepsArgSpec
    pos::Union{Int, Symbol}
    value_type::Type
    dep_mod::Any
    ainfo::AbstractAliasing
end
struct DTaskDAGID{id} end
struct DAGSpec
    g::SimpleDiGraph{Int}
    id_to_uid::Dict{Int, UInt}
    uid_to_id::Dict{UInt, Int}
    id_to_functype::Dict{Int, Type} # FIXME: DatadepsArgSpec
    id_to_argtypes::Dict{Int, Vector{DatadepsArgSpec}}
    DAGSpec() = new(SimpleDiGraph{Int}(),
                    Dict{Int, UInt}(), Dict{UInt, Int}(),
                    Dict{Int, Type}(),
                    Dict{Int, Vector{DatadepsArgSpec}}())
end
function dag_add_task!(dspec::DAGSpec, tspec::DTaskSpec, task::DTask)
    # Check if this task depends on any other tasks within the DAG,
    # which we are not yet ready to handle
    for (idx, (kwpos, arg)) in enumerate(tspec.args)
        arg, deps, _ = unwrap_inout(arg)
        pos = kwpos isa Symbol ? kwpos : idx
        for (dep_mod, readdep, writedep) in deps
            if arg isa DTask
                if arg.uid in keys(dspec.uid_to_id)
                    # Within-DAG dependency, bail out
                    return false
                end
            end
        end
    end

    add_vertex!(dspec.g)
    id = nv(dspec.g)

    # Record function signature
    dspec.id_to_functype[id] = typeof(tspec.f)
    argtypes = DatadepsArgSpec[]
    for (idx, (kwpos, arg)) in enumerate(tspec.args)
        arg, deps, _ = unwrap_inout(arg)
        pos = kwpos isa Symbol ? kwpos : idx
        for (dep_mod, readdep, writedep) in deps
            if arg isa DTask
                #= TODO: Re-enable this when we can handle within-DAG dependencies
                if arg.uid in keys(dspec.uid_to_id)
                    # Within-DAG dependency
                    arg_id = dspec.uid_to_id[arg.uid]
                    push!(dspec.id_to_argtypes[arg_id], DatadepsArgSpec(pos, DTaskDAGID{arg_id}, dep_mod, UnknownAliasing()))
                    add_edge!(dspec.g, arg_id, id)
                    continue
                end
                =#

                # External DTask, so fetch this and track it as a raw value
                arg = fetch(arg; raw=true)
            end
            ainfo = aliasing(arg, dep_mod)
            push!(argtypes, DatadepsArgSpec(pos, typeof(arg), dep_mod, ainfo))
        end
    end
    dspec.id_to_argtypes[id] = argtypes

    # FIXME: Also record some portion of options
    # FIXME: Record syncdeps
    dspec.id_to_uid[id] = task.uid
    dspec.uid_to_id[task.uid] = id

    return true
end
function dag_has_task(dspec::DAGSpec, task::DTask)
    return task.uid in keys(dspec.uid_to_id)
end
function Base.:(==)(dspec1::DAGSpec, dspec2::DAGSpec)
    # Are the graphs the same size?
    nv(dspec1.g) == nv(dspec2.g) || return false
    ne(dspec1.g) == ne(dspec2.g) || return false

    for id in 1:nv(dspec1.g)
        # Are all the vertices the same?
        id in keys(dspec2.id_to_uid) || return false
        id in keys(dspec2.id_to_functype) || return false
        id in keys(dspec2.id_to_argtypes) || return false

        # Are all the edges the same?
        inneighbors(dspec1.g, id) == inneighbors(dspec2.g, id) || return false
        outneighbors(dspec1.g, id) == outneighbors(dspec2.g, id) || return false

        # Are function types the same?
        dspec1.id_to_functype[id] === dspec2.id_to_functype[id] || return false

        # Are argument types/relative dependencies the same?
        for argspec1 in dspec1.id_to_argtypes[id]
            # Is this argument position present in both?
            argspec2_idx = findfirst(argspec2->argspec1.pos == argspec2.pos, dspec2.id_to_argtypes[id])
            argspec2_idx === nothing && return false
            argspec2 = dspec2.id_to_argtypes[id][argspec2_idx]

            # Are the arguments the same?
            argspec1.value_type === argspec2.value_type || return false
            argspec1.dep_mod === argspec2.dep_mod || return false
            if !equivalent_structure(argspec1.ainfo, argspec2.ainfo)
                @show argspec1.ainfo argspec2.ainfo
                return false
            end
        end
    end

    return true
end

struct DAGSpecSchedule
    id_to_proc::Dict{Int, Processor}
    DAGSpecSchedule() = new(Dict{Int, Processor}())
end

#const DAG_SPECS = Vector{DAGSpec}()
const DAG_SPECS = Vector{Pair{DAGSpec, DAGSpecSchedule}}()

#const DAG_SCHEDULE_CACHE = Dict{DAGSpec, DAGSpecSchedule}()

struct DataDepsAliasingState
    # Track original and current data locations
    # We track data => space
    data_origin::Dict{AbstractAliasing,MemorySpace}
    data_locality::Dict{AbstractAliasing,MemorySpace}

    # Track data => modifier
    data_format::Dict{AbstractAliasing,AbstractFormatModifier}

    # Track writers ("owners") and readers
    ainfos_owner::Dict{AbstractAliasing,Union{Pair{DTask,Int},Nothing}}
    ainfos_readers::Dict{AbstractAliasing,Vector{Pair{DTask,Int}}}
    ainfos_overlaps::Dict{AbstractAliasing,Set{AbstractAliasing}}

    # The data-dependency graph of all tasks
    g::SimpleDiGraph{Int}
    # The mapping from task to graph ID
    task_to_id::IdDict{DTask,Int}

    # Cache ainfo lookups
    ainfo_cache::Dict{Tuple{Any,Any},AbstractAliasing}

    function DataDepsAliasingState()
        data_origin = Dict{AbstractAliasing,MemorySpace}()
        data_locality = Dict{AbstractAliasing,MemorySpace}()

        data_format = Dict{AbstractAliasing,AbstractFormatModifier}()

        ainfos_owner = Dict{AbstractAliasing,Union{Pair{DTask,Int},Nothing}}()
        ainfos_readers = Dict{AbstractAliasing,Vector{Pair{DTask,Int}}}()
        ainfos_overlaps = Dict{AbstractAliasing,Set{AbstractAliasing}}()

        g = SimpleDiGraph()
        task_to_id = IdDict{DTask,Int}()

        ainfo_cache = Dict{Tuple{Any,Any},AbstractAliasing}()

        return new(data_origin, data_locality, data_format,
                   ainfos_owner, ainfos_readers, ainfos_overlaps,
                   g, task_to_id, ainfo_cache)
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

    # The set of processors to schedule on
    all_procs::Vector{Processor}

    # The ordered list of tasks and their read/write dependencies
    dependencies::Vector{Pair{DTask,Vector{Tuple{Bool,Bool,<:AbstractAliasing,<:Any,<:Any}}}}

    # The mapping of memory space to remote argument copies
    remote_args::Dict{MemorySpace,IdDict{Any,Dict{AbstractFormatModifier,Any}}}

    # The aliasing analysis state
    alias_state::State

    # The DAG specification
    dag_spec::DAGSpec

    function DataDepsState(aliasing::Bool, all_procs::Vector{Processor})
        dependencies = Pair{DTask,Vector{Tuple{Bool,Bool,<:AbstractAliasing,<:Any,<:Any}}}[]
        remote_args = Dict{MemorySpace,IdDict{Any,Dict{AbstractFormatModifier,Any}}}()
        if aliasing
            state = DataDepsAliasingState()
        else
            state = DataDepsNonAliasingState()
        end
        spec = DAGSpec()
        return new{typeof(state)}(aliasing, all_procs, dependencies, remote_args, state, spec)
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
function populate_task_info!(state::DataDepsState, spec::DTaskSpec, task::DTask, write_num::Int)
    astate = state.alias_state
    g, task_to_id = astate.g, astate.task_to_id
    if !haskey(task_to_id, task)
        add_vertex!(g)
        task_to_id[task] = nv(g)
    end

    # Populate task dependencies
    dependencies_to_add = Vector{Tuple{Bool,Bool,AbstractAliasing,<:Any,<:Any}}()

    # Track the task's arguments and access patterns
    for (idx, (pos, arg)) in enumerate(spec.args)
        # Unwrap In/InOut/Out wrappers and record dependencies
        arg, deps, _ = unwrap_inout(arg)

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
        populate_argument_info!(state, arg, deps, task, write_num)
    end

    # Track the task result too
    # N.B. We state no readdep/writedep because, while we can't model the aliasing info for the task result yet, we don't want to synchronize because of this
    push!(dependencies_to_add, (false, false, UnknownAliasing(), identity, task))

    # Record argument/result dependencies
    push!(state.dependencies, task => dependencies_to_add)

    return write_num + 1
end
function populate_argument_info!(state::DataDepsState{DataDepsAliasingState}, arg, deps, task, write_num)
    astate = state.alias_state
    g = astate.g
    task_to_id = astate.task_to_id
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

            # Assign data owner and locality
            if !haskey(astate.data_locality, ainfo)
                astate.data_locality[ainfo] = memory_space(arg)
                astate.data_origin[ainfo] = memory_space(arg)
                astate.data_format[ainfo] = NoModifier()
            end
        end

        # Calculate AOT task-to-task dependencies
        syncdeps = Set{DTask}()
        if writedep
            get_write_deps!(state, ainfo, task, write_num, syncdeps)
            add_writer!(state, ainfo, task, write_num)
        else
            get_read_deps!(state, ainfo, task, write_num, syncdeps)
            add_reader!(state, ainfo, task, write_num)
        end
        for syncdep in syncdeps
            add_edge!(g, task_to_id[syncdep], task_to_id[task])
        end
    end
end
function populate_argument_info!(state::DataDepsState{DataDepsNonAliasingState}, arg, deps)
    error("FIXME")
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
function clear_ainfo_owner_readers!(astate::DataDepsAliasingState)
    for ainfo in keys(astate.ainfos_owner)
        astate.ainfos_owner[ainfo] = nothing
        empty!(astate.ainfos_readers[ainfo])
    end
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
function generate_slot!(state::DataDepsState, dest_space, data, format_mod)
    if data isa DTask
        data = fetch(data; raw=true)
    end
    orig_space = memory_space(data)
    to_proc = first(processors(dest_space))
    from_proc = first(processors(orig_space))
    dest_space_args = get!(Dict{AbstractFormatModifier,Any}, get!(IdDict{Any,Dict{AbstractFormatModifier,Any}}, state.remote_args, dest_space), data)
    if orig_space == dest_space
        data_chunk = tochunk(modify_data_unwrap(data, format_mod), from_proc)
        dest_space_args[format_mod] = data_chunk
        @assert processor(data_chunk) in processors(dest_space) || data isa Chunk && processor(data) isa Dagger.OSProc
        @assert memory_space(data_chunk) == orig_space
    else
        w = only(unique(map(get_parent, collect(processors(dest_space))))).pid
        ctx = Sch.eager_context()
        id = rand(Int)
        timespan_start(ctx, :move, (;thunk_id=0, id, position=0, processor=to_proc), (;f=nothing, data))
        dest_space_args[format_mod] = remotecall_fetch(w, from_proc, to_proc, data) do from_proc, to_proc, data
            data_converted = modify_data_unwrap(move(from_proc, to_proc, data), format_mod)
            data_chunk = tochunk(data_converted, to_proc)
            @assert processor(data_chunk) in processors(dest_space)
            @assert memory_space(data_converted) == memory_space(data_chunk) "space mismatch! $(memory_space(data_converted)) != $(memory_space(data_chunk)) ($(typeof(data_converted)) vs. $(typeof(data_chunk))), spaces ($orig_space -> $dest_space)"
            @assert orig_space != memory_space(data_chunk) "space preserved! $orig_space != $(memory_space(data_chunk)) ($(typeof(data)) vs. $(typeof(data_chunk))), spaces ($orig_space -> $dest_space)"
            return data_chunk
        end
        timespan_finish(ctx, :move, (;thunk_id=0, id, position=0, processor=to_proc), (;f=nothing, data=dest_space_args[data]))
    end
    return dest_space_args[format_mod]
end
function get_or_generate_slot!(state::DataDepsState, dest_space::MemorySpace, data, format_mod)
    if !haskey(state.remote_args, dest_space)
        state.remote_args[dest_space] = IdDict{Any,Dict{AbstractFormatModifier,Any}}()
    end
    if !haskey(state.remote_args[dest_space], data)
        state.remote_args[dest_space][data] = Dict{AbstractFormatModifier,Any}()
    end
    if !haskey(state.remote_args[dest_space][data], format_mod)
        return generate_slot!(state, dest_space, data, format_mod)
    end
    return state.remote_args[dest_space][data][format_mod]
end

struct RoundRobinScheduler end
function datadeps_create_schedule(::RoundRobinScheduler, state, specs_tasks)
    astate = state.alias_state
    nprocs = length(state.all_procs)
    id_to_proc = Dict(i => p for (i, p) in enumerate(state.all_procs))

    task_to_proc = Dict{DTask, Processor}()
    for (idx, (_, task)) in enumerate(specs_tasks)
        proc_idx = mod1(idx, nprocs)
        task_to_proc[task] = id_to_proc[proc_idx]
    end

    return task_to_proc
end

struct RandomScheduler end
function datadeps_create_schedule(::RandomScheduler, state, specs_tasks)
    astate = state.alias_state
    nprocs = length(state.all_procs)
    id_to_proc = Dict(i => p for (i, p) in enumerate(state.all_procs))

    task_to_proc = Dict{DTask, Processor}()
    for (_, task) in specs_tasks
        proc_idx = rand(1:nprocs)
        task_to_proc[task] = id_to_proc[proc_idx]
    end

    return task_to_proc
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

    upper_queue = get_options(:task_queue)

    state = DataDepsState(queue.aliasing, all_procs)
    astate = state.alias_state

    schedule = Dict{DTask, Processor}()

    if DATADEPS_SCHEDULE_REUSABLE[]
        # Compute DAG spec
        for (spec, task) in queue.seen_tasks
            if !dag_add_task!(state.dag_spec, spec, task)
                # This task needs to be deferred
                break
            end
        end

        # Find any matching DAG specs and reuse their schedule
        for (other_spec, spec_schedule) in DAG_SPECS
            if other_spec == state.dag_spec
                @info "Found matching DAG spec!"
                #spec_schedule = DAG_SCHEDULE_CACHE[other_spec]
                schedule = Dict{DTask, Processor}()
                for (id, proc) in spec_schedule.id_to_proc
                    uid = state.dag_spec.id_to_uid[id]
                    task_idx = findfirst(spec_task -> spec_task[2].uid == uid, queue.seen_tasks)
                    task = queue.seen_tasks[task_idx][2]
                    schedule[task] = proc
                end
                break
            end
        end
    end

    # Populate all task dependencies
    write_num = 1
    task_num = 0
    for (spec, task) in queue.seen_tasks
        if !dag_has_task(state.dag_spec, task)
            # This task needs to be deferred
            break
        end
        write_num = populate_task_info!(state, spec, task, write_num)
        task_num += 1
    end
    @assert task_num > 0

    if isempty(schedule)
        # Run AOT scheduling
        schedule = datadeps_create_schedule(queue.scheduler, state, queue.seen_tasks[1:task_num])::Dict{DTask, Processor}

        if DATADEPS_SCHEDULE_REUSABLE[]
            # Cache the schedule
            spec_schedule = DAGSpecSchedule()
            for (task, proc) in schedule
                id = state.dag_spec.uid_to_id[task.uid]
                spec_schedule.id_to_proc[id] = proc
            end
            #DAG_SCHEDULE_CACHE[state.dag_spec] = spec_schedule
            push!(DAG_SPECS, state.dag_spec => spec_schedule)
        end
    end

    # Clear out ainfo database (will be repopulated during task execution)
    clear_ainfo_owner_readers!(astate)

    # Launch tasks and necessary copies
    write_num = 1
    for (spec, task) in queue.seen_tasks
        if !dag_has_task(state.dag_spec, task)
            # This task needs to be deferred
            break
        end

        our_proc = schedule[task]
        @assert our_proc in all_procs
        our_space = only(memory_spaces(our_proc))
        our_procs = filter(proc->proc in all_procs, collect(processors(our_space)))
        our_scope = UnionScope(map(ExactScope, our_procs)...)

        #format_mod = get(spec.options, :datadeps_format_mod, NoModifier())::AbstractFormatModifier
        #spec.options = NamedTuple(filter(opt->opt[1] != :datadeps_format_mod, Base.pairs(spec.options)))

        spec.f = move(ThreadProc(myid(), 1), our_proc, spec.f)
        @dagdebug nothing :spawn_datadeps "($(repr(spec.f))) Scheduling: $our_proc ($our_space)"

        # Copy raw task arguments for analysis
        task_args = copy(spec.args)

        # Copy args from local to remote
        for (idx, (pos, arg)) in enumerate(task_args)
            # Is the data written previously or now?
            arg, deps, format_mod = unwrap_inout(arg)
            arg = arg isa DTask ? fetch(arg; raw=true) : arg
            if !type_may_alias(typeof(arg))
                @dagdebug nothing :spawn_datadeps "($(repr(spec.f)))[$idx] Skipped copy-to (unwritten)"
                spec.args[idx] = pos => modify_data(arg, format_mod)
                continue
            end

            # Is the source of truth elsewhere?
            arg_remote = get_or_generate_slot!(state, our_space, arg, format_mod)
            if queue.aliasing
                for (dep_mod, _, _) in deps
                    ainfo = aliasing(astate, arg, dep_mod)
                    data_space = astate.data_locality[ainfo]
                    data_format = astate.data_format[ainfo]
                    nonlocal = our_space != data_space || format_mod != data_format
                    if nonlocal
                        # Add copy-to operation (depends on latest owner of arg)
                        @dagdebug nothing :spawn_datadeps "($(repr(spec.f)))[$idx][$dep_mod] Enqueueing copy-to: $data_space[$data_format] => $our_space[$format_mod]"
                        arg_local = get_or_generate_slot!(state, data_space, arg, format_mod)
                        copy_to_scope = our_scope
                        copy_to_syncdeps = Set{Any}()
                        get_write_deps!(state, ainfo, task, write_num, copy_to_syncdeps)
                        @dagdebug nothing :spawn_datadeps "($(repr(spec.f)))[$idx][$dep_mod] $(length(copy_to_syncdeps)) syncdeps"
                        copy_to = Dagger.@spawn scope=copy_to_scope syncdeps=copy_to_syncdeps meta=true Dagger.move!(dep_mod, our_space, data_space, arg_remote, arg_local)
                        add_writer!(state, ainfo, copy_to, write_num)

                        astate.data_locality[ainfo] = our_space
                        astate.data_format[ainfo] = format_mod
                    else
                        @dagdebug nothing :spawn_datadeps "($(repr(spec.f)))[$idx][$dep_mod] Skipped copy-to (local): $data_space[$data_format]"
                    end
                end
            else
                data_space = astate.data_locality[arg]
                data_format = astate.data_format[arg]
                nonlocal = our_space != data_space || format_mod != data_format
                if nonlocal
                    # Add copy-to operation (depends on latest owner of arg)
                    @dagdebug nothing :spawn_datadeps "($(repr(spec.f)))[$idx] Enqueueing copy-to: $data_space[$data_format] => $our_space[$format_mod]"
                    arg_local = get_or_generate_slot!(state, data_space, arg, format_mod)
                    copy_to_scope = our_scope
                    copy_to_syncdeps = Set{Any}()
                    get_write_deps!(state, arg, task, write_num, copy_to_syncdeps)
                    @dagdebug nothing :spawn_datadeps "($(repr(spec.f)))[$idx] $(length(copy_to_syncdeps)) syncdeps"
                    copy_to = Dagger.@spawn scope=copy_to_scope syncdeps=copy_to_syncdeps meta=true Dagger.move!(identity, our_space, data_space, arg_remote, arg_local)
                    add_writer!(state, arg, copy_to, write_num)

                    astate.data_locality[arg] = our_space
                    astate.data_format[arg] = format_mod
                else
                    @dagdebug nothing :spawn_datadeps "($(repr(spec.f)))[$idx] Skipped copy-to (local): $data_space[$data_format]"
                end
            end
            spec.args[idx] = pos => arg_remote
        end
        write_num += 1

        # Validate that we're not accidentally performing a copy
        for (idx, (_, arg)) in enumerate(spec.args)
            _, deps, _ = unwrap_inout(task_args[idx][2])
            if is_writedep(arg, deps, task)
                arg_space = memory_space(arg)
                @assert arg_space == our_space "($(repr(spec.f)))[$idx] Tried to pass $(typeof(arg)) from $arg_space to $our_space"
            end
        end

        # Calculate this task's syncdeps
        syncdeps = get(Set{Any}, spec.options, :syncdeps)
        for (idx, (_, arg)) in enumerate(task_args)
            arg, deps, _ = unwrap_inout(arg)
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
            arg, deps, _ = unwrap_inout(arg)
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
    end

    # Remove processed tasks
    deleteat!(queue.seen_tasks, 1:task_num)

    # Copy args from remote to local
    if queue.aliasing
        # We need to replay the writes from all tasks in-order (skipping any
        # outdated write owners), to ensure that overlapping writes are applied
        # in the correct order

        # First, find the latest owners of each live ainfo
        arg_writes = IdDict{Any,Vector{Tuple{AbstractAliasing,<:Any,MemorySpace,AbstractFormatModifier}}}()
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
                ainfo_writes = get!(Vector{Tuple{AbstractAliasing,<:Any,MemorySpace,AbstractFormatModifier}}, arg_writes, arg)

                #= FIXME: If we fully overlap any writer, evict them
                idxs = findall(ainfo_write->overlaps_all(ainfo, ainfo_write[1]), ainfo_writes)
                deleteat!(ainfo_writes, idxs)
                =#

                # Make ourselves the latest writer
                push!(ainfo_writes, (ainfo, dep_mod, astate.data_locality[ainfo], astate.data_format[ainfo]))
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
            for (ainfo, dep_mod, data_remote_space, data_remote_format) in ainfo_writes
                # Is the source of truth elsewhere?
                data_local_space = astate.data_origin[ainfo]
                if data_local_space != data_remote_space || data_remote_format != NoModifier()
                    # Add copy-from operation
                    @dagdebug nothing :spawn_datadeps "[$dep_mod] Enqueueing copy-from: $data_remote_space[$data_remote_format] => $data_local_space[NoModifier()]"
                    arg_local = get_or_generate_slot!(state, data_local_space, arg, NoModifier())
                    arg_remote = state.remote_args[data_remote_space][arg][data_remote_format]
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
            arg, deps, format_mod = unwrap_inout(arg)
            if !type_may_alias(typeof(arg)) || !has_writedep(state, arg, deps)
                @dagdebug nothing :spawn_datadeps "Skipped copy-from (unwritten)"
            end

            # Is the source of truth elsewhere?
            data_remote_space = astate.data_locality[arg]
            data_remote_format = astate.data_format[arg]
            data_local_space = astate.data_origin[arg]
            if data_local_space != data_remote_space || data_remote_format != NoModifier()
                # Add copy-from operation
                @dagdebug nothing :spawn_datadeps "Enqueueing copy-from: $data_remote_space[$data_remote_format] => $data_local_space[NoModifier()]"
                arg_local = state.remote_args[data_local_space][arg][format_mod]
                arg_remote = state.remote_args[data_remote_space][arg][format_mod]
                @assert arg_remote !== arg_local
                data_local_proc = first(processors(data_local_space))
                copy_from_scope = ExactScope(data_local_proc)
                copy_from_syncdeps = Set()
                get_write_deps!(state, arg, nothing, write_num, copy_from_syncdeps)
                @dagdebug nothing :spawn_datadeps "$(length(copy_from_syncdeps)) syncdeps"
                copy_from = Dagger.@spawn scope=copy_from_scope syncdeps=copy_from_syncdeps meta=true Dagger.move!(identity, data_local_space, data_remote_space, arg_local, arg_remote)
            else
                @dagdebug nothing :spawn_datadeps "Skipped copy-from (local): $data_remote_space[$data_remote_format]"
            end
        end
    end
end

"""
    spawn_datadeps(f::Base.Callable)

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
"""
function spawn_datadeps(f::Base.Callable; static::Bool=true,
                        scheduler=nothing,
                        aliasing::Bool=true,
                        launch_wait::Union{Bool,Nothing}=nothing)
    if !static
        throw(ArgumentError("Dynamic scheduling is no longer available"))
    end
    wait_all(; check_errors=true) do
        scheduler = something(scheduler, DATADEPS_SCHEDULER[], RoundRobinScheduler())
        launch_wait = something(launch_wait, DATADEPS_LAUNCH_WAIT[], false)::Bool
        local result
        if launch_wait
            spawn_bulk() do
                queue = DataDepsTaskQueue(get_options(:task_queue);
                                          scheduler, aliasing)
                result = with_options(f; task_queue=queue)
                while !isempty(queue.seen_tasks)
                    @dagdebug nothing :spawn_datadeps "Entering Datadeps region"
                    distribute_tasks!(queue)
                end
            end
        else
            queue = DataDepsTaskQueue(get_options(:task_queue);
                                      scheduler, aliasing)
            result = with_options(f; task_queue=queue)
            while !isempty(queue.seen_tasks)
                @dagdebug nothing :spawn_datadeps "Entering Datadeps region"
                distribute_tasks!(queue)
            end
        end
        return result
    end
end
const DATADEPS_SCHEDULER = ScopedValue{Any}(nothing)
const DATADEPS_SCHEDULE_REUSABLE = ScopedValue{Bool}(true)
const DATADEPS_LAUNCH_WAIT = ScopedValue{Union{Bool,Nothing}}(nothing)
