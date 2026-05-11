### DAG Analysis ###

Base.length(spec::DAGSpec) = nv(spec.g)
Base.isempty(spec::DAGSpec) = length(spec) == 0

function dag_add_task!(dspec::DAGSpec, state, tspec::DTaskSpec, task::DTask)
    # Check if this task depends on any other tasks within the DAG,
    # which we are not yet ready to handle
    for (idx, _arg) in enumerate(tspec.fargs)
        arg, deps = unwrap_inout(value(_arg))
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
    dspec.id_to_functype[id] = chunktype(tspec.fargs[1])
    argtypes = DatadepsArgSpec[]
    for (idx, _arg) in enumerate(tspec.fargs)
        arg, deps = unwrap_inout(value(_arg))
        pos = raw_position(_arg)
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
            # FIXME: Generate syncdeps and add edges
            push!(argtypes, DatadepsArgSpec(pos, typeof(arg), dep_mod, ainfo))
        end
    end
    dspec.id_to_argtypes[id] = argtypes

    # FIXME: Also record some portion of options
    # FIXME: Record syncdeps
    dspec.id_to_uid[id] = task.uid
    dspec.uid_to_id[task.uid] = id
    dspec.id_to_spec[id] = tspec
    dspec.id_to_task[id] = task

    return true
end
function dag_build_edges!(dag_spec::DAGSpec)
    # Naively build edges based on exact argument comparisons (no aliasing)
    arg_writes = Dict{Any,Int}()
    for idx in 1:nv(dag_spec.g)
        task_spec = dag_spec.id_to_spec[idx]

        # Get the raw arguments for this task
        task_raw_args = Vector{Any}()
        for arg in task_spec.fargs
            arg, deps = unwrap_inout(arg)
            for (dep_mod, readdep, writedep) in deps
                # Get the raw argument
                raw_arg = arg isa DTask ? fetch(arg; raw=true) : arg

                # Did any previous task write to this argument?
                if haskey(arg_writes, raw_arg) && arg_writes[raw_arg] != idx
                    prev_task_id = arg_writes[raw_arg]
                    add_edge!(dag_spec.g, prev_task_id, idx)
                end

                if writedep
                    # Record this write
                    arg_writes[raw_arg] = idx
                end
            end
        end
    end
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
            equivalent_structure(argspec1.ainfo, argspec2.ainfo) || return false
        end
    end

    return true
end

struct DAGSpecSchedule
    id_to_proc::Dict{Int, Processor}
    DAGSpecSchedule() = new(Dict{Int, Processor}())
end

const DATADEPS_DAG_SPECS = TaskLocalValue{Vector{Pair{DAGSpec, DAGSpecSchedule}}}(()->Vector{Pair{DAGSpec, DAGSpecSchedule}}())

### JIT Schedulers ###

mutable struct RoundRobinScheduler <: DataDepsScheduler
    proc_idx::Int
    RoundRobinScheduler() = new(1)
end
function datadeps_schedule_task_jit!(sched::RoundRobinScheduler, all_procs, all_scope, task_scope, spec::DTaskSpec, task::DTask)
    proc_idx = sched.proc_idx
    our_proc = all_procs[proc_idx]
    if task_scope == all_scope
        # all_procs is already limited to scope
    else
        if isa(constrain(task_scope, all_scope), InvalidScope)
            throw(Sch.SchedulingException("Scopes are not compatible: $(all_scope), $(task_scope)"))
        end
        while !proc_in_scope(our_proc, task_scope)
            proc_idx = mod1(proc_idx + 1, length(all_procs))
            our_proc = all_procs[proc_idx]
        end
    end
    proc_idx = mod1(proc_idx + 1, length(all_procs))
    sched.proc_idx = proc_idx
    return our_proc
end

struct NaiveScheduler <: DataDepsScheduler end
function datadeps_schedule_task_jit!(sched::NaiveScheduler, all_procs, all_scope, task_scope, spec::DTaskSpec, task::DTask)
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
    return our_proc
end

struct UltraScheduler <: DataDepsScheduler
    task_to_spec::Dict{DTask,DTaskSpec}
    assignments::Dict{DTask,MemorySpace}
    dependencies::Dict{DTask,Set{DTask}}
    task_completions::Dict{DTask,UInt64}
    space_completions::Dict{MemorySpace,UInt64}
    capacities::Dict{MemorySpace,Int}

    function UltraScheduler()
        return new(Dict{DTask,DTaskSpec}(),
                    Dict{DTask,MemorySpace}(),
                    Dict{DTask,Set{DTask}}(),
                    Dict{DTask,UInt64}(),
                    Dict{MemorySpace,UInt64}(),
                    Dict{MemorySpace,Int}())
    end
end
function datadeps_schedule_task_jit!(sched::UltraScheduler, all_procs, all_scope, task_scope, spec::DTaskSpec, task::DTask)
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
    deps = @something(spec.options.syncdeps, Set{ThunkSyncdep}())

    # Find latest time-to-completion of all syncdeps
    deps_completed = UInt64(0)
    for dep in deps
        haskey(sched.task_completions, dep) || continue # copy deps aren't recorded
        deps_completed = max(deps_completed, sched.task_completions[dep])
    end

    # Find latest time-to-completion of each memory space
    # FIXME: Figure out space completions based on optimal packing
    spaces_completed = Dict{MemorySpace,UInt64}()
    for space in exec_spaces
        completed = UInt64(0)
        for (task, other_space) in sched.assignments
            space == other_space || continue
            completed = max(completed, sched.task_completions[task])
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

    sched.task_to_spec[task] = spec
    sched.assignments[task] = our_space
    sched.task_completions[task] = our_space_completed + move_time + task_time

    return our_proc
end

### AOT Schedulers ###

function datadeps_schedule_dag_aot!(scheduler, schedule, dag_spec, all_procs, all_scope)
    # Fallback to JIT scheduling (done in distribute_task!)
    return
end

struct LayeredScheduler <: DataDepsScheduler end
function datadeps_schedule_dag_aot!(scheduler::LayeredScheduler, schedule, dag_spec, all_procs, all_scope)
    layer = 1
    layer_data = Vector{Any}()
    layers = Vector{Vector{Int}}()
    push!(layers, Vector{Int}())
    for idx in 1:nv(dag_spec.g)
        spec = dag_spec.id_to_spec[idx]

        # Get the raw arguments for this task
        task_raw_args = Vector{Any}()
        for arg in spec.fargs
            arg, deps = unwrap_inout(arg)
            for (dep_mod, readdep, writedep) in deps
                # We only care about write dependencies
                writedep || continue

                # Get the raw argument
                raw_arg = arg isa DTask ? fetch(arg; raw=true) : arg

                push!(task_raw_args, raw_arg)
            end
        end

        # Determine if this task stays in this layer
        if any(raw_arg -> raw_arg in layer_data, task_raw_args)
            # This argument is already written by a previous task in this layer
            # Generate new layer
            layer += 1
            empty!(layer_data)
            push!(layers, Vector{Int}())
        end

        # Add our data and this task to the current layer
        append!(layer_data, task_raw_args)
        push!(layers[layer], idx)
    end

    # Perform round-robin scheduling within each layer
    for layer in layers
        sub_scheduler = RoundRobinScheduler()
        for idx in layer
            spec = dag_spec.id_to_spec[idx]
            task = dag_spec.id_to_task[idx]
            task_scope = @something(spec.options.compute_scope, spec.options.scope, DefaultScope())
            our_proc = datadeps_schedule_task_jit!(sub_scheduler, all_procs, all_scope, task_scope, spec, task)
            schedule[task] = our_proc
        end
    end
end