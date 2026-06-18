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
    dspec.id_to_scope[id] = @something(tspec.options.compute_scope,
                                       tspec.options.scope,
                                       DefaultScope())

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

### DAGSpec Equivalence (scheduler-dispatched) ###

"""
    datadeps_dag_equivalent(scheduler, dspec1::DAGSpec, dspec2::DAGSpec) -> Bool

Returns `true` if a schedule cached for `dspec2` may safely be reused for
`dspec1`, as judged by `scheduler`. This is the top-level entry point used by
`distribute_tasks!` when looking up a cached schedule.

The default implementation requires:
- Same number of vertices and edges in the dependency graph
- Identical edge sets (per-vertex `outneighbors`)
- Per-vertex agreement on function type and on each task's compute scope
- Per-vertex agreement on the multiset of argspecs, compared via
  `datadeps_argspec_equivalent`

Schedulers that want completely custom matching (or want to opt out of caching
entirely) can override this directly. Schedulers that only want to tweak how
individual arguments are compared should instead override
`datadeps_argspec_equivalent` or `datadeps_ainfo_equivalent`.
"""
function datadeps_dag_equivalent(scheduler::DataDepsScheduler,
                                 dspec1::DAGSpec, dspec2::DAGSpec)
    # Graph shape
    nv(dspec1.g) == nv(dspec2.g) || return false
    ne(dspec1.g) == ne(dspec2.g) || return false

    @inbounds for id in 1:nv(dspec1.g)
        # outneighbors covers all edges; inneighbors would be redundant
        outneighbors(dspec1.g, id) == outneighbors(dspec2.g, id) || return false

        # Function type must match exactly
        dspec1.id_to_functype[id] === dspec2.id_to_functype[id] || return false

        # Per-task compute scope must match (different scopes can produce
        # different schedules and must not be aliased)
        dspec1.id_to_scope[id] == dspec2.id_to_scope[id] || return false

        # Argspecs must match as a multiset (Deps can put multiple argspecs at
        # the same position)
        _argspecs_equivalent(scheduler,
                             dspec1.id_to_argtypes[id],
                             dspec2.id_to_argtypes[id]) || return false
    end

    return true
end

# Backwards-compatible default `==`: use the no-scheduler default (i.e. as if
# all schedulers behaved like the base `DataDepsScheduler`). The runtime path
# in `distribute_tasks!` calls `datadeps_dag_equivalent` directly.
Base.:(==)(dspec1::DAGSpec, dspec2::DAGSpec) =
    datadeps_dag_equivalent(_DefaultEquivalenceScheduler(), dspec1, dspec2)

# A private marker scheduler used to provide a default for `Base.:(==)` on
# `DAGSpec`. Not exported and not intended for direct use.
struct _DefaultEquivalenceScheduler <: DataDepsScheduler end

"""
    datadeps_argspec_equivalent(scheduler,
                                a1::DatadepsArgSpec,
                                a2::DatadepsArgSpec) -> Bool

Returns `true` if argspecs `a1` and `a2` are interchangeable for the purposes
of `scheduler`'s cached-schedule lookup. The default requires equal positions,
equal value types, equal dep_mods, and structurally-equivalent ainfos (per
`datadeps_ainfo_equivalent`).
"""
function datadeps_argspec_equivalent(scheduler::DataDepsScheduler,
                                     a1::DatadepsArgSpec, a2::DatadepsArgSpec)
    a1.pos == a2.pos || return false
    a1.value_type === a2.value_type || return false
    a1.dep_mod === a2.dep_mod || return false
    return datadeps_ainfo_equivalent(scheduler, a1.ainfo, a2.ainfo)
end

"""
    datadeps_ainfo_equivalent(scheduler,
                              a1::AbstractAliasing,
                              a2::AbstractAliasing) -> Bool

Returns `true` if aliasings `a1` and `a2` are interchangeable for the purposes
of `scheduler`'s cached-schedule lookup. The default uses
`equivalent_structure`, which compares shape/strides/lengths while ignoring
absolute pointer addresses, enabling reuse across re-allocations.

Schedulers can override this to choose a different equivalence strategy, e.g.
pointer-identical (strictest), locality-only (memory-space only), or fully
permissive.
"""
datadeps_ainfo_equivalent(::DataDepsScheduler,
                          a1::AbstractAliasing, a2::AbstractAliasing) =
    equivalent_structure(a1, a2)

# Compare two argspec vectors as multisets, since `Deps` can place multiple
# argspecs at the same position. We pair each argspec in `as1` with a not-yet-
# matched argspec in `as2`; both lists must be exhausted simultaneously.
function _argspecs_equivalent(scheduler::DataDepsScheduler,
                              as1::Vector{DatadepsArgSpec},
                              as2::Vector{DatadepsArgSpec})
    length(as1) == length(as2) || return false
    n = length(as1)
    n == 0 && return true
    matched = falses(n)
    @inbounds for a1 in as1
        found = false
        for j in 1:n
            matched[j] && continue
            if datadeps_argspec_equivalent(scheduler, a1, as2[j])
                matched[j] = true
                found = true
                break
            end
        end
        found || return false
    end
    return true
end

### Schedule Cache (scheduler-owned) ###

struct DAGSpecSchedule
    id_to_proc::Dict{Int, Processor}
    DAGSpecSchedule() = new(Dict{Int, Processor}())
end

# Per-scheduler-type cache. Each entry in the inner Vector is a (DAGSpec =>
# DAGSpecSchedule) pair recorded by a prior call. The outer Dict partitions
# the cache by `typeof(scheduler)` so schedulers don't contaminate each other.
const DATADEPS_DAG_SPECS =
    TaskLocalValue{Dict{Type, Vector{Pair{DAGSpec, DAGSpecSchedule}}}}(
        ()->Dict{Type, Vector{Pair{DAGSpec, DAGSpecSchedule}}}())

"""
    datadeps_schedule_cache(scheduler) -> Vector{Pair{DAGSpec, DAGSpecSchedule}}

Returns the schedule cache that `scheduler` should consult for prior schedules
and append newly-computed schedules to. The default implementation returns a
task-local, per-scheduler-type cache.

Override this to implement custom caching strategies (e.g., bounded LRU, no
cache at all, cross-task shared cache).
"""
function datadeps_schedule_cache(scheduler::DataDepsScheduler)
    cache_by_type = DATADEPS_DAG_SPECS[]
    return get!(Vector{Pair{DAGSpec, DAGSpecSchedule}},
                cache_by_type, typeof(scheduler))
end

### JIT Schedulers ###

# Default for user-defined schedulers with a zero-arg constructor. Schedulers
# that carry mutable state should specialize `similar` to return a fresh shard
# (used when hierarchical scheduling clones a scheduler per partition).
Base.similar(s::DataDepsScheduler) = typeof(s)()

mutable struct RoundRobinScheduler <: DataDepsScheduler
    proc_idx::Int
    RoundRobinScheduler() = new(1)
end
Base.similar(::RoundRobinScheduler) = RoundRobinScheduler()
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
Base.similar(::NaiveScheduler) = NaiveScheduler()
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
Base.similar(::UltraScheduler) = UltraScheduler()
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
            return lock(sch_state.signature_time_cost) do stc; get(stc, sig, 1000^3); end
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

const GREEDY_DEFAULT_RUNTIME_NS = UInt64(1_000_000_000)
const GREEDY_DEFAULT_TRANSFER_RATE = UInt64(1_000_000)
const GREEDY_DEFAULT_OUTPUT_SIZE = UInt64(1_048_576)

"""
    GreedyScheduler <: DataDepsScheduler

A list-scheduling heuristic that assigns each task in topological order to the
processor minimizing its estimated finish time. The cost model uses
`metrics_lookup_runtime_median` for compute and per-input data-ready times built
from chunk sizes and per-(source, destination) transfer rates from
`metrics_lookup_move_rate`. Start time is taken as the maximum across inputs of
`(dep_finish + transfer_time)` rather than the latest dep finish plus aggregated
transfers, matching the standard HEFT semantics.

Decisions are local and never revisited, so the scheduler is fast and scales to
large DAGs but cannot recover from poor early choices. Suitable as a
low-overhead default or as the construction step inside iterative schedulers;
the primitives `greedy_assign_task!`, `greedy_schedule!`, `cost_of_schedule`,
and `ScheduleState` are exposed for that reuse.
"""
struct GreedyScheduler <: DataDepsScheduler end

mutable struct ScheduleState
    task_finish_ns::Dict{Int, Float64}
    task_proc::Dict{Int, Processor}
    proc_ready_ns::Dict{Processor, Float64}
end

ScheduleState() = ScheduleState(Dict{Int, Float64}(), Dict{Int, Processor}(), Dict{Processor, Float64}())

function Base.copy(s::ScheduleState)
    return ScheduleState(copy(s.task_finish_ns), copy(s.task_proc), copy(s.proc_ready_ns))
end

function Base.empty!(s::ScheduleState)
    empty!(s.task_finish_ns)
    empty!(s.task_proc)
    empty!(s.proc_ready_ns)
    return s
end

Base.isempty(s::ScheduleState) = isempty(s.task_proc)
Base.length(s::ScheduleState) = length(s.task_proc)

function cost_of_schedule(state::ScheduleState)
    isempty(state.task_finish_ns) && return 0.0
    return maximum(values(state.task_finish_ns))
end

function greedy_assign_task!(state::ScheduleState, snap::MT.MetricsSnapshot,
                              dag_spec::DAGSpec, all_procs::Vector{Processor}, idx::Int)
    spec = dag_spec.id_to_spec[idx]
    task_scope = @something(spec.options.compute_scope, spec.options.scope, DefaultScope())

    compatible = filter(p -> proc_in_scope(p, task_scope), all_procs)
    if isempty(compatible)
        throw(Sch.SchedulingException("GreedyScheduler: no compatible processor for task $idx (scope: $task_scope)"))
    end

    sig = Sch.signature(spec.fargs[1], @view spec.fargs[2:end]).sig

    best_proc = first(compatible)
    best_finish = Inf
    for proc in compatible
        target_space = only(memory_spaces(proc))
        data_ready_ns = _greedy_earliest_data_ready_ns(snap, dag_spec, spec, target_space, state)
        proc_avail = get(state.proc_ready_ns, proc, 0.0)
        start_ns = max(data_ready_ns, proc_avail)

        worker_id = root_worker_id(proc)
        runtime_lookup = metrics_lookup_runtime_median(snap, sig, proc, worker_id)
        runtime_ns = runtime_lookup === nothing ? Float64(GREEDY_DEFAULT_RUNTIME_NS) : Float64(runtime_lookup)

        finish = start_ns + runtime_ns
        if finish < best_finish
            best_finish = finish
            best_proc = proc
        end
    end

    state.task_proc[idx] = best_proc
    state.task_finish_ns[idx] = best_finish
    state.proc_ready_ns[best_proc] = best_finish
    return best_proc
end

function greedy_schedule!(state::ScheduleState, snap::MT.MetricsSnapshot,
                          dag_spec::DAGSpec, all_procs::Vector{Processor};
                          task_order::Union{Nothing, AbstractVector{Int}}=nothing)
    order = task_order === nothing ? (1:nv(dag_spec.g)) : task_order
    for idx in order
        greedy_assign_task!(state, snap, dag_spec, all_procs, idx)
    end
    return state
end

function datadeps_schedule_dag_aot!(scheduler::GreedyScheduler, schedule, dag_spec, all_procs, all_scope)
    snap = MT.snapshot(MT.global_metrics_cache())
    state = ScheduleState()
    greedy_schedule!(state, snap, dag_spec, all_procs)
    for idx in 1:nv(dag_spec.g)
        task = dag_spec.id_to_task[idx]
        schedule[task] = state.task_proc[idx]
    end
    return
end

function _greedy_earliest_data_ready_ns(snap, dag_spec::DAGSpec, spec,
                                          target_space::MemorySpace, state::ScheduleState)
    earliest_ns = 0.0
    for arg in spec.fargs
        raw_val, _ = unwrap_inout(value(arg))
        ready_ns = _greedy_arg_ready_time_ns(raw_val, snap, dag_spec, target_space, state)
        if ready_ns > earliest_ns
            earliest_ns = ready_ns
        end
    end
    return earliest_ns
end

function _greedy_arg_ready_time_ns(val::Chunk, snap::MT.MetricsSnapshot, ::DAGSpec,
                                    target_space::MemorySpace, ::ScheduleState)
    source_space = memory_space(val)
    source_space == target_space && return 0.0
    size_bytes = val.handle.size === nothing ? GREEDY_DEFAULT_OUTPUT_SIZE : UInt64(val.handle.size)
    rate_lookup = metrics_lookup_move_rate(snap, source_space, target_space)
    rate = rate_lookup === nothing ? GREEDY_DEFAULT_TRANSFER_RATE : rate_lookup
    return Float64(size_bytes) / Float64(rate) * 1e9
end

function _greedy_arg_ready_time_ns(val::DTask, snap::MT.MetricsSnapshot, dag_spec::DAGSpec,
                                    target_space::MemorySpace, state::ScheduleState)
    dep_id = get(dag_spec.uid_to_id, val.uid, nothing)
    dep_id === nothing && return 0.0
    dep_proc = get(state.task_proc, dep_id, nothing)
    dep_proc === nothing && return 0.0
    dep_finish = get(state.task_finish_ns, dep_id, 0.0)
    source_space = only(memory_spaces(dep_proc))
    source_space == target_space && return dep_finish
    rate_lookup = metrics_lookup_move_rate(snap, source_space, target_space)
    rate = rate_lookup === nothing ? GREEDY_DEFAULT_TRANSFER_RATE : rate_lookup
    transfer_ns = Float64(GREEDY_DEFAULT_OUTPUT_SIZE) / Float64(rate) * 1e9
    return dep_finish + transfer_ns
end

function _greedy_arg_ready_time_ns(::Any, ::MT.MetricsSnapshot, ::DAGSpec,
                                    ::MemorySpace, ::ScheduleState)
    return 0.0
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
