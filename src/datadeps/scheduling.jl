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

"""
    _propagate_aot_time_util!(spec::DTaskSpec, proc::Processor, task_time_ns::Real)

Attach the AOT-computed per-task runtime to `spec.options.time_util` so
`Sch.has_capacity` bypasses its metrics-snapshot scan. Keyed by
`typeof(proc)`; value in seconds. No-op on non-finite / non-positive input.
"""
function _propagate_aot_time_util!(spec::DTaskSpec, proc::Processor, task_time_ns::Real)
    task_time_ns > 0 || return
    isfinite(task_time_ns) || return
    time_util_secs = Float64(task_time_ns) / 1e9
    T = typeof(proc)
    if spec.options.time_util === nothing
        spec.options.time_util = Dict{Type,Any}(T => time_util_secs)
    else
        spec.options.time_util[T] = time_util_secs
    end
    return
end

# EFTCostCache-aware wrapper is below, after the struct.

const GREEDY_DEFAULT_RUNTIME_NS = UInt64(1_000_000_000)
const GREEDY_DEFAULT_TRANSFER_RATE = UInt64(1_000_000)
const GREEDY_DEFAULT_OUTPUT_SIZE = UInt64(1_048_576)

# EFTCostCache caches the outputs of every `_eft_runtime_ns` and
# `metrics_lookup_move_rate` call the heuristics would otherwise repeat
# each iteration. Built once at the top of a scheduling pass; every
# subsequent lookup is a bounds-checked array read. Values are byte-identical
# to what the uncached path would compute, so cost-model claims and every
# non-worsening / determinism / correctness invariant are preserved.
struct EFTCostCache
    task_times::Matrix{Float64}
    proc_compatible::Matrix{Bool}
    proc_spaces::Vector{MemorySpace}
    proc_to_idx::Dict{Processor, Int}
    move_rates::Matrix{Float64}
end

"""
    _propagate_aot_time_util_from_cache!(dag_spec, cache, task_idx, proc)

Pull the AOT runtime for `(task_idx, proc)` from `cache` and forward to
`_propagate_aot_time_util!`. No-op if `proc` isn't cached.
"""
function _propagate_aot_time_util_from_cache!(dag_spec::DAGSpec, cache::EFTCostCache,
                                                task_idx::Int, proc::Processor)
    proc_idx = get(cache.proc_to_idx, proc, nothing)
    proc_idx === nothing && return
    task_time_ns = cache.task_times[task_idx, proc_idx]
    _propagate_aot_time_util!(dag_spec.id_to_spec[task_idx], proc, task_time_ns)
    return
end

function _build_eft_cost_cache(snap::MT.MetricsSnapshot, dag_spec::DAGSpec,
                                all_procs::Vector{Processor})
    n_tasks = nv(dag_spec.g)
    n_procs = length(all_procs)

    task_times = zeros(Float64, n_tasks, n_procs)
    proc_compatible = falses(n_tasks, n_procs)

    @inbounds for k in 1:n_tasks
        spec = dag_spec.id_to_spec[k]
        task_scope = @something(spec.options.compute_scope, spec.options.scope, DefaultScope())
        for (w, proc) in enumerate(all_procs)
            if proc_in_scope(proc, task_scope)
                proc_compatible[k, w] = true
                task_times[k, w] = _eft_runtime_ns(snap, spec, proc)
            else
                # Placeholder — never consulted because compatibility guard skips it.
                task_times[k, w] = Float64(GREEDY_DEFAULT_RUNTIME_NS)
            end
        end
    end

    proc_spaces = MemorySpace[only(memory_spaces(p)) for p in all_procs]
    proc_to_idx = Dict{Processor, Int}(p => i for (i, p) in enumerate(all_procs))

    move_rates = zeros(Float64, n_procs, n_procs)
    @inbounds for w1 in 1:n_procs, w2 in 1:n_procs
        w1 == w2 && continue
        if proc_spaces[w1] == proc_spaces[w2]
            move_rates[w1, w2] = 0.0
        else
            r = metrics_lookup_move_rate(snap, proc_spaces[w1], proc_spaces[w2])
            move_rates[w1, w2] = r === nothing ? Float64(GREEDY_DEFAULT_TRANSFER_RATE) : Float64(r)
        end
    end

    return EFTCostCache(task_times, proc_compatible, proc_spaces, proc_to_idx, move_rates)
end

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
    # Aggregate readiness: when the processor is *fully* drained, i.e.
    # `maximum(proc_slots[proc])`. Retained as the schedule-level summary and
    # as the makespan input; equals `proc_slots[proc][1]` when concurrency is 1.
    proc_ready_ns::Dict{Processor, Float64}
    # Per-execution-unit readiness: one completion timestamp per concurrent
    # slot (`Dagger.proc_concurrency(proc)` of them). A `ThreadProc` has one
    # slot and behaves exactly as the single-timestamp model did; a multi-stream
    # GPU proc has one slot per stream, so K queued tasks finish in
    # ceil(K/slots) rounds rather than being charged K serial task times.
    # Capacity-aware processor allocation per Sinnen & Sousa 2005 §4.3.
    proc_slots::Dict{Processor, Vector{Float64}}
end

ScheduleState() = ScheduleState(Dict{Int, Float64}(), Dict{Int, Processor}(),
                                Dict{Processor, Float64}(), Dict{Processor, Vector{Float64}}())

function Base.copy(s::ScheduleState)
    return ScheduleState(copy(s.task_finish_ns), copy(s.task_proc), copy(s.proc_ready_ns),
                         Dict{Processor, Vector{Float64}}(k => copy(v) for (k, v) in s.proc_slots))
end

function Base.empty!(s::ScheduleState)
    empty!(s.task_finish_ns)
    empty!(s.task_proc)
    empty!(s.proc_ready_ns)
    empty!(s.proc_slots)
    return s
end

"""
    _slots_for(state, proc) -> Vector{Float64}

Per-slot completion timestamps for `proc`, allocated on first use with one
entry per concurrent execution unit. Entries start at `0.0` (idle).
"""
function _slots_for(state::ScheduleState, proc::Processor)
    slots = get(state.proc_slots, proc, nothing)
    if slots === nothing
        slots = zeros(Float64, max(1, Dagger.proc_concurrency(proc)))
        state.proc_slots[proc] = slots
    end
    return slots
end

# Claim the earliest-free slot on `proc` for a task that cannot start before
# `data_ready_ns`, returning the finish time. Mutates both the slot vector and
# the aggregate `proc_ready_ns`. With one slot this is exactly
# `max(data_ready, proc_ready) + runtime`, i.e. the pre-existing behaviour.
function _claim_slot!(state::ScheduleState, proc::Processor, data_ready_ns::Float64,
                      runtime_ns::Float64)
    slots = _slots_for(state, proc)
    idx = argmin(slots)
    finish = max(data_ready_ns, slots[idx]) + runtime_ns
    slots[idx] = finish
    state.proc_ready_ns[proc] = maximum(slots)
    return finish
end

# Finish time a task would have on `proc` without committing to it.
function _peek_slot(state::ScheduleState, proc::Processor, data_ready_ns::Float64,
                    runtime_ns::Float64)
    slots = get(state.proc_slots, proc, nothing)
    earliest = slots === nothing ? 0.0 : slots[argmin(slots)]
    return max(data_ready_ns, earliest) + runtime_ns
end

# In-place copy: reuse `dst`'s dict buffers instead of allocating fresh ones.
# Used by SA/IG hot loops to avoid per-iteration Dict allocations for the
# candidate/best/current buffers. Equivalent to `dst = copy(src)` in state
# semantics but O(entries) writes with no outer allocations.
function _copy_state!(dst::ScheduleState, src::ScheduleState)
    empty!(dst.task_finish_ns)
    for (k, v) in src.task_finish_ns
        dst.task_finish_ns[k] = v
    end
    empty!(dst.task_proc)
    for (k, v) in src.task_proc
        dst.task_proc[k] = v
    end
    # Reuse slot vectors in place so the SA/IG hot loop stays allocation-free.
    for (k, v) in src.proc_slots
        dv = get(dst.proc_slots, k, nothing)
        if dv === nothing || length(dv) != length(v)
            dst.proc_slots[k] = copy(v)
        else
            copyto!(dv, v)
        end
    end
    if length(dst.proc_slots) != length(src.proc_slots)
        for k in collect(keys(dst.proc_slots))
            haskey(src.proc_slots, k) || delete!(dst.proc_slots, k)
        end
    end
    empty!(dst.proc_ready_ns)
    for (k, v) in src.proc_ready_ns
        dst.proc_ready_ns[k] = v
    end
    return dst
end

Base.isempty(s::ScheduleState) = isempty(s.task_proc)
Base.length(s::ScheduleState) = length(s.task_proc)

function cost_of_schedule(state::ScheduleState)
    isempty(state.task_finish_ns) && return 0.0
    return maximum(values(state.task_finish_ns))
end

# --- Cached (fast) variants of the EFT helpers ---
# Each cached function has an uncached wrapper below with the original
# signature. The wrapper builds an `EFTCostCache` on demand and delegates,
# so external callers (tests, other packages) see no API change.

function greedy_assign_task!(state::ScheduleState, snap::MT.MetricsSnapshot,
                              dag_spec::DAGSpec, all_procs::Vector{Processor}, idx::Int,
                              cache::EFTCostCache)
    spec = dag_spec.id_to_spec[idx]
    n_procs = length(all_procs)

    best_proc = nothing
    best_finish = Inf
    best_data_ready = 0.0
    best_runtime = 0.0
    @inbounds for w in 1:n_procs
        cache.proc_compatible[idx, w] || continue
        proc = all_procs[w]
        target_space = cache.proc_spaces[w]
        data_ready_ns = _greedy_earliest_data_ready_ns_cached(snap, dag_spec, spec, target_space, state, cache, w)
        runtime_ns = cache.task_times[idx, w]
        finish = _peek_slot(state, proc, data_ready_ns, runtime_ns)
        if finish < best_finish
            best_finish = finish
            best_proc = proc
            best_data_ready = data_ready_ns
            best_runtime = runtime_ns
        end
    end

    if best_proc === nothing
        task_scope = @something(spec.options.compute_scope, spec.options.scope, DefaultScope())
        throw(Sch.SchedulingException("GreedyScheduler: no compatible processor for task $idx (scope: $task_scope)"))
    end

    state.task_proc[idx] = best_proc
    # Commit to the slot that produced `best_finish`; `_claim_slot!` recomputes
    # the same value from the same earliest-free slot.
    state.task_finish_ns[idx] = _claim_slot!(state, best_proc, best_data_ready, best_runtime)
    return best_proc
end

"""
    greedy_assign_task_randomized!(state, snap, dag_spec, all_procs, idx, cache, rng; alpha)

Stochastic variant of [`greedy_assign_task!`](@ref) used only by IG's
reconstruction path. Instead of always taking the single lowest-EFT processor
(strict `<`, fully deterministic), it builds a restricted candidate list of
every compatible processor whose peeked finish time is within `(1+alpha)` of
the best, then picks one uniformly at random.

This is what gives Iterated Greedy a real search neighborhood. With the
deterministic `greedy_assign_task!`, `_replay_schedule!` provably reproduces
the greedy seed exactly on every iteration (preserved tasks stay at their
original procs, destroyed tasks are re-derived identically), so IG could never
improve on Greedy — its neighborhood was a single point. Randomized
reconstruction is the stochastic-construction step of Ruiz & Stützle (2007);
`alpha=0` recovers the deterministic behavior.

Uses the same K-slot peek/claim helpers as `greedy_assign_task!`, so capacity
accounting is identical; only the choice among near-best procs differs.
"""
function greedy_assign_task_randomized!(state::ScheduleState, snap::MT.MetricsSnapshot,
                                        dag_spec::DAGSpec, all_procs::Vector{Processor},
                                        idx::Int, cache::EFTCostCache,
                                        rng::Random.AbstractRNG;
                                        alpha::Float64=IG_DEFAULT_ALPHA)
    spec = dag_spec.id_to_spec[idx]
    n_procs = length(all_procs)

    # Single pass: peek every compatible proc, remember what `_claim_slot!`
    # will need (data_ready, runtime), and track the best finish. Peeking does
    # not mutate slot state, so all candidates are evaluated against the same
    # partial schedule — exactly as the deterministic variant does.
    cand = Tuple{Processor,Float64,Float64,Float64}[]
    best_finish = Inf
    @inbounds for w in 1:n_procs
        cache.proc_compatible[idx, w] || continue
        proc = all_procs[w]
        target_space = cache.proc_spaces[w]
        data_ready_ns = _greedy_earliest_data_ready_ns_cached(snap, dag_spec, spec, target_space, state, cache, w)
        runtime_ns = cache.task_times[idx, w]
        finish = _peek_slot(state, proc, data_ready_ns, runtime_ns)
        push!(cand, (proc, data_ready_ns, runtime_ns, finish))
        finish < best_finish && (best_finish = finish)
    end

    if isempty(cand)
        task_scope = @something(spec.options.compute_scope, spec.options.scope, DefaultScope())
        throw(Sch.SchedulingException("IteratedGreedyScheduler: no compatible processor for task $idx (scope: $task_scope)"))
    end

    # Uniform choice among candidates within the RCL threshold, via reservoir
    # sampling so no second allocation is needed. `best_finish` is always
    # in-threshold, so at least one candidate is always selectable.
    threshold = best_finish * (1.0 + alpha)
    chosen = cand[1]
    n_in = 0
    @inbounds for c in cand
        if c[4] <= threshold
            n_in += 1
            if rand(rng) * n_in < 1.0   # replace with probability 1/n_in
                chosen = c
            end
        end
    end

    proc, dr, rt, _ = chosen
    state.task_proc[idx] = proc
    state.task_finish_ns[idx] = _claim_slot!(state, proc, dr, rt)
    return proc
end

function greedy_assign_task!(state::ScheduleState, snap::MT.MetricsSnapshot,
                              dag_spec::DAGSpec, all_procs::Vector{Processor}, idx::Int)
    cache = _build_eft_cost_cache(snap, dag_spec, all_procs)
    return greedy_assign_task!(state, snap, dag_spec, all_procs, idx, cache)
end

function greedy_schedule!(state::ScheduleState, snap::MT.MetricsSnapshot,
                          dag_spec::DAGSpec, all_procs::Vector{Processor};
                          task_order::Union{Nothing, AbstractVector{Int}}=nothing,
                          cache::Union{EFTCostCache, Nothing}=nothing)
    if cache === nothing
        cache = _build_eft_cost_cache(snap, dag_spec, all_procs)
    end
    order = task_order === nothing ? (1:nv(dag_spec.g)) : task_order
    for idx in order
        greedy_assign_task!(state, snap, dag_spec, all_procs, idx, cache)
    end
    return state
end

function datadeps_schedule_dag_aot!(scheduler::GreedyScheduler, schedule, dag_spec, all_procs, all_scope)
    snap = MT.snapshot(MT.global_metrics_cache())
    cache = _build_eft_cost_cache(snap, dag_spec, all_procs)
    state = ScheduleState()
    greedy_schedule!(state, snap, dag_spec, all_procs; cache=cache)
    for idx in 1:nv(dag_spec.g)
        task = dag_spec.id_to_task[idx]
        proc = state.task_proc[idx]
        schedule[task] = proc
        # Propagate our AOT-computed per-task runtime to Sch's fast path.
        _propagate_aot_time_util_from_cache!(dag_spec, cache, idx, proc)
    end
    return
end

# Original (uncached) arg-ready helpers with unchanged 5-arg signatures.
# Kept because tests exercise them directly.

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


function _greedy_earliest_data_ready_ns_cached(snap, dag_spec::DAGSpec, spec,
                                                 target_space::MemorySpace, state::ScheduleState,
                                                 cache::EFTCostCache, target_w::Int)
    earliest_ns = 0.0
    for arg in spec.fargs
        raw_val, _ = unwrap_inout(value(arg))
        ready_ns = _greedy_arg_ready_time_ns_cached(raw_val, snap, dag_spec, target_space, state, cache, target_w)
        if ready_ns > earliest_ns
            earliest_ns = ready_ns
        end
    end
    return earliest_ns
end

function _greedy_arg_ready_time_ns_cached(val::DTask, snap::MT.MetricsSnapshot, dag_spec::DAGSpec,
                                           target_space::MemorySpace, state::ScheduleState,
                                           cache::EFTCostCache, target_w::Int)
    dep_id = get(dag_spec.uid_to_id, val.uid, nothing)
    dep_id === nothing && return 0.0
    dep_proc = get(state.task_proc, dep_id, nothing)
    dep_proc === nothing && return 0.0
    dep_finish = get(state.task_finish_ns, dep_id, 0.0)
    dep_w = get(cache.proc_to_idx, dep_proc, 0)
    if dep_w == 0
        # dep_proc unexpectedly outside all_procs — fall back to uncached path.
        return _greedy_arg_ready_time_ns(val, snap, dag_spec, target_space, state)
    end
    cache.proc_spaces[dep_w] == target_space && return dep_finish
    rate = cache.move_rates[dep_w, target_w]
    rate == 0.0 && return dep_finish
    transfer_ns = Float64(GREEDY_DEFAULT_OUTPUT_SIZE) / rate * 1e9
    return dep_finish + transfer_ns
end


_greedy_arg_ready_time_ns_cached(val::Chunk, snap::MT.MetricsSnapshot, dag_spec::DAGSpec,
                                   target_space::MemorySpace, state::ScheduleState,
                                   ::EFTCostCache, ::Int) =
    _greedy_arg_ready_time_ns(val, snap, dag_spec, target_space, state)

_greedy_arg_ready_time_ns_cached(::Any, ::MT.MetricsSnapshot, ::DAGSpec,
                                   ::MemorySpace, ::ScheduleState,
                                   ::EFTCostCache, ::Int) = 0.0

### Iterated Greedy ###

const IG_DEFAULT_N_ITERS = 32
const IG_DEFAULT_DESTROY_FRAC = 0.30
# Ruiz & Stützle (2005/2007) §3.4 tune the destruction size to a *fixed count*
# d, with d=4 the best-performing level (not statistically different from 3 or
# 5; very low and very high d are significantly worse). Our DAGs are small
# (K=10..64), so we keep `destroy_frac` as an escape hatch but cap the actual
# count at `IG_DEFAULT_DESTROY_COUNT`, which yields d=4 for K>=14 and d=3 at
# K=10 -- both within the paper's tuned-optimal band.
const IG_DEFAULT_DESTROY_COUNT = 4
# Ruiz & Stützle §3.2 use an SA-like acceptance criterion with a *constant*
# temperature (eq. 1, after Osman & Potts 1989):
#   Temperature = T * (sum_ij p_ij) / (n * m * 10) = T * mean(p_ij) / 10
# with T the only acceptance parameter. Their §3.4 DOE finds T=0.5 best (0.0 =
# strict/greedy acceptance stagnates). We compute the mean over compatible
# (task, proc) runtimes; see `_ig_acceptance_temperature`.
const IG_DEFAULT_TEMPERATURE = 0.5
# Restricted-candidate-list width for IG's stochastic reconstruction: a
# destroyed task is placed uniformly at random among processors whose EFT is
# within `(1+IG_DEFAULT_ALPHA)` of the best. `alpha=0` collapses to
# deterministic greedy. NOTE: Ruiz & Stützle's construction is deterministic
# NEH best-insertion; the sequence-position neighborhood of the flowshop makes
# that construction non-trivial. Task->processor assignment has no such
# neighborhood (a re-derived task returns to the same proc under pinned
# predecessors), so we randomize the construction GRASP-style to give IG a real
# search space. This is a deliberate adaptation, not the paper's mechanism.
const IG_DEFAULT_ALPHA = 0.10
# `Inf` disables the wall-clock stop.
const IG_DEFAULT_TIME_LIMIT_SEC = Inf

# Ruiz & Stützle §3.2 eq. (1): constant acceptance temperature proportional to
# the mean per-task work. Averaged over compatible (task, proc) pairs only, so
# the `GREEDY_DEFAULT_RUNTIME_NS` placeholders written for incompatible pairs
# in the cost cache do not inflate the scale.
function _ig_acceptance_temperature(cache::EFTCostCache, temperature::Float64)
    total = 0.0
    count = 0
    n_tasks, n_procs = size(cache.task_times)
    @inbounds for k in 1:n_tasks, w in 1:n_procs
        cache.proc_compatible[k, w] || continue
        total += cache.task_times[k, w]
        count += 1
    end
    count == 0 && return 0.0
    return temperature * (total / count) / 10.0
end

"""
    IteratedGreedyScheduler{S<:DataDepsScheduler} <: DataDepsScheduler

Iterated Greedy (Ruiz & Stützle 2007) on top of an inner DAG-AOT scheduler.

Starts from the inner scheduler's full schedule, then repeats: destroy a
random fraction of task assignments, reinsert them in topological order via
`greedy_assign_task!`, and keep the new schedule iff its `cost_of_schedule`
improves on the best-so-far. The inner scheduler is consulted only to build
the seed schedule; per-iteration reinsertion always uses the greedy primitive
so the iteration cost is `O(K · W)`.

The acceptance rule is strict-improvement (hill-climbing); probabilistic
acceptance of worsening neighbors is left to `SimulatedAnnealingScheduler`.

Fields:
- `inner::S`            — initial-solution provider (default `GreedyScheduler`).
- `n_iters::Int`        — destroy/reinsert cycles per call.
- `destroy_frac::Float64` — fraction of tasks destroyed per cycle, in (0, 1].
- `rng::Random.AbstractRNG` — RNG used for destroy-set sampling.
- `time_limit_sec::Float64` — wall-clock stop, checked between iterations.
                              `Inf` (default) disables the stop; a finite
                              value returns the best-so-far the moment the
                              budget is exceeded, so the scheduler always
                              terminates within one iteration of the limit.

The schedule cache is delegated to `inner` so entries are partitioned by
inner-scheduler type, matching the `TimedScheduler` wrapper convention used
elsewhere in this file.
"""
struct IteratedGreedyScheduler{S<:DataDepsScheduler, R<:Random.AbstractRNG} <: DataDepsScheduler
    inner::S
    n_iters::Int
    destroy_frac::Float64
    rng::R
    time_limit_sec::Float64

    function IteratedGreedyScheduler(inner::S;
                                     n_iters::Integer=IG_DEFAULT_N_ITERS,
                                     destroy_frac::Real=IG_DEFAULT_DESTROY_FRAC,
                                     rng::R=Random.default_rng(),
                                     time_limit_sec::Real=IG_DEFAULT_TIME_LIMIT_SEC) where {S<:DataDepsScheduler, R<:Random.AbstractRNG}
        n_iters >= 0 || throw(ArgumentError("IteratedGreedyScheduler: n_iters must be ≥ 0, got $n_iters"))
        (destroy_frac > 0 && destroy_frac <= 1) ||
            throw(ArgumentError("IteratedGreedyScheduler: destroy_frac must be in (0, 1], got $destroy_frac"))
        time_limit_sec > 0 || throw(ArgumentError("IteratedGreedyScheduler: time_limit_sec must be > 0, got $time_limit_sec"))
        return new{S, R}(inner, Int(n_iters), Float64(destroy_frac), rng, Float64(time_limit_sec))
    end
end

IteratedGreedyScheduler(; kwargs...) = IteratedGreedyScheduler(GreedyScheduler(); kwargs...)

# Hierarchical partitions need independent RNGs; deep-copy so shards don't race.
Base.similar(s::IteratedGreedyScheduler) =
    IteratedGreedyScheduler(similar(s.inner);
                            n_iters=s.n_iters,
                            destroy_frac=s.destroy_frac,
                            rng=copy(s.rng),
                            time_limit_sec=s.time_limit_sec)

# Delegate cache/equivalence to the inner scheduler so its type owns cache keys.
datadeps_schedule_cache(sched::IteratedGreedyScheduler) =
    datadeps_schedule_cache(sched.inner)
datadeps_dag_equivalent(sched::IteratedGreedyScheduler, dspec1::DAGSpec, dspec2::DAGSpec) =
    datadeps_dag_equivalent(sched.inner, dspec1, dspec2)
datadeps_argspec_equivalent(sched::IteratedGreedyScheduler, a1::DatadepsArgSpec, a2::DatadepsArgSpec) =
    datadeps_argspec_equivalent(sched.inner, a1, a2)
datadeps_ainfo_equivalent(sched::IteratedGreedyScheduler, a1::AbstractAliasing, a2::AbstractAliasing) =
    datadeps_ainfo_equivalent(sched.inner, a1, a2)

"""
    iterated_greedy_step!(state, snap, dag_spec, all_procs, destroyed)

Reconstruct `state` by replaying every task index not in `destroyed` at its
existing `task_proc` assignment, then assigning each destroyed task via
`greedy_assign_task!`. Both passes walk task IDs in increasing order, which
is a valid topological order since `dag_add_task!` appends parents before
children.

Mutates and returns `state`. The caller is responsible for providing a
freshly-`copy(prev_state)` if the previous state must be preserved on
rejection.
"""
# `spec.fargs` is a `Tuple` for typed tasks; `@view fargs[2:end]` doesn't work
# on tuples.
_eft_tail_args(fargs::Tuple) = Base.tail(fargs)
_eft_tail_args(fargs::AbstractVector) = @view fargs[2:end]

# Diagnostics for the signature lookup: set `DAGGER_TRACE_EFT_LOOKUP=1` to
# count hits/misses and dump the first few missed signatures. Gated behind a
# `Ref{Bool}` load because `_eft_runtime_ns` runs O(tasks x procs) times.
const TRACE_EFT_LOOKUP = Ref{Union{Nothing,Bool}}(nothing)
const EFT_LOOKUP_HITS = Threads.Atomic{Int}(0)
const EFT_LOOKUP_MISSES = Threads.Atomic{Int}(0)
const EFT_LOOKUP_TRANSLATED = Threads.Atomic{Int}(0)
function trace_eft_lookup()
    tr = TRACE_EFT_LOOKUP[]
    tr === nothing || return tr
    tr = get(ENV, "DAGGER_TRACE_EFT_LOOKUP", "0") != "0"
    TRACE_EFT_LOOKUP[] = tr
    return tr
end

# `spec.fargs` carry datadeps annotations (`In`/`Out`/`InOut`/`Deps`), which are
# stripped before the task executes. `Sch.signature` resolves an annotated arg
# through `chunktype(::InOut{T}) where T = T` -- a *type-level* unwrap that
# yields the `Chunk{...}` type parameter and cannot recurse further. The
# recorded signature, built at execution time, sees a bare `Chunk` instance and
# so resolves through `chunktype(c::Chunk) = c.chunktype` to the materialized
# type (e.g. `Matrix{Float64}`). Unwrapping the annotation here hands
# `Sch.signature` the same bare `Chunk` the write side saw, so both sides
# construct an identical signature and `LookupExact(SignatureMetric(), sig)` can
# match. Without this every lookup misses all four tiers of
# `_runtime_lookup_chain` and every task falls back to
# `GREEDY_DEFAULT_RUNTIME_NS`, flattening the cost model.
_eft_unwrap_arg(arg) = with_value(arg, first(unwrap_inout(value(arg))))

function _eft_runtime_ns(snap::MT.MetricsSnapshot, spec, proc::Processor)
    f = _eft_unwrap_arg(spec.fargs[1])
    tail = map(_eft_unwrap_arg, _eft_tail_args(spec.fargs))
    sig = Sch.signature(f, tail).sig
    worker_id = root_worker_id(proc)
    # Try the accelerator-side signature *first* on procs that have a
    # translation. The untranslated lookup cannot be used as the gate: the last
    # tier of `_runtime_lookup_chain` drops the processor constraint entirely,
    # so on a GPU it matches the CPU-recorded entries and returns a CPU runtime
    # instead of missing. Translating only after a miss would therefore be dead
    # code. Falling back to the untranslated lookup keeps a CPU-derived estimate
    # (still better than the 1s default) when a signature has no mapping.
    translated_sig = _translate_sig_for(sig, proc)
    runtime_lookup = nothing
    via_translation = false
    if translated_sig !== nothing
        runtime_lookup = metrics_lookup_runtime_median(snap, translated_sig, proc, worker_id)
        via_translation = runtime_lookup !== nothing
    end
    if runtime_lookup === nothing
        runtime_lookup = metrics_lookup_runtime_median(snap, sig, proc, worker_id)
    end
    if trace_eft_lookup()
        if runtime_lookup === nothing
            n = Threads.atomic_add!(EFT_LOOKUP_MISSES, 1)
            n < 5 && @warn "EFT lookup MISS" sig proc
        else
            Threads.atomic_add!(EFT_LOOKUP_HITS, 1)
            via_translation && Threads.atomic_add!(EFT_LOOKUP_TRANSLATED, 1)
        end
    end
    return runtime_lookup === nothing ? Float64(GREEDY_DEFAULT_RUNTIME_NS) : Float64(runtime_lookup)
end

# Replay in task-index order; destroyed tasks reassign via
# `greedy_assign_task!`, preserved tasks recompute finish times fresh (SA needs
# true makespan to evaluate ΔC).
function _replay_schedule!(state::ScheduleState, snap::MT.MetricsSnapshot,
                            dag_spec::DAGSpec, all_procs::Vector{Processor},
                            destroyed::AbstractSet{Int},
                            cache::EFTCostCache;
                            rng::Union{Nothing,Random.AbstractRNG}=nothing,
                            alpha::Float64=IG_DEFAULT_ALPHA)
    # Only the finish-time and proc-ready-time dicts need to be cleared for a
    # fresh replay; `task_proc` is either overwritten by `greedy_assign_task!`
    # (destroyed tasks) or read as the previous assignment (preserved tasks).
    # Since the loop walks task ids in increasing order and each iteration
    # only reads/writes `task_proc[idx]` for its own idx, there is no aliasing
    # hazard from skipping the copy.
    #
    # `rng === nothing` (the default) reconstructs destroyed tasks with the
    # deterministic `greedy_assign_task!` — used by every non-IG caller,
    # including SA (which always passes an empty `destroyed` set). When an rng
    # is supplied (IG only), destroyed tasks use the randomized variant so IG
    # actually searches. Greedy is untouched: `greedy_schedule!` never routes
    # through here.
    empty!(state.task_finish_ns)
    empty!(state.proc_ready_ns)
    empty!(state.proc_slots)

    @inbounds for idx in 1:nv(dag_spec.g)
        if idx in destroyed
            if rng === nothing
                greedy_assign_task!(state, snap, dag_spec, all_procs, idx, cache)
            else
                greedy_assign_task_randomized!(state, snap, dag_spec, all_procs, idx, cache, rng; alpha=alpha)
            end
        else
            proc = state.task_proc[idx]
            w = get(cache.proc_to_idx, proc, 0)
            spec = dag_spec.id_to_spec[idx]
            if w == 0
                target_space = only(memory_spaces(proc))
                data_ready_ns = _greedy_earliest_data_ready_ns(snap, dag_spec, spec, target_space, state)
                runtime_ns = _eft_runtime_ns(snap, spec, proc)
            else
                target_space = cache.proc_spaces[w]
                data_ready_ns = _greedy_earliest_data_ready_ns_cached(snap, dag_spec, spec, target_space, state, cache, w)
                runtime_ns = cache.task_times[idx, w]
            end
            state.task_finish_ns[idx] = _claim_slot!(state, proc, data_ready_ns, runtime_ns)
        end
    end
    return state
end

# Backward-compat wrapper: builds a cache on demand for callers (tests etc.)
# that use the historical 5-arg signature.
function _replay_schedule!(state::ScheduleState, snap::MT.MetricsSnapshot,
                            dag_spec::DAGSpec, all_procs::Vector{Processor},
                            destroyed::AbstractSet{Int})
    cache = _build_eft_cost_cache(snap, dag_spec, all_procs)
    return _replay_schedule!(state, snap, dag_spec, all_procs, destroyed, cache)
end

function iterated_greedy_step!(state::ScheduleState, snap::MT.MetricsSnapshot,
                                dag_spec::DAGSpec, all_procs::Vector{Processor},
                                destroyed::AbstractSet{Int},
                                cache::EFTCostCache;
                                rng::Union{Nothing,Random.AbstractRNG}=nothing,
                                alpha::Float64=IG_DEFAULT_ALPHA)
    return _replay_schedule!(state, snap, dag_spec, all_procs, destroyed, cache; rng=rng, alpha=alpha)
end

function iterated_greedy_step!(state::ScheduleState, snap::MT.MetricsSnapshot,
                                dag_spec::DAGSpec, all_procs::Vector{Processor},
                                destroyed::AbstractSet{Int})
    return _replay_schedule!(state, snap, dag_spec, all_procs, destroyed)
end

"""
    iterated_greedy_schedule!(state, snap, dag_spec, all_procs;
                              n_iters, destroy_frac, rng) -> ScheduleState

Run IG on top of an already-initialized `state` (typically produced by the
inner scheduler's pass). Returns the best `ScheduleState` found across
`n_iters` destroy/reinsert cycles, judged by `cost_of_schedule`.

Exposed as a primitive (alongside `greedy_schedule!`) so future schedulers
(SA, hybrid IG+SA pipelines) can drive it directly without instantiating an
`IteratedGreedyScheduler`.
"""
function iterated_greedy_schedule!(state::ScheduleState, snap::MT.MetricsSnapshot,
                                    dag_spec::DAGSpec, all_procs::Vector{Processor};
                                    n_iters::Integer=IG_DEFAULT_N_ITERS,
                                    destroy_frac::Real=IG_DEFAULT_DESTROY_FRAC,
                                    rng::Random.AbstractRNG=Random.default_rng(),
                                    cache::Union{EFTCostCache, Nothing}=nothing,
                                    time_limit_sec::Real=IG_DEFAULT_TIME_LIMIT_SEC)
    n_iters >= 0 || throw(ArgumentError("iterated_greedy_schedule!: n_iters must be ≥ 0"))
    (destroy_frac > 0 && destroy_frac <= 1) ||
        throw(ArgumentError("iterated_greedy_schedule!: destroy_frac must be in (0, 1]"))
    time_limit_sec > 0 || throw(ArgumentError("iterated_greedy_schedule!: time_limit_sec must be > 0"))
    n_tasks = nv(dag_spec.g)
    (n_tasks == 0 || n_iters == 0) && return state
    if cache === nothing
        cache = _build_eft_cost_cache(snap, dag_spec, all_procs)
    end

    # Two tracked solutions, per Ruiz & Stützle §3.2 (and standard SA/ILS):
    #   `best_*`      — best found so far; only ever improves; returned.
    #   `incumbent_*` — the current solution the search walks from; the SA-like
    #                   acceptance criterion may move it *uphill* to escape
    #                   local optima. Destruction always operates on the
    #                   incumbent, not on best. Conflating the two (accepting a
    #                   worse solution *into* best) would forfeit the guarantee
    #                   that IG never returns worse than its greedy seed.
    best_state = copy(state)
    best_cost = cost_of_schedule(best_state)
    incumbent = copy(state)
    incumbent_cost = best_cost

    # Reset in place via `_copy_state!` to avoid Dict allocations in the loop.
    candidate = copy(state)

    # Ruiz & Stützle §3.4: fixed destruction count d=4, capped at K. See
    # `IG_DEFAULT_DESTROY_COUNT`. `destroy_frac` is retained as an escape hatch.
    n_destroy = max(1, ceil(Int, destroy_frac * n_tasks))
    n_destroy = min(n_destroy, IG_DEFAULT_DESTROY_COUNT, n_tasks)

    # Constant acceptance temperature, eq. (1). Zero temperature recovers strict
    # (greedy) acceptance.
    temperature = _ig_acceptance_temperature(cache, IG_DEFAULT_TEMPERATURE)

    # Shared buffers reused across iters to avoid allocations.
    perm_buf = collect(1:n_tasks)
    destroyed = Set{Int}()
    sizehint!(destroyed, n_destroy)

    # `Inf` disables the guard via typemax(UInt64); unsigned elapsed subtraction.
    start_ns = time_ns()
    budget_ns = isinf(time_limit_sec) ? typemax(UInt64) :
                round(UInt64, time_limit_sec * 1e9)

    for _ in 1:n_iters
        (time_ns() - start_ns) >= budget_ns && break
        # Partial Fisher-Yates on perm_buf: O(n_destroy), no fresh allocations.
        @inbounds for i in 1:n_destroy
            j = rand(rng, i:n_tasks)
            perm_buf[i], perm_buf[j] = perm_buf[j], perm_buf[i]
        end
        empty!(destroyed)
        @inbounds for i in 1:n_destroy
            push!(destroyed, perm_buf[i])
        end

        # Destruct + (randomized) reconstruct from the *incumbent*.
        _copy_state!(candidate, incumbent)
        iterated_greedy_step!(candidate, snap, dag_spec, all_procs, destroyed, cache; rng=rng)
        new_cost = cost_of_schedule(candidate)

        # Track best-so-far (monotone).
        if new_cost < best_cost
            _copy_state!(best_state, candidate)
            best_cost = new_cost
        end

        # SA-like acceptance into the incumbent, eq. (1). Better always accepted;
        # worse accepted with probability exp(-ΔC / temperature).
        ΔC = new_cost - incumbent_cost
        if ΔC < 0 || (temperature > 0 && rand(rng) < exp(-ΔC / temperature))
            _copy_state!(incumbent, candidate)
            incumbent_cost = new_cost
        end
    end
    return best_state
end

function datadeps_schedule_dag_aot!(scheduler::IteratedGreedyScheduler,
                                    schedule, dag_spec, all_procs, all_scope)
    n_tasks = nv(dag_spec.g)
    n_tasks == 0 && return

    snap = MT.snapshot(MT.global_metrics_cache())
    # Build the cost cache once and share it between the greedy seed and IG.
    cache = _build_eft_cost_cache(snap, dag_spec, all_procs)

    state = ScheduleState()
    greedy_schedule!(state, snap, dag_spec, all_procs; cache=cache)

    state = iterated_greedy_schedule!(state, snap, dag_spec, all_procs;
                                      time_limit_sec=scheduler.time_limit_sec,
                                       n_iters=scheduler.n_iters,
                                       destroy_frac=scheduler.destroy_frac,
                                       rng=scheduler.rng,
                                       cache=cache)

    @inbounds for idx in 1:n_tasks
        task = dag_spec.id_to_task[idx]
        proc = state.task_proc[idx]
        schedule[task] = proc
        _propagate_aot_time_util_from_cache!(dag_spec, cache, idx, proc)
    end
    return
end

### Simulated Annealing ###

# Defaults follow Orsila, Salminen, Hämäläinen 2008 §4.3 and §5.3.
const SA_DEFAULT_Q = 0.95
const SA_DEFAULT_K = 1.0
const SA_DEFAULT_N_RESTARTS = 1
const SA_TF_FLOOR = 1e-12
const SA_DEFAULT_TIME_LIMIT_SEC = Inf

"""
    SimulatedAnnealingScheduler{S<:DataDepsScheduler, R<:Random.AbstractRNG} <: DataDepsScheduler

Simulated annealing (Kirkpatrick et al. 1983) over the same EFT cost model
used by `GreedyScheduler` and `IteratedGreedyScheduler`. The algorithm
follows the best-practice parameterization of Orsila et al. (2008):
geometric cooling, normalized inverse exponential acceptance, coupled
temperature-and-rejection termination, and the closed-form temperature
range of their §4.3 derived from the per-(task, processor) runtime
distribution.

Fields:
- `inner::S`            — initial-solution provider. Default
                          `IteratedGreedyScheduler()` so SA refines an
                          already-improved seed. Only
                          `GreedyScheduler` and `IteratedGreedyScheduler`
                          inners are honored as seed sources; any other
                          subtype is treated as a Greedy seed.
- `q::Float64`          — geometric cooling factor in (0, 1).
- `k::Float64`          — coefficient `k > 0` in Orsila §4.3 Eqs. 18, 19.
- `n_restarts::Int`     — independent SA runs, each seeded from the
                          best-known solution so far (Orsila §5.3 rule 7).
- `time_limit_sec::Float64` — wall-clock stop, checked between restarts and
                          per cooling level. `Inf` disables. When an IG
                          seed is used, seed and SA each observe their own
                          budget; the pipeline sums them worst-case.
- `rng::R`              — RNG used for moves and acceptance sampling.
                          The `rng` is mutated during scheduling, so a
                          single `SimulatedAnnealingScheduler` instance
                          must not be shared across concurrent
                          `spawn_datadeps` blocks.

The schedule cache and DAG-equivalence hooks are delegated to `inner`, so
cached schedules remain partitioned per inner-scheduler type.
"""
struct SimulatedAnnealingScheduler{S<:DataDepsScheduler, R<:Random.AbstractRNG} <: DataDepsScheduler
    inner::S
    q::Float64
    k::Float64
    n_restarts::Int
    rng::R
    time_limit_sec::Float64

    function SimulatedAnnealingScheduler(inner::S;
                                          q::Real=SA_DEFAULT_Q,
                                          k::Real=SA_DEFAULT_K,
                                          n_restarts::Integer=SA_DEFAULT_N_RESTARTS,
                                          rng::R=Random.default_rng(),
                                          time_limit_sec::Real=SA_DEFAULT_TIME_LIMIT_SEC) where {S<:DataDepsScheduler, R<:Random.AbstractRNG}
        (0 < q < 1) || throw(ArgumentError("SimulatedAnnealingScheduler: q must be in (0, 1), got $q"))
        k > 0 || throw(ArgumentError("SimulatedAnnealingScheduler: k must be > 0, got $k"))
        n_restarts >= 1 || throw(ArgumentError("SimulatedAnnealingScheduler: n_restarts must be ≥ 1, got $n_restarts"))
        time_limit_sec > 0 || throw(ArgumentError("SimulatedAnnealingScheduler: time_limit_sec must be > 0, got $time_limit_sec"))
        return new{S, R}(inner, Float64(q), Float64(k), Int(n_restarts), rng, Float64(time_limit_sec))
    end
end

SimulatedAnnealingScheduler(; kwargs...) =
    SimulatedAnnealingScheduler(IteratedGreedyScheduler(); kwargs...)

Base.similar(s::SimulatedAnnealingScheduler) =
    SimulatedAnnealingScheduler(similar(s.inner);
                                 q=s.q, k=s.k, n_restarts=s.n_restarts,
                                 rng=copy(s.rng),
                                 time_limit_sec=s.time_limit_sec)

datadeps_schedule_cache(sched::SimulatedAnnealingScheduler) =
    datadeps_schedule_cache(sched.inner)
datadeps_dag_equivalent(sched::SimulatedAnnealingScheduler, dspec1::DAGSpec, dspec2::DAGSpec) =
    datadeps_dag_equivalent(sched.inner, dspec1, dspec2)
datadeps_argspec_equivalent(sched::SimulatedAnnealingScheduler, a1::DatadepsArgSpec, a2::DatadepsArgSpec) =
    datadeps_argspec_equivalent(sched.inner, a1, a2)
datadeps_ainfo_equivalent(sched::SimulatedAnnealingScheduler, a1::AbstractAliasing, a2::AbstractAliasing) =
    datadeps_ainfo_equivalent(sched.inner, a1, a2)

# Cost-matrix aggregates for the Orsila §4.3 closed-form T0/Tf.
struct SAEnergyParams
    t_min::Float64
    t_max::Float64
    t_min_sum::Float64
    t_max_sum::Float64
end

function _sa_compatible_procs(spec, all_procs::Vector{Processor})
    task_scope = @something(spec.options.compute_scope, spec.options.scope, DefaultScope())
    return filter(p -> proc_in_scope(p, task_scope), all_procs)
end

function _sa_compute_energy_params(snap::MT.MetricsSnapshot, dag_spec::DAGSpec,
                                    all_procs::Vector{Processor},
                                    cache::EFTCostCache)
    n_tasks = nv(dag_spec.g)
    n_tasks == 0 && return SAEnergyParams(0.0, 0.0, 0.0, 0.0)
    n_procs = length(all_procs)

    t_min = Inf
    t_max = 0.0
    t_min_sum = 0.0
    t_max_sum = 0.0

    @inbounds for idx in 1:n_tasks
        task_min = Inf
        task_max = 0.0
        any_compat = false
        for w in 1:n_procs
            cache.proc_compatible[idx, w] || continue
            any_compat = true
            r = cache.task_times[idx, w]
            r < task_min && (task_min = r)
            r > task_max && (task_max = r)
        end
        if !any_compat
            throw(Sch.SchedulingException("SimulatedAnnealingScheduler: no compatible processor for task $idx"))
        end

        t_min = min(t_min, task_min)
        t_max = max(t_max, task_max)
        t_min_sum += task_min
        t_max_sum += task_max
    end

    return SAEnergyParams(t_min, t_max, t_min_sum, t_max_sum)
end

function _sa_compute_energy_params(snap::MT.MetricsSnapshot, dag_spec::DAGSpec,
                                    all_procs::Vector{Processor})
    cache = _build_eft_cost_cache(snap, dag_spec, all_procs)
    return _sa_compute_energy_params(snap, dag_spec, all_procs, cache)
end

# Normalized inverse exponential acceptance (Orsila Eq. 6). ΔC = 0 yields
# P = 0.5 by design (Orsila §3.3.1), letting SA drift between equally-good
# solutions.
function _sa_accept(ΔC::Float64, T::Float64, C0::Float64, rng::Random.AbstractRNG)
    ΔC < 0 && return true
    denom = C0 * T
    denom <= 0 && return false
    arg = ΔC / denom
    arg > 700.0 && return false   # exp(arg) overflows; P ≈ 0
    return rand(rng) < 1.0 / (1.0 + exp(arg))
end

# Single-task move per Orsila §3.6.1. The current proc is excluded from
# the alternative set (Orsila §3.6) so the move is never a no-op when the
# task has at least one other compatible proc.
function _sa_propose_neighbor!(candidate::ScheduleState, snap::MT.MetricsSnapshot,
                                dag_spec::DAGSpec, all_procs::Vector{Processor},
                                rng::Random.AbstractRNG,
                                cache::EFTCostCache)
    n_tasks = nv(dag_spec.g)
    n_tasks == 0 && return candidate

    task_idx = rand(rng, 1:n_tasks)
    current_proc = candidate.task_proc[task_idx]
    n_procs = length(all_procs)

    # Uniformly pick a scope-compatible proc different from the current one,
    # using the cache's compatibility matrix. First count alternatives to
    # size the sample; then step through and take the pick-th match.
    n_alt = 0
    @inbounds for w in 1:n_procs
        cache.proc_compatible[task_idx, w] || continue
        all_procs[w] === current_proc && continue
        n_alt += 1
    end
    n_alt == 0 && return candidate   # forced assignment; no-op move

    pick = rand(rng, 1:n_alt)
    new_proc = current_proc
    seen = 0
    @inbounds for w in 1:n_procs
        cache.proc_compatible[task_idx, w] || continue
        all_procs[w] === current_proc && continue
        seen += 1
        if seen == pick
            new_proc = all_procs[w]
            break
        end
    end
    candidate.task_proc[task_idx] = new_proc

    _replay_schedule!(candidate, snap, dag_spec, all_procs, Set{Int}(), cache)
    return candidate
end

"""
    simulated_annealing_schedule!(state, snap, dag_spec, all_procs;
                                  q, k, n_restarts, rng) -> ScheduleState

SA refinement on top of an already-initialized `state`. Returns the best
`ScheduleState` found across `n_restarts` independent runs, each judged by
`cost_of_schedule` and accepted via Orsila's normalized inverse exponential
rule. The initial and final temperatures are derived in closed form from
the snapshot's EFT cost matrix per Orsila §4.3:

    T0 = k * t_max / t_min_sum         (Eq. 18)
    Tf = t_min / (k * t_max_sum)       (Eq. 19)

Inner-loop length is `L = K · max(W − 1, 1)` (§5.3 rule 1), termination is
the coupled `Temp(i) ≤ Tf ∧ R ≥ Rmax` with `Rmax = L` (§3.9.6, §5.3
rule 4), and the best-known solution is preserved across restarts so SA
never returns a state worse than `state`.

Exposed as a primitive (alongside `greedy_schedule!` and
`iterated_greedy_schedule!`) for reuse by hybrid pipelines.
"""
function simulated_annealing_schedule!(state::ScheduleState, snap::MT.MetricsSnapshot,
                                        dag_spec::DAGSpec, all_procs::Vector{Processor};
                                        q::Real=SA_DEFAULT_Q, k::Real=SA_DEFAULT_K,
                                        n_restarts::Integer=SA_DEFAULT_N_RESTARTS,
                                        rng::Random.AbstractRNG=Random.default_rng(),
                                        cache::Union{EFTCostCache, Nothing}=nothing,
                                        time_limit_sec::Real=SA_DEFAULT_TIME_LIMIT_SEC)
    (0 < q < 1) || throw(ArgumentError("simulated_annealing_schedule!: q must be in (0, 1)"))
    k > 0 || throw(ArgumentError("simulated_annealing_schedule!: k must be > 0"))
    n_restarts >= 1 || throw(ArgumentError("simulated_annealing_schedule!: n_restarts must be ≥ 1"))
    time_limit_sec > 0 || throw(ArgumentError("simulated_annealing_schedule!: time_limit_sec must be > 0"))

    n_tasks = nv(dag_spec.g)
    n_tasks == 0 && return state

    if cache === nothing
        cache = _build_eft_cost_cache(snap, dag_spec, all_procs)
    end
    params = _sa_compute_energy_params(snap, dag_spec, all_procs, cache)

    # Guarding the closed-form against divide-by-zero / degenerate ranges.
    (params.t_min_sum <= 0 || params.t_max_sum <= 0) && return state

    T0 = Float64(k) * params.t_max / params.t_min_sum
    Tf = max(params.t_min / (Float64(k) * params.t_max_sum), SA_TF_FLOOR)
    T0 <= Tf && return state

    W = length(all_procs)
    L = max(1, n_tasks * max(W - 1, 1))   # Orsila §5.3 rule 1
    Rmax = L                              # Orsila §5.3 rule 4
    C0 = max(cost_of_schedule(state), 1.0)   # Orsila §3.5.4

    overall_best = copy(state)
    overall_best_cost = cost_of_schedule(overall_best)


    current = copy(overall_best)
    candidate = copy(overall_best)
    best_in_run = copy(overall_best)

    # Safety cap on iterations per restart (16× the expected count).
    expected_levels = max(1, ceil(Int, log(Tf / T0) / log(Float64(q))))
    max_iters_per_restart = 16 * L * expected_levels

    # Same clock/`Inf`-disable pattern as `iterated_greedy_schedule!`.
    start_ns = time_ns()
    budget_ns = isinf(time_limit_sec) ? typemax(UInt64) :
                round(UInt64, time_limit_sec * 1e9)

    for _ in 1:n_restarts
        (time_ns() - start_ns) >= budget_ns && break
        _copy_state!(current, overall_best)
        current_cost = overall_best_cost
        _copy_state!(best_in_run, current)
        best_in_run_cost = current_cost

        T = T0
        R = 0
        iters_at_level = 0
        total_iters = 0

        while true
            (T <= Tf && R >= Rmax) && break
            total_iters >= max_iters_per_restart && break
            (time_ns() - start_ns) >= budget_ns && break

            _copy_state!(candidate, current)
            _sa_propose_neighbor!(candidate, snap, dag_spec, all_procs, rng, cache)
            new_cost = cost_of_schedule(candidate)
            ΔC = new_cost - current_cost

            if _sa_accept(ΔC, T, C0, rng)
                # Swap references: `current` now points at candidate's buffer
                # (the accepted state), `candidate` holds the old current's
                # buffer (stale data, overwritten by _copy_state! next iter).
                current, candidate = candidate, current
                current_cost = new_cost
                if current_cost < best_in_run_cost
                    _copy_state!(best_in_run, current)
                    best_in_run_cost = current_cost
                end
                R = 0
            else
                R += 1
            end

            iters_at_level += 1
            total_iters += 1
            if iters_at_level >= L
                T *= Float64(q)
                iters_at_level = 0
            end
        end

        if best_in_run_cost < overall_best_cost
            _copy_state!(overall_best, best_in_run)
            overall_best_cost = best_in_run_cost
        end
    end

    _copy_state!(state, overall_best)
    return state
end

function datadeps_schedule_dag_aot!(scheduler::SimulatedAnnealingScheduler,
                                    schedule, dag_spec, all_procs, all_scope)
    n_tasks = nv(dag_spec.g)
    n_tasks == 0 && return

    snap = MT.snapshot(MT.global_metrics_cache())

    cache = _build_eft_cost_cache(snap, dag_spec, all_procs)

    seed_state = ScheduleState()
    greedy_schedule!(seed_state, snap, dag_spec, all_procs; cache=cache)

    if scheduler.inner isa IteratedGreedyScheduler
        ig = scheduler.inner
        seed_state = iterated_greedy_schedule!(seed_state, snap, dag_spec, all_procs;
                                                n_iters=ig.n_iters,
                                                destroy_frac=ig.destroy_frac,
                                                rng=ig.rng,
                                                cache=cache,
                                                time_limit_sec=ig.time_limit_sec)
    end

    final_state = simulated_annealing_schedule!(seed_state, snap, dag_spec, all_procs;
                                                 q=scheduler.q, k=scheduler.k,
                                                 n_restarts=scheduler.n_restarts,
                                                 rng=scheduler.rng,
                                                 cache=cache,
                                                 time_limit_sec=scheduler.time_limit_sec)

    @inbounds for idx in 1:n_tasks
        task = dag_spec.id_to_task[idx]
        proc = final_state.task_proc[idx]
        schedule[task] = proc
        _propagate_aot_time_util_from_cache!(dag_spec, cache, idx, proc)
    end
    return
end

### MILP ###

# Constant fallback matching the heuristics' per-edge transfer model.
# `dag_add_task!` defers within-DAG DTask args so per-edge chunk sizes are
# not recoverable from the `DAGSpec`; kept as a hook for future MILP
# variants that plug in an affinity-based size estimate.
_milp_edge_size_bytes(::DAGSpec, ::Int, ::Int) = GREEDY_DEFAULT_OUTPUT_SIZE

function _milp_transfer_time_ns(snap::MT.MetricsSnapshot,
                                 source_space::MemorySpace, dest_space::MemorySpace,
                                 size_bytes::UInt64)
    source_space == dest_space && return 0.0
    rate = metrics_lookup_move_rate(snap, source_space, dest_space)
    rate === nothing && (rate = GREEDY_DEFAULT_TRANSFER_RATE)
    return Float64(size_bytes) / Float64(rate) * 1e9
end

"""
    JuMPScheduler(optimizer; Z=10.0, time_limit_sec=60.0) <: DataDepsScheduler

Exact MILP scheduler that solves the basic formulation in §Mathematical
programming specification of the paper (also DagScheduler.jl README): bi-linear
penalty linearisation, sequential big-M execution constraint, makespan
objective. Provides provably optimal schedules for small instances and
ground-truth reference for measuring the optimality gap of `GreedyScheduler`,
`IteratedGreedyScheduler` and `SimulatedAnnealingScheduler`.

This scheduler is implemented as a package extension that loads only when
`JuMP` is available. A separate solver package (e.g. `HiGHS`, `Gurobi`) must
also be loaded and its `Optimizer` passed as the first argument.

Fields:
- `optimizer`         — solver constructor passed to `JuMP.Model`.
- `Z::Float64`        — weight on `t_last_end` in the objective (default `10.0`,
                        matches DagScheduler.jl).
- `time_limit_sec`    — solver wall-clock budget in seconds (default `60.0`).

Usage:
```julia
using Dagger, JuMP, HiGHS
Dagger.with_options(scheduler=Dagger.JuMPScheduler(HiGHS.Optimizer)) do
    ...
end
```

Loading order is enforced at construction time; without `JuMP` loaded the
constructor raises an informative `ArgumentError`.
"""
struct JuMPScheduler <: DataDepsScheduler
    optimizer::Any
    Z::Float64
    time_limit_sec::Float64

    function JuMPScheduler(optimizer; Z::Real=10.0, time_limit_sec::Real=60.0)
        if Base.get_extension(@__MODULE__, :JuMPExt) === nothing
            throw(ArgumentError("JuMPScheduler requires JuMP to be loaded. Run `using JuMP` (and a solver package such as HiGHS) before constructing this scheduler."))
        end
        time_limit_sec > 0 || throw(ArgumentError("JuMPScheduler: time_limit_sec must be > 0, got $time_limit_sec"))
        return new(optimizer, Float64(Z), Float64(time_limit_sec))
    end
end

Base.similar(s::JuMPScheduler) =
    JuMPScheduler(s.optimizer; Z=s.Z, time_limit_sec=s.time_limit_sec)

# `datadeps_schedule_dag_aot!(::JuMPScheduler, ...)` lives in ext/JuMPExt.jl;
# the constructor prevents instantiation without the extension.
const OPT_DEFAULT_MILP_THRESHOLD = 12

"""
    OptimizingScheduler{R<:Random.AbstractRNG} <: DataDepsScheduler

Adaptive DAG scheduler that dispatches to `JuMPScheduler` for small
instances and to `SimulatedAnnealingScheduler` seeded from
`IteratedGreedyScheduler` for larger ones. The
MILP path is used when `K ≤ milp_threshold`, the `optimizer` argument is
non-`nothing`, and the JuMP extension is loaded; otherwise the
heuristic pipeline `SA(IG(Greedy))` runs regardless of `K`. Sub-schedulers
are constructed fresh per invocation with the fields below forwarded
verbatim, so the same `OptimizingScheduler` instance can adapt across
`spawn_datadeps` blocks with different `K`.

Fields:
- `optimizer`               — solver constructor (e.g. `HiGHS.Optimizer`)
                              or `nothing` to disable the MILP path.
- `milp_threshold::Int`     — inclusive upper bound on `K` for the MILP
                              path.
- `milp_time_limit_sec`     — forwarded to `JuMPScheduler`.
- `milp_Z`                  — forwarded to `JuMPScheduler`.
- `ig_n_iters`, `ig_destroy_frac`, `ig_time_limit_sec` — forwarded to IG.
- `sa_q`, `sa_k`, `sa_n_restarts`, `sa_time_limit_sec` — forwarded to SA.
- `rng::R`                  — used by IG/SA; unused on the MILP path.
"""
struct OptimizingScheduler{R<:Random.AbstractRNG} <: DataDepsScheduler
    optimizer::Any
    milp_threshold::Int
    milp_time_limit_sec::Float64
    milp_Z::Float64
    ig_n_iters::Int
    ig_destroy_frac::Float64
    ig_time_limit_sec::Float64
    sa_q::Float64
    sa_k::Float64
    sa_n_restarts::Int
    sa_time_limit_sec::Float64
    rng::R

    function OptimizingScheduler(;
        optimizer=nothing,
        milp_threshold::Integer=OPT_DEFAULT_MILP_THRESHOLD,
        milp_time_limit_sec::Real=60.0,
        milp_Z::Real=10.0,
        ig_n_iters::Integer=IG_DEFAULT_N_ITERS,
        ig_destroy_frac::Real=IG_DEFAULT_DESTROY_FRAC,
        ig_time_limit_sec::Real=IG_DEFAULT_TIME_LIMIT_SEC,
        sa_q::Real=SA_DEFAULT_Q,
        sa_k::Real=SA_DEFAULT_K,
        sa_n_restarts::Integer=SA_DEFAULT_N_RESTARTS,
        sa_time_limit_sec::Real=SA_DEFAULT_TIME_LIMIT_SEC,
        rng::R=Random.default_rng(),
    ) where {R<:Random.AbstractRNG}
        milp_threshold >= 0 || throw(ArgumentError("OptimizingScheduler: milp_threshold must be ≥ 0, got $milp_threshold"))
        milp_time_limit_sec > 0 || throw(ArgumentError("OptimizingScheduler: milp_time_limit_sec must be > 0, got $milp_time_limit_sec"))
        ig_n_iters >= 0 || throw(ArgumentError("OptimizingScheduler: ig_n_iters must be ≥ 0, got $ig_n_iters"))
        (0 < ig_destroy_frac && ig_destroy_frac <= 1) ||
            throw(ArgumentError("OptimizingScheduler: ig_destroy_frac must be in (0, 1], got $ig_destroy_frac"))
        ig_time_limit_sec > 0 || throw(ArgumentError("OptimizingScheduler: ig_time_limit_sec must be > 0, got $ig_time_limit_sec"))
        (0 < sa_q && sa_q < 1) || throw(ArgumentError("OptimizingScheduler: sa_q must be in (0, 1), got $sa_q"))
        sa_k > 0 || throw(ArgumentError("OptimizingScheduler: sa_k must be > 0, got $sa_k"))
        sa_n_restarts >= 1 || throw(ArgumentError("OptimizingScheduler: sa_n_restarts must be ≥ 1, got $sa_n_restarts"))
        sa_time_limit_sec > 0 || throw(ArgumentError("OptimizingScheduler: sa_time_limit_sec must be > 0, got $sa_time_limit_sec"))

        return new{R}(
            optimizer,
            Int(milp_threshold),
            Float64(milp_time_limit_sec),
            Float64(milp_Z),
            Int(ig_n_iters),
            Float64(ig_destroy_frac),
            Float64(ig_time_limit_sec),
            Float64(sa_q),
            Float64(sa_k),
            Int(sa_n_restarts),
            Float64(sa_time_limit_sec),
            rng,
        )
    end
end

Base.similar(s::OptimizingScheduler) =
    OptimizingScheduler(; optimizer=s.optimizer,
                          milp_threshold=s.milp_threshold,
                          milp_time_limit_sec=s.milp_time_limit_sec,
                          milp_Z=s.milp_Z,
                          ig_n_iters=s.ig_n_iters,
                          ig_destroy_frac=s.ig_destroy_frac,
                          ig_time_limit_sec=s.ig_time_limit_sec,
                          sa_q=s.sa_q,
                          sa_k=s.sa_k,
                          sa_n_restarts=s.sa_n_restarts,
                          sa_time_limit_sec=s.sa_time_limit_sec,
                          rng=copy(s.rng))

"""
    opt_uses_milp(sched::OptimizingScheduler, n_tasks::Integer) -> Bool

Return `true` when `sched` would dispatch a DAG of `n_tasks` tasks through
the MILP path (i.e. `JuMPScheduler`), and `false` when it would use the
heuristic pipeline `SimulatedAnnealingScheduler(IteratedGreedyScheduler(GreedyScheduler()))`.

The MILP path is taken iff all of the following hold:
- `n_tasks ≤ sched.milp_threshold`
- `sched.optimizer !== nothing`
- the JuMP package extension is loaded (`Base.get_extension(Dagger, :JuMPExt)` non-`nothing`)

Exposed so callers and tests can predict routing without invoking the
scheduler.
"""
function opt_uses_milp(sched::OptimizingScheduler, n_tasks::Integer)
    return n_tasks <= sched.milp_threshold &&
           sched.optimizer !== nothing &&
           Base.get_extension(@__MODULE__, :JuMPExt) !== nothing
end

function datadeps_schedule_dag_aot!(sched::OptimizingScheduler, schedule, dag_spec, all_procs, all_scope)
    n_tasks = nv(dag_spec.g)
    n_tasks == 0 && return

    if opt_uses_milp(sched, n_tasks)
        sub = JuMPScheduler(sched.optimizer;
                            Z=sched.milp_Z,
                            time_limit_sec=sched.milp_time_limit_sec)
    else
        ig = IteratedGreedyScheduler(GreedyScheduler();
                                     n_iters=sched.ig_n_iters,
                                     destroy_frac=sched.ig_destroy_frac,
                                     rng=sched.rng,
                                     time_limit_sec=sched.ig_time_limit_sec)
        sub = SimulatedAnnealingScheduler(ig;
                                          q=sched.sa_q,
                                          k=sched.sa_k,
                                          n_restarts=sched.sa_n_restarts,
                                          rng=sched.rng,
                                          time_limit_sec=sched.sa_time_limit_sec)
    end

    datadeps_schedule_dag_aot!(sub, schedule, dag_spec, all_procs, all_scope)
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

                raw_arg = arg isa DTask ? fetch(arg; raw=true) : arg

                push!(task_raw_args, raw_arg)
            end
        end

        # Determine if this task stays in this layer
        if any(raw_arg -> raw_arg in layer_data, task_raw_args)
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
