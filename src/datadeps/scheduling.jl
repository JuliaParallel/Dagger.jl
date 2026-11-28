abstract type DataDepsScheduler end

# Default for user-defined schedulers with a zero-arg constructor. Schedulers
# that carry mutable state should specialize `similar` to return a fresh shard
# (used when hierarchical scheduling clones a scheduler per partition).
Base.similar(s::DataDepsScheduler) = typeof(s)()

mutable struct RoundRobinScheduler <: DataDepsScheduler
    proc_idx::Int
    RoundRobinScheduler() = new(1)
end
Base.similar(::RoundRobinScheduler) = RoundRobinScheduler()
"""
    is_driver_thread_proc(proc::Processor) -> Bool

Whether `proc` is the thread that plans (and submits from) a Datadeps region.
Under uniform (MPI) execution every task is pinned to the processor the
scheduler chose, round-robin over the canonical processor list, which starts
with thread 1 of rank 0: the thread that is busy planning. A task pinned there
cannot start until planning yields it, and the first one is the critical path of
most graphs (a Cholesky's first `potrf`). `RoundRobinScheduler` therefore skips
such processors for tasks that name no processor of their own. Processors
default to `false`; MPIExt marks thread 1 of each rank.
"""
is_driver_thread_proc(::Processor) = false


"""
    datadeps_schedule_is_stateless(sched) -> Bool

Whether `datadeps_schedule_task` depends only on `sched`'s own counter, the
task's scope and the processor list, and never on the `DataDepsState` that
planning the *earlier* tasks mutates. Such a scheduler may be asked about a
task before the tasks ahead of it are planned (in order), which is what lets
`schedule_slot_window!` batch the slot aliasing exchanges of a window of tasks.
"""
datadeps_schedule_is_stateless(::DataDepsScheduler) = false
datadeps_schedule_is_stateless(::RoundRobinScheduler) = true
function datadeps_schedule_task(sched::RoundRobinScheduler, state::DataDepsState, all_procs, all_scope, task_scope, spec::DTaskSpec, task::DTask)
    proc_idx = sched.proc_idx
    our_proc = all_procs[proc_idx]
    if task_scope === all_scope
        # all_procs is already limited to scope
    elseif task_scope === DefaultScope()
        # Common case: no user-specified scope. `DefaultScope()` is a shared
        # singleton, so `===` identifies it exactly. Its inner scope is
        # `AnyScope`, so `constrain(task_scope, all_scope)` can never be an
        # `InvalidScope` and the compatibility check is skippable; all that
        # remains of `proc_in_scope(proc, DefaultScope())` is its lone
        # `DefaultEnabledTaint`, i.e. `default_enabled(proc)`.
        # Under uniform execution the driver thread is skipped as well (see
        # `is_driver_thread_proc`), unless the list holds nothing else (one thread per
        # rank), where skipping would funnel every task to one rank. A task with a
        # scope of its own keeps whatever it names.
        avoid = uniform_execution() && any(!is_driver_thread_proc, all_procs)
        skipped = 0
        while !default_enabled(our_proc) || (avoid && skipped < length(all_procs) - 1 && is_driver_thread_proc(our_proc))
            proc_idx = mod1(proc_idx + 1, length(all_procs))
            our_proc = all_procs[proc_idx]
            skipped += 1
        end
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
function datadeps_schedule_task(sched::NaiveScheduler, state::DataDepsState, all_procs, all_scope, task_scope, spec::DTaskSpec, task::DTask)
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
function datadeps_schedule_task(sched::UltraScheduler, state::DataDepsState, all_procs, all_scope, task_scope, spec::DTaskSpec, task::DTask)
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
