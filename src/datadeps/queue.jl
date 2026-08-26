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

const DATADEPS_CURRENT_TASK = TaskLocalValue{Union{DTask,Nothing}}(Returns(nothing))

# Tag for datadeps-internal tasks (copies, frees) launched outside the user
# task queue. Under uniform (MPI) execution every task needs a unique,
# rank-uniform tag for its P2P transfers; under Distributed this is unused.
datadeps_task_tag() = uniform_execution() ? UInt32(to_tag()) : nothing

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

At the end of executing `f`, `spawn_datadeps` will, by default
(`sync=true`), wait for all launched tasks to complete (including deferred
write-back and free tasks from this and any earlier un-synchronized region on
this task), rethrowing the first error, if any. Passing `sync=false` instead
returns as soon as `f` has been planned, leaving execution (and the eventual
write-back/free) to a later [`Dagger.synchronize`](@ref) call -- this is what
lets consecutive regions pipeline instead of each one being a full barrier.
`sync` defaults to `Dagger.DATADEPS_SYNC[]`, itself defaulting to `true`, so
every existing caller is unaffected unless it opts in. Neither
`hierarchical=true` (the default) nor MPI/SPMD execution
(`uniform_execution()`) forces `sync=true` anymore; see the N.B. below.
Deferred planning/flushing is rank-uniform, protected by
[`with_datadeps_planning_token`](@ref) so only one Julia Task per rank ever
performs it at a time. The result of `f` will be returned from
`spawn_datadeps`.
"""
function spawn_datadeps(f::Base.Callable; static::Bool=true,
                        traversal::Symbol=:inorder,
                        scheduler::Union{DataDepsScheduler,Nothing}=nothing,
                        aliasing::Bool=true,
                        launch_wait::Union{Bool,Nothing}=nothing,
                        hierarchical::Union{Bool,Nothing}=nothing,
                        sync::Union{Bool,Nothing}=nothing)
    if !static
        throw(ArgumentError("Dynamic scheduling is no longer available"))
    end
    if traversal != :inorder
        throw(ArgumentError("Traversal order is no longer configurable, and always :inorder"))
    end
    if !aliasing
        throw(ArgumentError("Aliasing analysis is no longer optional"))
    end
    # The context is task-local and, starting this phase, genuinely persists
    # across regions when `sync=false`: `ddctx.slots` still spans the whole
    # region, not just planning (the copies and frees that touch a slot are
    # tasks, so entries stay checked out until they've all drained -- see
    # `retiring_slots`).
    ddctx = get_context!()

    # Refuse to plan on top of a region that failed without anyone having
    # observed it yet. Only reachable under `sync=false`: default settings
    # always drain (and thereby observe-and-clear) trailing every region, so
    # a caller using defaults never sees this -- see `_do_synchronize!`'s N.B.
    # on when `err`/`err_region` clears.
    poisoned = @lock ddctx.lock begin
        ddctx.err === nothing ? nothing :
            DataDepsPoisonedError(ddctx.err_region, ddctx.err, _resolve_region_bt(ddctx, ddctx.err_region))
    end
    poisoned === nothing || throw(poisoned)

    # Bound how large a carried-over `state` may get before we insist on a
    # drain. See `DATADEPS_STATE_LIMIT` (context.jl) for the measurements and
    # for why this is a stopgap rather than the real fix. Deliberately here,
    # at region entry, rather than next to `apply_inflight_backpressure!`
    # inside `distribute_tasks!`: a drain re-enters
    # `with_datadeps_planning_token`, which the planner already holds by that
    # point.
    apply_state_size_backpressure!(ddctx)

    hierarchical = something(hierarchical, DATADEPS_HIERARCHICAL[], true)::Bool
    # N.B. Neither `hierarchical=true` (the default) nor uniform (MPI/SPMD)
    # execution forces `sync=true` anymore. Uniform execution stopped forcing
    # it in Phase 7a: a deferred epilogue spanning multiple regions does mean
    # tags/`MPIRefID`s get allocated by code running arbitrarily long after
    # the region that requested them returned, but that's rank-uniform as
    # long as every rank performs those allocations in the same relative
    # order -- which is true of the deferred write-back/free emission (still
    # sorted by `arg_w.hash`, still driven purely by `ddctx.state`, itself
    # built identically on every rank) and is guarded by
    # `with_datadeps_planning_token` (context.jl) so a second Julia Task
    # racing to plan or flush on the same rank gets a clear error instead of
    # silently reordering the allocation sequence relative to another rank's.
    #
    # Hierarchical stopped forcing it in Phase 7b: `distribute_tasks_hierarchical!`
    # now seeds its shared-state path directly from `ddctx.state`/`ddctx.write_num`
    # (carry-in) and defers that path's write-back/free into `ddctx.pending_writeback`/
    # `ddctx.pending_free` instead of copying-from-and-freeing immediately
    # (publish-back) -- see `hierarchical.jl`'s `_distribute_tasks_hierarchical!`
    # for the carry-in/publish-back mechanics and for why the *other*
    # (single-memory-space, parallel-per-partition) scheduling strategy there
    # still forces a synchronous drain around itself rather than participating.
    sync = something(sync, DATADEPS_SYNC[], true)::Bool

    # N.B. Store the *raw* backtrace, not `stacktrace(backtrace())`. Resolving
    # frames to symbols eagerly costs ~119us against ~4.6us for the raw
    # capture -- 26x -- because it walks debug info for every frame. That is
    # invisible next to a large region's planning cost but not next to a small
    # one: `@stencil` emits one region per expression per iteration, ~8ms each,
    # where it measured as a several-percent regression across every stencil
    # baseline. Resolution is deferred to `stacktrace(bt)` at the point an
    # error is actually reported, which is rare and already slow.
    #
    # `region_id`/`region_bt` are mutated under `ddctx.lock`: `_do_synchronize!`
    # (possibly running on a foreign task, via `synchronize_task!`/
    # `synchronize_all!`) reads and evicts `region_bt` entries under the same
    # lock, and `ContextQueue.enqueue!` reads `region_id` under it too.
    bt = backtrace()
    @lock ddctx.lock begin
        ddctx.region_id += 1
        ddctx.region_bt[ddctx.region_id] = bt
    end
    ddctx.slots = SlotReuseRegion(Set{UInt}())
    return with(SLOT_REUSE_REGION => ddctx.slots) do
        try
            _spawn_datadeps(ddctx, f, scheduler, launch_wait, hierarchical, sync)
        finally
            maybe_drop_context!()
            # If the context is still this task's live, registered one at
            # this point, it just survived past a `spawn_datadeps` call
            # (`sync=false`, or a planning failure that left tasks retained
            # -- see `_spawn_datadeps`'s N.B.) and needs a drain watcher armed
            # so it isn't silently forgotten if nothing ever synchronizes it.
            # See `maybe_arm_drain_watcher!` for why this isn't done eagerly
            # in `get_context!` instead.
            DATADEPS_CONTEXT[] === ddctx && maybe_arm_drain_watcher!(ddctx)
        end
    end
end
function _spawn_datadeps(ddctx::DataDepsContext, f::Base.Callable, scheduler, launch_wait, hierarchical, sync::Bool)
    # `ContextQueue` replaces `WaitAllQueue` as the region's `:task_queue`,
    # collecting into `ddctx.inflight` instead of a queue-local `Vector{DTask}`.
    queue_outer = ContextQueue(get_options(:task_queue, DefaultTaskQueue()), ddctx)
    result = try
        with_options(; task_queue=queue_outer) do
            scheduler = something(scheduler, DATADEPS_SCHEDULER[], RoundRobinScheduler())
            launch_wait = something(launch_wait, DATADEPS_LAUNCH_WAIT[], false)::Bool
            # N.B. Declared as a named function (rather than a closure bound to a
            # local) so it shows up by name in stacktraces and profiles, which is
            # the boundary between region setup and the whole planning pipeline.
            function run_distribute(queue)
                # One aliasing memo per region: planning asks for the same chunks'
                # aliasing info from Phase 1, from every slot, and from the write-back
                # epilogue, and each answer costs a round-trip to the owner.
                ddctx.memo = ChunkAinfoMemo()
                with(CHUNK_AINFO_MEMO => ddctx.memo) do
                    # Under uniform (MPI/SPMD) execution, only one Julia Task
                    # per rank may be inside here at a time -- see
                    # `with_datadeps_planning_token` (context.jl). A no-op
                    # wrapper otherwise.
                    with_datadeps_planning_token() do
                        if hierarchical
                            distribute_tasks_hierarchical!(queue, ddctx)
                        else
                            distribute_tasks!(queue, ddctx)
                        end
                    end
                end
            end
            if launch_wait
                result = spawn_bulk() do
                    queue = DataDepsTaskQueue(get_options(:task_queue); scheduler)
                    with_options(f; task_queue=queue)
                    run_distribute(queue)
                end
            else
                queue = DataDepsTaskQueue(get_options(:task_queue); scheduler)
                result = with_options(f; task_queue=queue)
                run_distribute(queue)
            end
            return result
        end
    finally
        # This region's `SlotReuseRegion` cannot be released until its
        # (possibly still in-flight, under `sync=false`) copy/free tasks are
        # known done -- see `retiring_slots`'s field comment in context.jl --
        # so it is handed off for later release rather than released inline.
        # Done unconditionally, including on a planning failure below.
        #
        # N.B. Unlike the old `wait_all`-based epilogue (and Phase 3, which
        # matched it), a planning failure here does *not* `empty!(ddctx.inflight)`.
        # Whatever this region -- or an earlier, healthy one sharing this
        # context -- already launched is doing real work the user's data
        # depends on; it stays tracked and gets drained by whichever
        # `synchronize`-family call runs next (including the task-exit drain
        # / finalizer backstop, if nothing else ever does). A pure planning
        # failure (a bad scheduler, an invalid scope) does not poison the
        # context for *future* regions either: any tasks it already launched
        # remain individually consistent, and the exception below propagates
        # to the caller immediately, exactly as it always has -- nothing is
        # silently lost, and nothing is silently blocked either.
        @lock ddctx.lock push!(ddctx.retiring_slots, ddctx.slots)
    end
    if sync
        _do_synchronize!(ddctx; write_back=true, free=true, gpu_sync=:fence,
                          check_errors=true, wrap_errors=false, from_owner=true)
    end
    return result
end
const DATADEPS_SCHEDULER = ScopedValue{Union{DataDepsScheduler,Nothing}}(nothing)
const DATADEPS_LAUNCH_WAIT = ScopedValue{Union{Bool,Nothing}}(nothing)
const DATADEPS_HIERARCHICAL = ScopedValue{Union{Bool,Nothing}}(nothing)

"""
Default for `spawn_datadeps`'s `sync` keyword: `true` reproduces today's full
per-region barrier; `false` lets consecutive regions pipeline, deferring
write-back, frees, and waiting to a later `Dagger.synchronize`-family call.
Overridable per-call via the `sync` keyword; both are overridden to `true`
under `hierarchical=true` or MPI/SPMD execution regardless (see
`spawn_datadeps`).
"""
const DATADEPS_SYNC = ScopedValue{Union{Bool,Nothing}}(nothing)

# Current task uid, propagated into `tochunk` so uniform-execution backends
# (MPIExt) can derive deterministic, rank-agreed handle IDs. Core datadeps sets
# this during planning/execution; MPIExt reads it. 0 means "no task in scope".
const DATADEPS_THUNK_ID = ScopedValue{Int64}(0)

# Deterministic, rank-agreed task tag for uniform execution. Only invoked when
# `uniform_execution()` holds (i.e. under MPIExt), which provides the method.
function to_tag end

"""
    _spec_has_region_dtask_arg(spec::DTaskSpec, region_uids::Set{UInt}) -> Bool

Whether `spec` takes an argument produced by another task in the same Datadeps
region. Used to stop a *partial* DAG build before it reaches such a task.
"""
function _spec_has_region_dtask_arg(spec::DTaskSpec, region_uids::Set{UInt})
    for _arg in spec.fargs
        arg, _ = unwrap_inout(value(_arg))
        if arg isa DTask && arg.uid in region_uids
            return true
        end
    end
    return false
end

"""
    datadeps_build_schedule!(scheduler, pairs, all_procs, all_scope;
                             region_uids=nothing, cache=true) -> (dag_spec, schedule)

Build a `DAGSpec` from `pairs` (in submission order) and compute an AOT
processor assignment over `all_procs`/`all_scope`, reusing a cached schedule
when an equivalent DAG has been seen before and caching a freshly-computed one
otherwise. Schedulers with no `datadeps_schedule_dag_aot!` method leave the
schedule empty, and those tasks fall back to JIT placement in
`distribute_task!`.

`region_uids`, when given, is the set of *all* in-region task uids, and makes
this bail out before `dag_add_task!` reaches a task whose argument is produced
by another in-region task. That matters only when `pairs` is a *subset* of the
region, as it is for one partition of the hierarchical path: a producer sitting
in another partition is not in `pairs`, so `dag_add_task!` would treat it as an
ordinary external value and `fetch` it — blocking forever, since that partition
has not been submitted yet. For the flat path `pairs` is the whole region and
`dag_add_task!`'s own in-DAG check suffices, so `region_uids` can be omitted.

`cache=false` neither consults nor extends the schedule cache. The hierarchical
path passes it when planning one partition: it caches the merged whole-region
plan itself, under the whole-region `DAGSpec`, and a partition's partial spec
would only lengthen every later lookup.

Used by both `distribute_tasks!` (whole region) and
`distribute_tasks_hierarchical!` (per partition).
"""
function datadeps_build_schedule!(scheduler::DataDepsScheduler,
                                  pairs::Vector{DTaskPair},
                                  all_procs, all_scope;
                                  region_uids::Union{Set{UInt},Nothing}=nothing,
                                  cache::Bool=true)
    # A JIT scheduler never reads the result, and building the spec is neither
    # free nor side-effect-free (see `datadeps_uses_aot`), so don't build one.
    dag_spec = DAGSpec()
    datadeps_uses_aot(scheduler) || return dag_spec, Dict{DTask, Processor}()
    for (spec, task) in pairs
        if region_uids !== nothing && _spec_has_region_dtask_arg(spec, region_uids)
            # Produced by an in-region task that may live outside `pairs`;
            # defer the rest to JIT scheduling.
            break
        end
        if !dag_add_task!(dag_spec, spec, task)
            # This task depends on an in-region task's result; defer the rest
            # to JIT scheduling (they won't appear in `schedule`).
            break
        end
    end

    # Attempt to find any matching DAG specs and reuse their schedule
    schedule = Dict{DTask, Processor}()
    schedule_cache = cache ? datadeps_schedule_cache(scheduler) : nothing
    cache_hit = false
    stale = nothing
    if schedule_cache !== nothing
        found, stale = _schedule_cache_find(scheduler, schedule_cache, dag_spec)
        if found !== nothing
            @dagdebug nothing :spawn_datadeps "Found matching DAG spec!"
            for (id, proc) in found.id_to_proc
                uid = dag_spec.id_to_uid[id]
                task_idx = findfirst(spec_task -> spec_task.task.uid == uid, pairs)
                task = pairs[task_idx].task
                schedule[task] = proc
            end
            cache_hit = true
        end
    end

    isempty(dag_spec) && return dag_spec, schedule

    accel = current_acceleration()
    if !cache_hit && aot_plans_locally(accel)
        # Compute a fresh AOT schedule (no-op for schedulers that fall back
        # to JIT in distribute_task!)
        datadeps_schedule_dag_aot!(scheduler, schedule, dag_spec, all_procs, all_scope)
    end
    # Hit or miss, every rank exchanges exactly once here, so the exchange
    # stays aligned even if the ranks' caches were ever to disagree.
    uniform_aot_schedule!(accel, schedule, pairs, all_procs)

    # Persist the schedule for reuse by future equivalent DAGs. This follows
    # the exchange, so under uniform execution every rank caches the same plan.
    if schedule_cache !== nothing && !cache_hit && !isempty(schedule)
        # A plan replacing a provisional one is final either way, so a DAG shape
        # is planned at most twice (see `datadeps_plan_informed`).
        spec_schedule = DAGSpecSchedule(stale === nothing &&
                                        !datadeps_plan_informed(scheduler, dag_spec))
        for (task, proc) in schedule
            id = dag_spec.uid_to_id[task.uid]
            spec_schedule.id_to_proc[id] = proc
        end
        _schedule_cache_store!(schedule_cache, dag_spec, spec_schedule, stale)
    end

    return dag_spec, schedule
end

"""
    aot_plans_locally(accel) -> Bool

Whether this process computes AOT schedules itself, rather than adopting one
computed elsewhere through [`uniform_aot_schedule!`](@ref). Under uniform
execution only one rank needs to run the planner.
"""
aot_plans_locally(::Acceleration) = true

"""
    uniform_aot_schedule!(accel, schedule, pairs, all_procs) -> schedule

Make an AOT `schedule` for the tasks in `pairs` identical on every process that
plans the region. A no-op unless execution is uniform.

Under uniform execution (MPI) every rank plans every region, and each must place
every task exactly where its peers do. AOT schedulers cannot promise that on
their own: their cost models read rank-local metrics (each rank records only the
tasks it ran), and a search with a wall-clock budget stops after a different
number of iterations on each rank. Two ranks that disagree about one task's
processor desynchronize every later tag, so the acceleration has one rank plan
and the others adopt that plan. Every rank must call this at the same point.
"""
uniform_aot_schedule!(::Acceleration, schedule, pairs, all_procs) = schedule

"""
    DATADEPS_LAUNCH_NS_PER_TASK

How long flat-mode Datadeps takes to launch one task (computing its aliasing,
allocating remote buffers, spawning its copies and the task itself): a running
average over recent regions, in nanoseconds, or 0 before any has been timed.
Planners use it as the interval at which a region releases its tasks
(`DATADEPS_RELEASE_NS`). Process-local; under MPI only rank 0 plans.
"""
const DATADEPS_LAUNCH_NS_PER_TASK = Threads.Atomic{Float64}(0.0)

"""
    DATADEPS_HIER_LAUNCH_NS_PER_TASK

The same for the hierarchical path's shared-state launch loop
(`schedule_partitions_sequential!`), which walks every partition's tasks
from one task just as the flat loop does, only with the aliasing and
dependency work done beforehand. Kept apart from the flat rate because the
two loops cost differently per task, and a plan for one path released at the
other's rate is wrong in the way lesson 62 describes: planned without any
release times, the hierarchical planners moved a blocked stencil off its
owners on four MPI nodes (1.7 GB per call against RoundRobin's 24 MB), where
the flat planners, released at the measured rate, kept it on them.
"""
const DATADEPS_HIER_LAUNCH_NS_PER_TASK = Threads.Atomic{Float64}(0.0)

# Regions smaller than this are dominated by fixed per-region costs.
const LAUNCH_RATE_MIN_TASKS = 16

record_launch_rate!(ntasks::Int, ns::Integer) =
    _record_launch_rate!(DATADEPS_LAUNCH_NS_PER_TASK, ntasks, ns)
record_hier_launch_rate!(ntasks::Int, ns::Integer) =
    _record_launch_rate!(DATADEPS_HIER_LAUNCH_NS_PER_TASK, ntasks, ns)
function _record_launch_rate!(rate::Threads.Atomic{Float64}, ntasks::Int, ns::Integer)
    ntasks < LAUNCH_RATE_MIN_TASKS && return
    x = Float64(ns) / ntasks
    old = rate[]
    rate[] = old == 0.0 ? x : 0.7 * old + 0.3 * x
    return
end

function distribute_tasks!(queue::DataDepsTaskQueue, ddctx::DataDepsContext)
    # N.B. Named `ddctx`, not `ctx`: this file's `flush_pending_writeback!`
    # (synchronize.jl), which shares this function's former copy-from-skip
    # branch, locally rebinds `ctx = Sch.eager_context()`. `distribute_tasks!`
    # no longer does that itself, but keeps the same naming discipline as
    # every other function in this directory that takes a `DataDepsContext` --
    # see the Phase 3/4 history for why this bit once before: a `for` loop is
    # a nested (not shadowing) scope, so a same-named local rebind silently
    # clobbers a parameter for the rest of the call, no ambiguity error.
    #= TODO: Improvements to be made:
    # - Support for copying non-AbstractArray arguments
    # - Parallelize read copies
    # - Unreference unused slots
    # - Reuse memory when possible
    # - Account for differently-sized data
    =#

    # Backpressure: a planner running under `sync=false` outruns execution
    # arbitrarily otherwise, queuing up thousands of regions' worth of work
    # (and their slots/buffers) ahead of whatever's actually retiring them.
    apply_inflight_backpressure!(ddctx)

    # Get the set of all processors to be scheduled on
    accel = current_acceleration()
    accel_procs = filter(procs(Dagger.Sch.eager_context())) do proc
        Dagger.accel_matches_proc(accel, proc)
    end
    all_procs = unique(vcat([collect(Dagger.get_processors(gp)) for gp in accel_procs]...))
    select_processors_uniform!(all_procs, accel)
    scope = get_compute_scope()
    filter!(proc->proc_in_scope(proc, scope), all_procs)
    if isempty(all_procs)
        throw(Sch.SchedulingException("No processors available, try widening scope"))
    end
    if uniform_execution(accel)
        for proc in all_procs
            @check_uniform(proc)
        end
    end
    all_scope = UnionScope(map(ExactScope, all_procs))

    upper_queue = get_options(:task_queue)

    # Plan the whole region ahead of time, if the scheduler supports it.
    # Schedulers that don't define `datadeps_schedule_dag_aot!` leave `schedule`
    # empty and every task falls through to JIT placement below. A planner is
    # told how fast this loop will release the tasks it places (see
    # `DATADEPS_RELEASE_NS`).
    release_ns = DATADEPS_LAUNCH_NS_PER_TASK[]
    _dag_spec, schedule = if datadeps_uses_aot(queue.scheduler) && release_ns > 0.0
        with(DATADEPS_RELEASE_NS => release_ns) do
            datadeps_build_schedule!(queue.scheduler, queue.seen_tasks, all_procs, all_scope)
        end
    else
        datadeps_build_schedule!(queue.scheduler, queue.seen_tasks, all_procs, all_scope)
    end

    # Start launching tasks and necessary copies
    # N.B. `state` now genuinely persists across regions on this (flat) path:
    # region N+1's planning consults region N's `ainfos_owner`/`ainfos_readers`/
    # `arg_current` directly. It is only rebuilt when there is none yet (the
    # first region since context creation, or the first since the last full
    # drain reset it -- see `_do_synchronize!`, step 7): `distribute_tasks!`
    # itself must never overwrite a `state` that earlier, still-undrained
    # regions' pending write-backs/frees and in-flight tasks refer to. This is
    # inseparable from deferring the free loop below: freeing every region
    # would `unsafe_free!` buffers a *later* region's `arg_current` still
    # points at.
    if !isdefined(ddctx, :state)
        ddctx.state = DataDepsState()
    end
    state = ddctx.state
    write_num = ddctx.write_num
    proc_to_scope_lfu = BasicLFUCache{Processor,AbstractScope}(1024)
    launch_start = time_ns()
    for pair in queue.seen_tasks
        spec = pair.spec
        task = pair.task
        proc = get(schedule, task, nothing)
        write_num = distribute_task!(queue, state, all_procs, all_scope, spec, task, spec.fargs, proc_to_scope_lfu, write_num; proc)
    end
    record_launch_rate!(length(queue.seen_tasks), time_ns() - launch_start)

    defer_writeback_and_free!(ddctx, state, write_num)
    return
end

"""
    defer_writeback_and_free!(ddctx::DataDepsContext, state::DataDepsState, write_num::Int)

Record `state`'s writers as pending write-back and mark its object cache as
worth re-examining for frees, instead of emitting either now, then persist
`write_num` into `ddctx`. Shared by flat `distribute_tasks!` and hierarchical's
shared-state scheduling path (`_distribute_tasks_hierarchical!`'s
`use_shared_state` branch) -- both plan directly against `ddctx.state`, so
both need the identical publish-back: nothing here is specific to how `state`
was populated, only that it *is* `ddctx.state`.

Deferred to flush time (`flush_pending_writeback!`/`flush_pending_frees!`,
synchronize.jl) rather than emitted here, because:
- The write-back skip condition (`origin_space in arg_current[arg_w]`) can
  only be answered against the state as it stands when execution actually
  catches up to this point -- an intervening, not-yet-planned region may
  still make this write-back unnecessary. The `queue.jl:188-190`-style rule
  (mid-region copies serialize readers against later writers and must not be
  skipped) is unaffected: this is only ever the *final* write-back, and
  "final" now means "at the flush point" rather than "at this region's end".
- The free loop `unsafe_free!`s buffers that this same `state`'s cache lists
  as live, so running it every region would free slots a later,
  already-planned region's `arg_current` still points at. Deferral also
  means the old N.B. about not `mpi_cleanup_tid`-ing planning-time uid keys
  here still applies, but "reclaim at wait_all after the region finishes"
  now means "reclaim at synchronize" -- see `_do_synchronize!`.

Persisting `write_num` here is load-bearing across regions, not just
monotonic bookkeeping in preparation for it: the next region's `write_num`
must never repeat a value this one already used.
"""
function defer_writeback_and_free!(ddctx::DataDepsContext, state::DataDepsState, write_num::Int)
    @check_uniform(length(state.arg_owner))
    @lock ddctx.lock for arg_w in sort(collect(keys(state.arg_owner)); by=arg_w->arg_w.hash)
        @check_uniform(arg_w)
        push!(ddctx.pending_writeback, arg_w)
    end
    @lock ddctx.lock ddctx.pending_free[state] = nothing
    ddctx.write_num = write_num + 1
    return
end

"""
    apply_inflight_backpressure!(ddctx::DataDepsContext)

Block the calling (planning) task until `length(ddctx.inflight) <=
ddctx.inflight_limit`, by waiting on the oldest in-flight tasks first (FIFO).

Only meaningful under `sync=false`: `inflight` is always empty on entry
otherwise, since the previous region's trailing `_do_synchronize!` drained it.
A failure discovered here is recorded (see `_do_synchronize!`'s wait loop for
the same pattern) and, once draining stops, causes this call -- and hence the
region currently being planned -- to fail immediately via
[`DataDepsPoisonedError`](@ref), rather than let a planner outrun a broken
pipeline in silence.
"""
function apply_inflight_backpressure!(ddctx::DataDepsContext)
    while length(ddctx.inflight) > ddctx.inflight_limit
        # `popfirst!`/`delete!` (mutations `_do_synchronize!` also performs
        # under `ddctx.lock`) are taken under the lock; the potentially-slow
        # `fetch` below deliberately is not, so a foreign
        # `synchronize_task!`/`synchronize_all!` drain isn't blocked behind
        # this planner waiting on one task.
        task, region = @lock ddctx.lock begin
            t = popfirst!(ddctx.inflight)
            r = get(ddctx.task_region, t, ddctx.region_id)
            delete!(ddctx.task_region, t)
            (t, r)
        end
        try
            fetch(task; move_value=false, unwrap=false)
        catch err
            @lock ddctx.lock begin
                if ddctx.err === nothing
                    ddctx.err = err
                    ddctx.err_region = region
                end
            end
        end
    end
    if ddctx.err !== nothing
        throw(DataDepsPoisonedError(ddctx.err_region, ddctx.err,
                                    _resolve_region_bt(ddctx, ddctx.err_region)))
    end
    return
end
map_or_ntuple(f, xs::Vector) = map(f, 1:length(xs))
# N.B. Accept any `Tuple` (typed specs produce heterogeneous tuples of
# `TypedArgument{T}`, not a homogeneous `NTuple{N,T}`).
@inline map_or_ntuple(@specialize(f), xs::Tuple) = ntuple(f, Val(length(xs)))
# Indices, among the positional arguments `execute!` will receive (the function
# excluded), of the arguments a task writes; see the `writes` option.
function written_positions(task_arg_ws, deps_vec)
    writes = nothing
    npos = 0
    for idx in 2:length(task_arg_ws)
        arg_ws = task_arg_ws[idx]
        ispositional(arg_ws.pos) || continue
        npos += 1
        if any(di->deps_vec[di].writedep, arg_deps_range(arg_ws))
            writes = something(writes, Int[])
            push!(writes, npos)
        end
    end
    return something(writes, Int[])
end

function distribute_task!(queue::DataDepsTaskQueue, state::DataDepsState, all_procs, all_scope, spec::DTaskSpec{typed}, task::DTask, fargs, proc_to_scope_lfu, write_num::Int; proc::Union{Processor,Nothing}=nothing, ownership=nothing) where typed
    @specialize spec fargs

    if typed
        fargs::Tuple
    else
        fargs::Vector{Argument}
    end

    DATADEPS_CURRENT_TASK[] = task

    task_scope = @something(spec.options.compute_scope, spec.options.scope, DefaultScope())
    if proc === nothing
        # No AOT placement for this task; schedule it JIT
        our_proc = datadeps_schedule_task(queue.scheduler, state, all_procs, all_scope, task_scope, spec, task)
    else
        # Use the processor chosen by AOT scheduling
        our_proc = proc
    end
    @assert our_proc in all_procs
    our_space = only(memory_spaces(our_proc))
    @check_uniform(our_proc)
    @check_uniform(our_space)

    # Query once per task: this is also used for tags and occupancy below.
    uniform = uniform_execution()

    # Find the scope for this task (and its copies)
    # N.B. `task_scope` was already computed above for scheduling; the scope a
    # task is scheduled under and the scope its copies run under are the same.
    if uniform
        # Preserve the planner's deterministic thread choice. Widening to the
        # memory space loses it: the uniform scheduler ignores local capacity
        # and always picks the first usable processor, and MPI cannot steal.
        proc_scope = get!(proc_to_scope_lfu, our_proc) do
            ExactScope(our_proc)
        end
        our_scope = proc_in_scope(our_proc, task_scope) ? proc_scope :
                    InvalidScope(proc_scope, task_scope)
    elseif task_scope === DefaultScope()
        # Optimize for the common case (no user-specified scope), and cache the
        # proc=>scope mapping. `DefaultScope()` is a shared singleton, so `===`
        # identifies it exactly; the cached value is the same
        # `constrain(<our procs>, task_scope)` the general branch computes, and
        # it depends only on `our_proc` (via `our_space`) and the region-wide
        # `all_procs`, both of which are fixed for this cache's lifetime.
        our_scope = get!(proc_to_scope_lfu, our_proc) do
            our_procs = filter(proc->proc in all_procs, collect(processors(our_space)))
            return constrain(UnionScope(map(ExactScope, our_procs)...), DefaultScope())
        end
    else
        # Use the provided scope and constrain it to the available processors
        our_procs = filter(proc->proc in all_procs, collect(processors(our_space)))
        our_scope = constrain(UnionScope(map(ExactScope, our_procs)...), task_scope)
    end
    if our_scope isa InvalidScope
        throw(Sch.SchedulingException("Scopes are not compatible: $(our_scope.x), $(our_scope.y)"))
    end

    tid = task.uid
    # N.B. `with_value` returns a fresh argument rather than mutating; this is
    # required for typed specs, whose `TypedArgument`s disallow `setproperty!`.
    f = with_value(spec.fargs[1], move(default_processor(), our_proc, value(spec.fargs[1])))
    @dagdebug tid :spawn_datadeps "($(repr(value(f)))) Scheduling: $our_proc ($our_space)"

    # Populate all task dependencies into the state's flat per-task scratch
    # buffers (one concrete `TaskArgInfo` per argument, dependencies in
    # `deps_vec`). The moved function argument (`f`) replaces the original at
    # position 1.
    task_arg_ws = populate_task_info!(state, f, spec.fargs, spec, task)
    deps_vec = state.scratch_deps

    # Truncate the history for each argument
    for arg_ws in task_arg_ws
        for di in arg_deps_range(arg_ws)
            truncate_history!(state, deps_vec[di].arg_w)
        end
    end

    # Hierarchical scheduling only: for shared backing chunks whose current
    # version was produced by another partition, seed this partition's state so
    # the copy-to below pulls a fresh whole-chunk copy from the true owner (and
    # syncs on its producer). No-op on the flat path (`ownership === nothing`).
    # N.B. Currently always `nothing` -- see the dead-code note on
    # "Cross-partition chunk ownership" in `datadeps/hierarchical.jl`.
    if ownership !== nothing
        _sync_incoming_ownership!(state, ownership, our_space, task_arg_ws, write_num)
    end

    # Copy args from local to remote
    remote_args = state.scratch_remote
    resize!(remote_args, length(task_arg_ws))
    for (idx, arg_ws) in enumerate(task_arg_ws)
        arg = arg_ws.arg

        # Is the data written previously or now?
        if !arg_ws.may_alias
            @dagdebug tid :spawn_datadeps "($(repr(value(f))))[$(idx-1)] Skipped copy-to (immutable)"
            remote_args[idx] = arg
            continue
        end

        # Is the data writeable?
        if !arg_ws.inplace_move
            @dagdebug tid :spawn_datadeps "($(repr(value(f))))[$(idx-1)] Skipped copy-to (non-writeable)"
            remote_args[idx] = arg
            continue
        end

        # Is the source of truth elsewhere?
        arg_remote = get_or_generate_slot!(state, our_space, arg)
        for di in arg_deps_range(arg_ws)
            dep = deps_vec[di]
            arg_w = dep.arg_w
            dep_mod = arg_w.dep_mod
            # A replica that is still current here needs no copy: copies only
            # add replicas, and only writes make them stale (see `add_writer!`).
            # The write history cannot tell, because it records each copy as a
            # write in its destination space, so a read-only tile read on
            # workers 3, 4 and 3 again was copied back and forth between them.
            # The reader still waits on the copy that filled this replica, and
            # is still registered as its reader, so later writes wait for it.
            current = get(state.arg_current, arg_w, nothing)
            remainder = if current !== nothing && our_space in current
                NoAliasing()
            else
                first(compute_remainder_for_arg!(state, our_space, arg_w, write_num))
            end
            if remainder isa MultiRemainderAliasing
                enqueue_remainder_copy_to!(state, our_space, arg_w, remainder, value(f), idx, our_scope, task, write_num)
            elseif remainder isa FullCopy
                enqueue_copy_to!(state, our_space, arg_w, value(f), idx, our_scope, task, write_num)
            else
                @assert remainder isa NoAliasing "Expected NoAliasing, got $(typeof(remainder))"
                @dagdebug tid :spawn_datadeps "($(repr(value(f))))[$(idx-1)][$dep_mod] Skipped copy-to (up-to-date): $our_space"
            end
        end
        remote_args[idx] = arg_remote
    end
    write_num += 1

    # Validate that we're not accidentally performing a copy
    for (idx, arg_ws) in enumerate(task_arg_ws)
        arg = remote_args[idx]

        # Check that any mutable and written arguments are already in the correct space
        # N.B. We only do this check when the argument supports in-place
        # moves, because for the moment, we are not guaranteeing updates or
        # write-back of results
        if arg_ws.may_alias && arg_ws.inplace_move &&
           any(di->deps_vec[di].writedep, arg_deps_range(arg_ws))
            arg_space = memory_space(arg)
            @assert arg_space == our_space "($(repr(value(f))))[$(idx-1)] Tried to pass $(typeof(arg)) from $arg_space to $our_space"
        end
    end

    # Calculate this task's syncdeps
    if spec.options.syncdeps === nothing
        spec.options.syncdeps = take_syncdeps_set!()
    end
    if spec.options.tag === nothing && uniform
       spec.options.tag = to_tag()
    end
    syncdeps = spec.options.syncdeps
    for (idx, arg_ws) in enumerate(task_arg_ws)
        (arg_ws.may_alias && arg_ws.inplace_move) || continue
        for di in arg_deps_range(arg_ws)
            dep = deps_vec[di]
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
    end
    @dagdebug tid :spawn_datadeps "($(repr(value(f)))) Task has $(length(syncdeps)) syncdeps"

    # Launch user's task
    # N.B. Always as an untyped Vector{Argument} spec: the task's return type
    # was already inferred (and stored in options) at the original eager_spawn,
    # and eager_launch! would only convert a typed tuple straight back to
    # Vector{Argument} — building the heterogeneous TypedArgument tuple here
    # cost ~50 allocations per task for nothing.
    new_fargs = Argument[Argument(task_arg_ws[idx].pos, remote_args[idx]) for idx in 1:length(task_arg_ws)]
    new_spec = DTaskSpec(new_fargs, spec.options)
    new_spec.options.scope = our_scope
    new_spec.options.exec_scope = our_scope
    new_spec.options.writes = written_positions(task_arg_ws, deps_vec)
    if uniform
        new_spec.options.occupancy = Dict(Any=>0)
    end
    ctx = Sch.eager_context()
    @logstart ctx LogDatadepsExecute LogDatadepsExecuteId(task.uid) nothing
    enqueue!(queue.upper_queue, DTaskPair(new_spec, task))
    # N.B. `task_arg_ws`/`remote_args` are per-task scratch buffers, so the
    # logged payload snapshots them (only evaluated when logging is enabled)
    @logfinish ctx LogDatadepsExecute LogDatadepsExecuteId(task.uid) (;space=our_space, deps=logged_task_args(deps_vec, task_arg_ws), args=copy(remote_args))

    # Reclaim the syncdeps set when the (synchronous) submission above has
    # already consumed it — see `syncdeps_consumed` for the guard rationale.
    # Under deferred submission (launch_wait / batched hierarchical enqueue)
    # the guard fails and the set stays with the spec.
    let sd = new_spec.options.syncdeps
        if sd !== nothing && syncdeps_consumed(sd)
            new_spec.options.syncdeps = nothing
            return_syncdeps_set!(sd)
        end
    end

    # Update read/write tracking for arguments
    for (idx, arg_ws) in enumerate(task_arg_ws)
        (arg_ws.may_alias && arg_ws.inplace_move) || continue
        for di in arg_deps_range(arg_ws)
            dep = deps_vec[di]
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
    end

    # Hierarchical scheduling only: publish this task as the new authoritative
    # owner of each shared backing chunk it writes, so later cross-partition
    # consumers pull the up-to-date version from here.
    if ownership !== nothing
        _commit_ownership!(state, ownership, our_space, task, task_arg_ws)
    end

    write_num += 1

    DATADEPS_CURRENT_TASK[] = nothing

    return write_num
end
