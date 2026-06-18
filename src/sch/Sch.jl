module Sch

import Preferences: @load_preference
if @load_preference("distributed-package") == "DistributedNext"
    import DistributedNext: Future, ProcessExitedException, RemoteChannel, RemoteException, myid, remote_do, remotecall_fetch, remotecall_wait, workers
else
    import Distributed: Future, ProcessExitedException, RemoteChannel, RemoteException, myid, remote_do, remotecall_fetch, remotecall_wait, workers
end

import MemPool
import MemPool: DRef, StorageResource
import MemPool: poolset, storage_capacity, storage_utilized
import Random: randperm, randperm!
import Base: @invokelatest

import ..Dagger
import ..Dagger: Context, Processor, SchedulerOptions, Options, Thunk, WeakThunk, ThunkFuture, ThunkID, DTaskFailedException, Chunk, WeakChunk, OSProc, AnyScope, DefaultScope, InvalidScope, LockedObject, Argument, Signature
import ..Dagger: Sealed, SEALED, FutureNode, futures_push!, futures_seal!
import ..Dagger: DepNode, deps_push!, deps_seal!
import ..Dagger: order, dependents, noffspring, istask, inputs, unwrap_weak, unwrap_weak_checked, wrap_weak, affinity, tochunk, timespan_start, timespan_finish, procs, move, chunktype, default_enabled, processor, get_processors, get_parent, execute!, rmprocs!, task_processor, constrain, cputhreadtime, maybe_take_or_alloc!
import ..Dagger: @dagdebug, @safe_lock_spin1, @maybelog, @take_or_alloc!
import DataStructures: PriorityQueue

import ..Dagger: ReusableCache, ReusableLinkedList, ReusableDict
import ..Dagger: @reusable, @reusable_dict, @reusable_vector, @reusable_tasks, @reuse_scope, @reuse_defer_cleanup

import TimespanLogging

import TaskLocalValues: TaskLocalValue
import ScopedValues: @with

import ..Dagger: SignatureMetric, ProcessorMetric, WorkerMetric, TransferSizeMetric, TransferTimeMetric, TransferRateMetric
import ..Dagger: TASK_SIGNATURE, TASK_PROCESSOR, TASK_WORKER, TASK_TRANSFER_SIZE, TASK_TRANSFER_TIME
import ..Dagger: execute_metrics_spec, metrics_lookup_runtime, metrics_lookup_alloc, metrics_lookup_transfer_rate
import ..Dagger: extract_collected_metrics, apply_collected_metrics!
import ..Dagger.MetricsTracker as MT

include("util.jl")
include("fault-handler.jl")
include("dynamic.jl")

struct TaskResult
    pid::Int
    proc::Processor
    thunk_id::Int
    result::Any
    metadata::Union{NamedTuple,Nothing}
end

const AnyTaskResult = Union{RescheduleSignal, TaskResult}

"""
    ComputeState

The internal state-holding struct of the scheduler.

Fields:
- `uid::UInt64` - Unique identifier for this scheduler instance
- `initial_ready::Vector{Thunk}` - Bootstrap buffer: thunks that became ready during `start_state` wiring (before workers are online). Drained once in `scheduler_init` via `schedule_one!`; empty for the lifetime of the scheduler loop.
- `ctx::Context` - The scheduler context; stored here so `schedule_one!` and friends can access it without threading `ctx` through every call site.
- `sch_options::SchedulerOptions` - Scheduler-wide options; stored for the same reason as `ctx`.
- `running_count::Threads.Atomic{Int}` - Number of `Thunk`s that are running *or* have become ready but are not yet fired (replaces `running::Set{Thunk}`; per-thunk state lives on `thunk.running` and `thunk.running_on`). A thunk is credited here the moment it's placed into a `ready_out` buffer (`schedule_dependents!`/`reschedule_syncdeps!`/`start_state`), not when `fire_tasks!` actually dispatches it — this way, whenever a finishing thunk's decrement frees up a new ready dependent, the corresponding increment always happens first, so the counter never has a transient window where it looks like 0 while replacement work is still pending. Every code path that removes a thunk from a `ready_out` buffer without ultimately running it through `finish_task!` (the scheduling-inconsistency and no-processors-available/invalid-scope branches in `schedule_one!`) must release its credit with a matching decrement.
- `thunk_dict::LockedObject{Dict{Int, WeakThunk}}` - Maps from thunk IDs to a `Thunk`; has its own lock so it can be accessed independently of `state.lock`
- `node_order::Any` - Function that returns the order of a thunk
- `equiv_chunks::LockedObject{WeakKeyDict{DRef,Chunk}}` - Cache mapping from `DRef` to a `Chunk` which contains it; has its own lock
- `worker_time_pressure::LockedObject{Dict{Int,Dict{Processor,Threads.Atomic{UInt64}}}}` - Maps from worker ID to per-processor pressure counters (atomic leaves, own lock for membership changes; capture-the-ref invariant: hold the Atomic object across reserve→release)
- `worker_storage_pressure::LockedObject{Dict{Int,Dict{Union{StorageResource,Nothing},UInt64}}}` - Maps from worker ID to storage resource pressure (own lock)
- `worker_storage_capacity::LockedObject{Dict{Int,Dict{Union{StorageResource,Nothing},UInt64}}}` - Maps from worker ID to storage resource capacity (own lock)
- `worker_loadavg::LockedObject{Dict{Int,NTuple{3,Float64}}}` - Worker load average (own lock)
- `worker_chans::Dict{Int, Tuple{RemoteChannel,RemoteChannel}}` - Communication channels between the scheduler and each worker
- `running_pressure_refs::LockedObject{Dict{Int,Tuple{Threads.Atomic{UInt64},UInt64}}}` - Maps running thunk IDs to their captured pressure counter ref and reserved estimate; used for symmetric atomic release on completion; own lock for thread-safe access from concurrent `schedule_one!` calls
- `halt::Base.Event` - Event indicating that the scheduler is halting
- `lock::ReentrantLock` - Lock around operations which modify the state
- `thunks_to_delete::Set{Thunk}` - The list of `Thunk`s ready to be deleted upon completion.
- `chan::RemoteChannel{Channel{AnyTaskResult}}` - Channel for receiving completed thunks.

Note: `errored`, `valid`, `running`, and `running_on` per-thunk state now live directly on
`Thunk` fields (`thunk.errored`, `thunk.valid`, `thunk.running`, `thunk.running_on`).
Per-thunk futures use the Treiber list `thunk.futures_head` (`futures_push!`/`futures_seal!`).
Per-thunk dependency tracking uses `thunk.pending_deps` (dataflow counter) and
`thunk.dependents_head` (downstream Treiber list, `deps_push!`/`deps_seal!`).
`initial_ready` replaces `ready`; `schedule_one!` is called inline from the
completion handler (via `schedule_dependents!`) so there is no central ready queue.
"""
struct ComputeState
    uid::UInt64
    initial_ready::Vector{Thunk}
    ctx::Context
    sch_options::SchedulerOptions
    running_count::Threads.Atomic{Int}
    thunk_dict::LockedObject{Dict{Int, WeakThunk}}
    node_order::Any
    equiv_chunks::LockedObject{WeakKeyDict{DRef,Chunk}}
    worker_time_pressure::LockedObject{Dict{Int,Dict{Processor,Threads.Atomic{UInt64}}}}
    worker_storage_pressure::LockedObject{Dict{Int,Dict{Union{StorageResource,Nothing},UInt64}}}
    worker_storage_capacity::LockedObject{Dict{Int,Dict{Union{StorageResource,Nothing},UInt64}}}
    worker_loadavg::LockedObject{Dict{Int,NTuple{3,Float64}}}
    worker_chans::Dict{Int, Tuple{RemoteChannel,RemoteChannel}}
    running_pressure_refs::LockedObject{Dict{Int,Tuple{Threads.Atomic{UInt64},UInt64}}}
    halt::Base.Event
    # Set when the scheduler is being halted as a result of a cancellation
    # (`cancel!(...; halt_sch=true)`), so that teardown resolves any still-pending
    # futures with an `InterruptException` (reflecting the cancellation) rather
    # than the generic `SchedulingException` used for other scheduler exits.
    halt_cancelled::Threads.Atomic{Bool}
    lock::ReentrantLock
    thunks_to_delete::Set{Thunk}
    chan::RemoteChannel{Channel{AnyTaskResult}}
    # Strong references to eager-submitted thunks, held from submission until the
    # thunk reaches a terminal state and is removed via `task_delete!`. The
    # `thunk_dict` holds only *weak* refs (to allow fire-and-forget GC of results
    # once consumed), and an unfinished eager thunk's only other strong ref is the
    # MemPool datastore entry behind its result `DRef` — which is dropped the
    # instant the user releases the `DTask`. Without this set, dropping a `DTask`
    # before its thunk runs (e.g. intermediate layers in a wide DAG) lets GC
    # collect a thunk that still has pending dependents, whose `pending_deps`
    # then never decrements -> scheduler deadlock. Mutated only under `lock`.
    strong_thunks::Set{Thunk}
end

const UID_COUNTER = Threads.Atomic{UInt64}(1)

"""
    SCHEDULER_STATES

Process-local registry mapping a scheduler `uid` to its `ComputeState`. Used by
the in-process completion fast-path (`DoTaskSpec`) to reach the owning
`ComputeState` and run `handle_result!` locally instead of round-tripping the
result through `state.chan`. Only ever populated on the process that runs the
scheduler (which is the same process as any in-process worker thread), so a
lookup succeeds exactly when the local fast-path is applicable.
"""
const SCHEDULER_STATES = LockedObject(Dict{UInt64,ComputeState}())

function register_scheduler_state!(state::ComputeState)
    lock(SCHEDULER_STATES) do d
        d[state.uid] = state
    end
end
function deregister_scheduler_state!(state::ComputeState)
    lock(SCHEDULER_STATES) do d
        delete!(d, state.uid)
    end
end
function lookup_scheduler_state(uid::UInt64)
    lock(SCHEDULER_STATES) do d
        get(d, uid, nothing)
    end
end

function start_state(deps::Dict, node_order, chan, ctx::Context, sch_options::SchedulerOptions)
    state = ComputeState(Threads.atomic_add!(UID_COUNTER, UInt64(1)),
                         Vector{Thunk}(undef, 0),
                         ctx,
                         sch_options,
                         Threads.Atomic{Int}(0),
                         LockedObject(Dict{Int, WeakThunk}()),
                         node_order,
                         LockedObject(WeakKeyDict{DRef,Chunk}()),
                         LockedObject(Dict{Int,Dict{Processor,Threads.Atomic{UInt64}}}()),
                         LockedObject(Dict{Int,Dict{Union{StorageResource,Nothing},UInt64}}()),
                         LockedObject(Dict{Int,Dict{Union{StorageResource,Nothing},UInt64}}()),
                         LockedObject(Dict{Int,NTuple{3,Float64}}()),
                         Dict{Int, Tuple{RemoteChannel,RemoteChannel}}(),
                         LockedObject(Dict{Int,Tuple{Threads.Atomic{UInt64},UInt64}}()),
                         Base.Event(),
                         Threads.Atomic{Bool}(false),
                         ReentrantLock(),
                         Set{Thunk}(),
                         chan,
                         Set{Thunk}())

    # Initialize per-thunk dataflow counters and dependents lists.
    # Process in node_order so upstreams are seen before downstreams (allowing
    # deps_push! to always succeed since nothing has run yet).
    for k in sort(collect(keys(deps)), by=node_order)
        istask(k) || continue
        # Submission-guard: start pending_deps at 1 so no upstream can drive it
        # to zero while we are still wiring edges.
        @atomic k.pending_deps = 1
        if k.options !== nothing && k.options.syncdeps !== nothing
            for upstream in Dagger.syncdeps_iterator(k)
                @atomic k.pending_deps += 1
                deps_push!(upstream, k)   # always true at init (nothing finished yet)
            end
        end
        # Release the guard; if no upstreams remain, k is immediately ready.
        n = @atomic k.pending_deps -= 1   # n = new value after decrement
        if n == 0
            # Workers aren't online yet; buffer in initial_ready for scheduler_init.
            # Count it as running now (this runs single-threaded, before
            # scheduler_run's loop starts reading `running_count`, so there's
            # no race here) so `fire_tasks!` doesn't need to increment it later.
            Threads.atomic_add!(state.running_count, 1)
            push!(state.initial_ready, k)
        end
        @atomic k.valid = true
    end
    state
end

# Eager scheduling
include("eager.jl")

const WORKER_MONITOR_LOCK = Threads.ReentrantLock()
const WORKER_MONITOR_TASKS = Dict{Int,Task}()
const WORKER_MONITOR_CHANS = Dict{Int,Dict{UInt64,RemoteChannel}}()
function init_proc(state, p, log_sink)
    ctx = Context(Int[]; log_sink)
    @maybelog ctx timespan_start(ctx, :init_proc, (;uid=state.uid, worker=p.pid), nothing)
    # Initialize pressure and capacity
    gproc = OSProc(p.pid)
    lock(state.worker_time_pressure) do wtp
        wtp[p.pid] = Dict{Processor,Threads.Atomic{UInt64}}()
    end
    lock(state.worker_storage_pressure) do wsp
        wsp[p.pid] = Dict{Union{StorageResource,Nothing},UInt64}()
    end
    lock(state.worker_storage_capacity) do wsc
        wsc[p.pid] = Dict{Union{StorageResource,Nothing},UInt64}()
    end
    lock(state.worker_loadavg) do wla
        wla[p.pid] = (0.0, 0.0, 0.0)
    end
    if p.pid != 1
        lock(WORKER_MONITOR_LOCK) do
            wid = p.pid
            if !haskey(WORKER_MONITOR_TASKS, wid)
                t = Threads.@spawn begin
                    try
                        # Wait until this connection is terminated
                        remotecall_fetch(sleep, wid, typemax(UInt64))
                    catch err
                        # TODO: Report other kinds of errors? IOError, etc.
                        #if !(err isa ProcessExitedException)
                        #end
                    finally
                        lock(WORKER_MONITOR_LOCK) do
                            d = WORKER_MONITOR_CHANS[wid]
                            for uid in keys(d)
                                try
                                    put!(d[uid], TaskResult(wid, OSProc(wid), 0, ProcessExitedException(wid), nothing))
                                catch
                                end
                            end
                            empty!(d)
                            delete!(WORKER_MONITOR_CHANS, wid)
                            delete!(WORKER_MONITOR_TASKS, wid)
                        end
                    end
                end
                errormonitor_tracked("worker monitor $wid", t)
                WORKER_MONITOR_TASKS[wid] = t
                WORKER_MONITOR_CHANS[wid] = Dict{UInt64,RemoteChannel}()
            end
            WORKER_MONITOR_CHANS[wid][state.uid] = state.chan
        end
    end

    # Setup worker-to-scheduler channels
    inp_chan = RemoteChannel(p.pid)
    out_chan = RemoteChannel(p.pid)
    lock(state.lock) do
        state.worker_chans[p.pid] = (inp_chan, out_chan)
    end

    # Setup dynamic listener
    dynamic_listener!(ctx, state, p.pid)

    @maybelog ctx timespan_finish(ctx, :init_proc, (;uid=state.uid, worker=p.pid), nothing)
end
function _cleanup_proc(uid, log_sink)
    empty!(CHUNK_CACHE) # FIXME: Should be keyed on uid!
    states = proc_states(uid)
    MemPool.lock_read(states.lock) do
        for (proc, state) in states.dict
            istate = state.state
            istate.done[] = true
            notify(istate.reschedule)
        end
    end
    MemPool.lock(states.lock) do
        empty!(states.dict)
    end
end
function cleanup_proc(state, p, log_sink)
    ctx = Context(Int[]; log_sink)
    wid = p.pid
    @maybelog ctx timespan_start(ctx, :cleanup_proc, (;uid=state.uid, worker=wid), nothing)
    lock(WORKER_MONITOR_LOCK) do
        if haskey(WORKER_MONITOR_CHANS, wid)
            delete!(WORKER_MONITOR_CHANS[wid], state.uid)
        end
    end

    # If the worker process is still alive, clean it up
    if wid in workers()
        try
            remotecall_wait(_cleanup_proc, wid, state.uid, log_sink)
        catch ex
            # We allow ProcessExitedException's, which means that the worker
            # shutdown halfway through cleanup.
            if !(ex isa ProcessExitedException)
                rethrow()
            end
        end
    end

    @maybelog ctx timespan_finish(ctx, :cleanup_proc, (;uid=state.uid, worker=wid), nothing)
end

"Process-local condition variable (and lock) indicating task completion."
const TASK_SYNC = Threads.Condition()

"Process-local set of running task IDs."
const TASKS_RUNNING = Set{Int}()

"Process-local dictionary tracking per-processor total time utilization."
const PROCESSOR_TIME_UTILIZATION = Dict{UInt64,Dict{Processor,Ref{UInt64}}}()

# TODO: "Process-local count of actively-executing Dagger tasks per processor type."

"""
    MaxUtilization

Indicates a thunk that uses all processors of a given type.
"""
struct MaxUtilization end

function compute_dag(ctx::Context, d::Thunk, options=SchedulerOptions())
    if options.restore !== nothing
        try
            result = options.restore()
            if result isa Chunk
                return result
            elseif result !== nothing
                throw(ArgumentError("Invalid restore return type: $(typeof(result))"))
            end
        catch err
            report_catch_error(err, "Scheduler restore failed")
        end
    end

    chan = RemoteChannel(()->Channel{AnyTaskResult}(typemax(Int)))
    deps = dependents(d)
    ord = order(d, noffspring(deps))

    node_order = x -> -get(ord, x, 0)
    state = start_state(deps, node_order, chan, ctx, options)

    master = OSProc(myid())

    # Register so in-process completions can find this state
    register_scheduler_state!(state)

    @maybelog ctx timespan_start(ctx, :scheduler_init, (;uid=state.uid), master)
    try
        scheduler_init(ctx, state, d, options, deps)
    finally
        @maybelog ctx timespan_finish(ctx, :scheduler_init, (;uid=state.uid), master)
    end

    value, errored = try
        scheduler_run(ctx, state, d, options)
    finally
        # Always try to tear down the scheduler
        @maybelog ctx timespan_start(ctx, :scheduler_exit, (;uid=state.uid), master)
        try
            scheduler_exit(ctx, state, options)
        catch err
            @error "Error when tearing down scheduler" exception=(err,catch_backtrace())
        finally
            deregister_scheduler_state!(state)
            @maybelog ctx timespan_finish(ctx, :scheduler_exit, (;uid=state.uid), master)
        end
    end

    if errored
        throw(value)
    end
    return value
end

function scheduler_init(ctx, state::ComputeState, d::Thunk, options::SchedulerOptions, deps)
    # setup thunk_dict mappings
    lock(state.thunk_dict) do d
        for node in filter(istask, keys(deps))
            d[node.id] = WeakThunk(node)
            for dep in deps[node]
                d[dep.id] = WeakThunk(dep)
            end
        end
    end

    # Initialize workers
    procs = procs_to_use(ctx, options)
    @sync for p in procs
        Threads.@spawn begin
            try
                init_proc(state, p, ctx.log_sink)
            catch err
                @error "Error initializing worker $p" exception=(err,catch_backtrace())
                remove_dead_proc!(ctx, state, p, options)
            end
        end
    end

    # All workers are online; now schedule the bootstrap-ready thunks.
    # These were buffered in state.initial_ready during start_state's edge-wiring
    # because workers weren't initialized yet at that point.
    for thunk in state.initial_ready
        schedule_one!(state, thunk, procs)
    end
    empty!(state.initial_ready)

    # Halt scheduler on Julia exit
    atexit() do
        notify(state.halt)
    end

    # Listen for new workers
    Threads.@spawn begin
        try
            monitor_procs_changed!(ctx, state, options)
        catch err
            @error "Error assigning workers" exception=(err,catch_backtrace())
        end
    end
end

"""
    handle_result!(ctx, state, pid, proc, thunk_id, res, metadata)

Handle one completed task end-to-end: release its reserved pressure, fold its
cost-model metadata back in, store its result (single-finisher CAS), run its
checkpoint, finish it (`finish_task!`), and schedule any newly-ready dependents
**outside** `state.lock` (via `schedule_ready!`).

This is the runner-callable finish entry point. It is invoked both:
- from `scheduler_run` for results that travel over `state.chan` (cross-worker
  results, cancellation signals), and
- directly on the producing worker thread for in-process results (the local
  fast-path in `DoTaskSpec`), so N worker threads can finish N tasks
  concurrently.

The only serialized region is the brief `state.lock` critical section; the
dependent placement runs lock-free and in parallel. `ProcessExitedException`
(worker death) is *not* handled here — it arrives only over the channel and is
handled by `scheduler_run`.
"""
function handle_result!(ctx, state::ComputeState, pid, proc, thunk_id, res, metadata)
    ready = Thunk[]
    proceed = lock(state.lock) do
        thunk_failed = false
        if res isa Exception
            # A non-fault task error. In allow-errors mode we propagate it to the
            # task's dependents/futures; otherwise we abort the (batch) scheduler.
            # Note: the local fast-path never routes exceptions here (it forwards
            # them over the channel), so this throw always unwinds on the master's
            # scheduler_run task, preserving batch-mode error semantics.
            if something(state.sch_options.allow_errors, false)
                thunk_failed = true
            else
                throw(res)
            end
        end
        node = lock(state.thunk_dict) do d
            haskey(d, thunk_id) ? unwrap_weak(d[thunk_id]) : nothing
        end
        if node === nothing
            # A result arrived for a task that is no longer tracked.
            # Two known-safe reasons:
            #   1. Cancellation posted a duplicate result after finish_task! ran.
            #   2. The user dropped their DTask reference (fire-and-forget), the
            #      Thunk was GC'd, and the result arrived just after.
            # Ignore it rather than crashing the scheduler.
            @dagdebug thunk_id :take "Ignoring result for untracked or GC'd task"
            return false
        end
        # Release the reserved pressure via the captured counter ref
        # (capture-the-ref invariant: same Atomic object as was atomic_add!'d
        # at schedule time, stable even if the worker was removed between
        # reserve and release).
        entry = lock(state.running_pressure_refs) do rpr
            pop!(rpr, thunk_id, nothing)
        end
        if entry !== nothing
            counter_ref, est = entry
            Threads.atomic_sub!(counter_ref, est)
        end
        if metadata !== nothing
            #to_storage = fetch(node.options.storage)
            #state.worker_storage_pressure[pid][to_storage] = metadata.storage_pressure
            #state.worker_storage_capacity[pid][to_storage] = metadata.storage_capacity
            #state.worker_loadavg[pid] = metadata.loadavg
            # Fold the task's collected metrics into the global MetricsTracker
            # cache (its own synchronization makes this safe under state.lock and
            # from concurrent in-process finishers). The scheduler's cost model
            # (has_capacity/estimate_task_costs!) reads these back via snapshots.
            if metadata.metrics !== nothing
                apply_collected_metrics!(MT.global_metrics_cache(), thunk_id, metadata.metrics)
            end
        end
        if res isa Chunk
            lock(state.equiv_chunks) do ec
                if !haskey(ec, res.handle::DRef)
                    ec[res.handle::DRef] = res
                end
            end
        end
        won = store_result!(state, node, res; error=thunk_failed)
        if !won
            # The single-finisher CAS lost: another path (cancellation,
            # fault recovery) already stored a result. Discard this one.
            @dagdebug thunk_id :take "Ignoring duplicate result for already-finished task"
            return false
        end
        if node.options !== nothing && node.options.checkpoint !== nothing
            try
                @invokelatest node.options.checkpoint(node, res)
            catch err
                report_catch_error(err, "Thunk checkpoint failed")
            end
        end

        @maybelog ctx timespan_start(ctx, :finish, (;uid=state.uid, thunk_id), (;thunk_id, result=res))
        finish_task!(ctx, state, node, thunk_failed, ready)
        @maybelog ctx timespan_finish(ctx, :finish, (;uid=state.uid, thunk_id), (;thunk_id, result=res))
        return true
    end
    proceed || return

    # Schedule newly-ready dependents with NO state.lock held — concurrent
    # finishers place their freed dependents in parallel.
    schedule_ready!(state, ready)

    # Termination wake (B.5): in batch `compute_dag` mode the master's
    # scheduler_run blocks on `take!(state.chan)`. When the final task finishes
    # in-process (local fast-path) and drives `running_count` to 0, no further
    # channel traffic would arrive, so nudge the master to re-check its loop
    # condition and exit. (In eager mode the long-lived eager root thunk keeps
    # running_count >= 1, so this never fires spuriously.) `schedule_ready!`
    # above has already run synchronously (inline for the common case; see
    # `INLINE_SCHEDULE_FANOUT_THRESHOLD`), and any dependent it discovered was
    # already credited to `running_count` before this thunk's own decrement
    # in `finish_task!`, so a read of 0 here is never a false negative.
    if state.running_count[] == 0
        try
            put!(state.chan, RescheduleSignal())
        catch
            # Channel may already be closed during teardown; ignore.
        end
    end
    return
end

function scheduler_run(ctx, state::ComputeState, d::Thunk, options::SchedulerOptions)
    @dagdebug nothing :global "Initializing scheduler" uid=state.uid

    safepoint(state)

    # Loop while any thunks are still running. Scheduling now happens
    # inline from the completion handler (finish_task! → schedule_dependents! →
    # schedule_one!), so there is no central schedule! call here. The bootstrap-
    # ready set is drained in scheduler_init before this loop starts.
    while state.running_count[] > 0
        check_workers_available(ctx, options)

        @maybelog ctx timespan_start(ctx, :take, (;uid=state.uid), nothing)
        @dagdebug nothing :take "Waiting for results"
        tresult = take!(state.chan) # get result of completed thunk
        @maybelog ctx timespan_finish(ctx, :take, (;uid=state.uid), nothing)
        if tresult isa RescheduleSignal
            continue
        end

        tresult::TaskResult
        pid = tresult.pid
        proc = tresult.proc
        thunk_id = tresult.thunk_id
        res = tresult.result

        @dagdebug thunk_id :take "Got finished task"
        safepoint(state)

        # Fault path: a worker death (ProcessExitedException) is reported only
        # over the channel — local, in-process completions never produce it — so
        # it is handled here on the master, not in handle_result!.
        if res isa Exception && unwrap_nested_exception(res) isa ProcessExitedException
            gproc = OSProc(pid)
            fault_ready = Thunk[]
            lock(state.lock) do
                @warn "Worker $(pid) died, rescheduling work"

                # Remove dead worker from procs list
                @maybelog ctx timespan_start(ctx, :remove_procs, (;uid=state.uid, worker=pid), nothing)
                remove_dead_proc!(ctx, state, gproc, options)
                @maybelog ctx timespan_finish(ctx, :remove_procs, (;uid=state.uid, worker=pid), nothing)

                @maybelog ctx timespan_start(ctx, :handle_fault, (;uid=state.uid, worker=pid), nothing)
                handle_fault(ctx, state, gproc, fault_ready)
                @maybelog ctx timespan_finish(ctx, :handle_fault, (;uid=state.uid, worker=pid), nothing)
            end
            # Reschedule the dead worker's orphaned thunks outside state.lock.
            schedule_ready!(state, fault_ready)
            tresult = nothing
            res = nothing
            safepoint(state)
            continue
        end

        # Normal completion (or a non-fault task error). Handle it end-to-end,
        # including scheduling newly-ready dependents outside state.lock.
        handle_result!(ctx, state, pid, proc, thunk_id, res, tresult.metadata)

        # Allow data to be GC'd
        tresult = nothing
        res = nothing

        safepoint(state)
    end

    # Final value is ready
    value = load_result(state, d)
    errored = (@atomic d.errored)
    if !errored
        if options.checkpoint !== nothing
            try
                options.checkpoint(value)
            catch err
                report_catch_error(err, "Scheduler checkpoint failed")
            end
        end
    end
    return value, errored
end
function scheduler_exit(ctx, state::ComputeState, options::SchedulerOptions)
    @dagdebug nothing :global "Tearing down scheduler" uid=state.uid

    @sync for p in procs_to_use(ctx, options)
        Threads.@spawn cleanup_proc(state, p, ctx.log_sink)
    end

    lock(state.lock) do
        close(state.chan)
        notify(state.halt)

        # Notify any waiting tasks. If the scheduler is halting because of a
        # cancellation, surface that as an `InterruptException` (the result a
        # caller expects from a cancelled task) rather than a generic
        # `SchedulingException`.
        teardown_ex = state.halt_cancelled[] ? InterruptException() :
                                               SchedulingException("Scheduler exited")
        lock(state.thunk_dict) do d
            for (_, wt) in d
                t = unwrap_weak(wt)
                t === nothing && continue
                # Seal this thunk's futures list (no-op if already sealed by finish_task!).
                # Any futures remaining in the captured list were never fulfilled normally.
                head = futures_seal!(t)
                head === nothing && continue
                node = head
                while node !== nothing
                    put!(node.future, teardown_ex; error=true)
                    node = @atomic node.next
                end
            end
        end
    end

    # Let the context procs handler clean itself up
    lock(ctx.proc_notify) do
        notify(ctx.proc_notify)
    end

    @dagdebug nothing :global "Tore down scheduler" uid=state.uid
end

function procs_to_use(ctx::Context, options::SchedulerOptions)
    return if options.single !== nothing
        @assert options.single in vcat(1, workers()) "Sch option `single` must specify an active worker ID."
        OSProc[OSProc(options.single)]
    else
        procs(ctx)
    end
end

check_workers_available(ctx, options) = @assert !isempty(procs_to_use(ctx, options)) "No remaining workers available."

struct SchedulingException <: Exception
    reason::String
end
function Base.show(io::IO, se::SchedulingException)
    print(io, "SchedulingException ($(se.reason))")
end

const CHUNK_CACHE = Dict{Chunk,Dict{Processor,Any}}()

struct ScheduleTaskLocation
    gproc::OSProc
    proc::Processor
end
struct ScheduleTaskSpec
    task::Thunk
    scope::Dagger.AbstractScope
    est_time_util::UInt64
    est_alloc_util::UInt64
    est_occupancy::UInt32
end
"""
    INLINE_SCHEDULE_FANOUT_THRESHOLD

Maximum number of newly-ready dependents to schedule inline (on the calling
thread) in `schedule_ready!`. When more than this many dependents become ready
at once, additional ones are dispatched via `Threads.@spawn` so that a single
wide fan-out does not serialize all of its placements on one thread.
"""
const INLINE_SCHEDULE_FANOUT_THRESHOLD = 8

"""
    schedule_ready!(state, ready[, procs])

Drain a buffer of newly-ready thunks (collected under `state.lock` by
`schedule_dependents!` / `reschedule_syncdeps!`) by placing each onto a
processor via `schedule_one!`. **The caller must NOT hold `state.lock`** —
placement (cost estimation, optimistic pressure
reservation, queue insertion) runs lock-free and concurrently across threads.

The first `INLINE_SCHEDULE_FANOUT_THRESHOLD` thunks are scheduled inline on the
calling thread; any beyond that are dispatched via `Threads.@spawn` so a single
wide fan-out doesn't serialize all placements on one thread. `ready` is emptied
on return.
"""
function schedule_ready!(state, ready::Vector{Thunk}, procs=procs_to_use(state.ctx, state.sch_options))
    n = length(ready)
    n == 0 && return
    @inbounds for i in 1:n
        t = ready[i]
        if i <= INLINE_SCHEDULE_FANOUT_THRESHOLD
            schedule_one!(state, t, procs)
        else
            tt = t
            Threads.@spawn schedule_one!(state, tt, procs)
        end
    end
    empty!(ready)
    return
end

"""
    schedule_one!(state, task[, procs])

Schedule a single `task` onto the best available processor. This is the
per-thunk equivalent of the former central `schedule!` loop: it computes
scope, estimates costs, performs an optimistic pressure reservation
(`atomic_add!` + recheck), and calls `fire_tasks!` to push the `TaskSpec`
into the chosen processor's queue.

Called from `schedule_ready!` (which drains the ready buffers collected by
`schedule_dependents!` / `reschedule_syncdeps!`) and from `scheduler_init` for
the bootstrap-ready set. It acquires `state.lock` internally for its brief
Phase 1 / Phase 3 critical sections; **callers must not already hold
`state.lock`**, so that Phase 2 runs genuinely lock-free and
concurrently across threads.
"""
@reuse_scope function schedule_one!(state, task, procs=procs_to_use(state.ctx, state.sch_options))
    # Phase 1 — brief state.lock hold: resolve inputs and bail early if already done.
    # The expensive work (cost estimation, processor selection, pressure reservation)
    # is deferred to Phase 2 so that concurrent submissions don't serialize through it.
    local ctx, procs_filt
    local p1_resolved = false

    lock(state.lock) do
        safepoint(state)

        # Remove processors that aren't yet initialized
        procs_filt = filter(p -> haskey(state.worker_chans, Dagger.root_worker_id(p)), procs)
        if isempty(procs_filt)
            p1_resolved = true
            return
        end

        ctx = state.ctx
        @dagdebug task :schedule "Scheduling task"
        @maybelog ctx timespan_start(ctx, :schedule, (;uid=state.uid, thunk_id=task.id), (;thunk_id=task.id))

        if has_result(state, task)
            if (@atomic task.errored)
                # An error was eagerly propagated to this task
                @dagdebug task :schedule "Task received upstream error, finishing"
                set_failed!(state, task)
            else
                # This shouldn't have happened
                @dagdebug task :schedule "Scheduling inconsistency: Task being scheduled is already cached!"
                iob = IOBuffer()
                println(iob, "Scheduling inconsistency: Task being scheduled is already cached!")
                println(iob, "  Task: $(task.id)")
                println(iob, "  Cache Entry: $(typeof(something(task.cache_ref)))")
                ex = SchedulingException(String(take!(iob)))
                store_result!(state, task, ex; error=true)
            end
            # `task` was already credited to `running_count` when it was placed
            # into a `ready_out` buffer, but it neither reaches `fire_tasks!`
            # nor `finish_task!` on this path — release that credit now so the
            # counter doesn't leak (which would otherwise hang the scheduler).
            Threads.atomic_sub!(state.running_count, 1)
            @maybelog ctx timespan_finish(ctx, :schedule, (;uid=state.uid, thunk_id=task.id), (;thunk_id=task.id))
            p1_resolved = true
            return
        end

        # Resolve Thunk-typed inputs to their cached Chunk results.
        # After this call all values in task.inputs are Chunks or plain values.
        collect_task_inputs!(state, task)
    end

    p1_resolved && return

    # Phase 2 — NO state.lock held: expensive scheduling work.
    # collect_task_inputs! above ensures all task inputs are resolved, so:
    # - signature() is safe (throws if any input is still a Thunk)
    # - scope calculation only needs task.inputs (now Chunks/values), not state.cache
    # - estimate_task_costs! / has_capacity acquire their own fine-grained locks

    # Calculate signature
    sig = signature(state, task)

    # Merge scheduler options and populate defaults
    options = task.options
    Dagger.options_merge!(options, state.sch_options)
    Dagger.populate_defaults!(options, sig)

    # Calculate scope.  Errors are stored in `scope_ex` and applied in Phase 3.
    local scope
    local scope_ex = nothing
    if options.exec_scope !== nothing
        # Bypass scope calculation if it's been done for us already
        scope = options.exec_scope
    else
        scope = constrain(@something(options.compute_scope, options.scope, DefaultScope()),
                          @something(options.result_scope, AnyScope()))
        if scope isa InvalidScope
            scope_ex = SchedulingException("compute_scope and result_scope are not compatible: $(scope.x), $(scope.y)")
        else
            for arg in task.inputs
                scope_ex === nothing || break
                value = unwrap_weak_checked(Dagger.value(arg))
                # After collect_task_inputs!, Thunk inputs are resolved to Chunks.
                @assert !istask(value)
                chunk = if value isa Chunk
                    value
                else
                    nothing
                end
                chunk isa Chunk || continue
                scope = constrain(scope, chunk.scope)
                if scope isa InvalidScope
                    scope_ex = SchedulingException("Current scope and argument Chunk scope are not compatible: $(scope.x), $(scope.y)")
                end
            end
        end
    end

    if scope_ex !== nothing
        # scope is invalid — fail the task under state.lock (set_failed! requires it)
        lock(state.lock) do
            set_failed!(state, task; ex=scope_ex)
            # Release the `running_count` credit `task` received when it
            # entered `ready_out` (see comment at the other `set_failed!`
            # call sites in this function for why this is necessary).
            Threads.atomic_sub!(state.running_count, 1)
            @maybelog ctx timespan_finish(ctx, :schedule, (;uid=state.uid, thunk_id=task.id), (;thunk_id=task.id))
        end
        return
    end

    input_procs = @reusable_vector :schedule_one!_input_procs Processor OSProc() 32
    input_procs_cleanup = @reuse_defer_cleanup empty!(input_procs)
    compat = Dagger.compatible_processors(scope, procs_filt)
    for proc in compat
        if !(proc in input_procs)
            push!(input_procs, proc)
        end
    end

    sorted_procs = @reusable_vector :schedule_one!_sorted_procs Processor OSProc() 32
    sorted_procs_cleanup = @reuse_defer_cleanup empty!(sorted_procs)
    resize!(sorted_procs, length(input_procs))
    costs = @reusable_dict :schedule_one!_costs Processor Float64 OSProc() 0.0 32
    costs_cleanup = @reuse_defer_cleanup empty!(costs)
    estimate_task_costs!(sorted_procs, costs, state, input_procs, task; sig)
    input_procs_cleanup()

    # Select the best available processor and reserve time pressure optimistically.
    # These operations use their own fine-grained locks — no state.lock needed.
    scheduled = false
    local best_loc, best_spec
    for proc in sorted_procs
        gproc = get_parent(proc)
        can_use, scope = can_use_proc(state, task, gproc, proc, options, scope)
        if can_use
            has_cap, est_time_util, est_alloc_util, est_occupancy =
                has_capacity(state, proc, gproc.pid, options.time_util, options.alloc_util, options.occupancy, sig)
            if has_cap
                # Optimistic reservation: capture-the-ref, atomic_add!
                counter_ref = lock(state.worker_time_pressure) do wtp
                    proc_map = get!(wtp, gproc.pid) do
                        Dict{Processor,Threads.Atomic{UInt64}}()
                    end
                    get!(proc_map, proc) do
                        Threads.Atomic{UInt64}(UInt64(0))
                    end
                end
                Threads.atomic_add!(counter_ref, est_time_util)
                lock(state.running_pressure_refs) do rpr
                    rpr[task.id] = (counter_ref, est_time_util)
                end
                @dagdebug task :schedule "Scheduling to $gproc -> $proc (cost: $(costs[proc]), pressure: $(counter_ref[]))"
                best_loc = ScheduleTaskLocation(gproc, proc)
                best_spec = ScheduleTaskSpec(task, scope, est_time_util, est_alloc_util, est_occupancy)
                scheduled = true
                break
            end
        end
    end
    sorted_procs_cleanup()
    costs_cleanup()

    # Phase 3 — brief state.lock hold: fire or fail the task.
    # `restore_ready` only becomes non-empty if a task completes synchronously
    # via its `restore` callback inside fire_tasks!; those dependents are
    # scheduled after the lock is released.
    restore_ready = Thunk[]
    lock(state.lock) do
        if scheduled
            fire_tasks!(ctx, best_loc, [best_spec], state, restore_ready)
        else
            ex = SchedulingException("No processors available, try widening scope")
            set_failed!(state, task; ex)
            @dagdebug task :schedule "No processors available, skipping"
            # `task` was already credited to `running_count` when it entered
            # `ready_out`, but `set_failed!` here never routes it through
            # `finish_task!` — release that credit now to avoid leaking it.
            Threads.atomic_sub!(state.running_count, 1)
        end
        @maybelog ctx timespan_finish(ctx, :schedule, (;uid=state.uid, thunk_id=task.id), (;thunk_id=task.id))
    end
    schedule_ready!(state, restore_ready, procs)
end

"""
Monitors for workers being added/removed to/from `ctx`, sets up or tears down
per-worker state, and notifies the scheduler so that work can be reassigned.
"""
function monitor_procs_changed!(ctx, state, options)
    # Load current set of procs
    old_ps = procs_to_use(ctx, options)

    while !state.halt.set
        # Wait for the notification that procs have changed
        lock(ctx.proc_notify) do
            wait(ctx.proc_notify)
        end

        @maybelog ctx timespan_start(ctx, :assign_procs, (;uid=state.uid), nothing)

        # Load new set of procs
        new_ps = procs_to_use(ctx, options)

        # Initialize new procs
        diffps = setdiff(new_ps, old_ps)
        for p in diffps
            init_proc(state, p, ctx.log_sink)

            # Force reschedule
            put!(state.chan, RescheduleSignal())
        end

        # Cleanup removed procs
        diffps = setdiff(old_ps, new_ps)
        for p in diffps
            cleanup_proc(state, p, ctx.log_sink)
        end

        @maybelog ctx timespan_finish(ctx, :assign_procs, (;uid=state.uid), nothing)
        old_ps = new_ps
    end
end

function remove_dead_proc!(ctx, state, proc, options)
    @assert options.single !== proc.pid "Single worker failed, cannot continue."
    rmprocs!(ctx, [proc])
    # COW-style membership removal: lock each map and delete the worker's entry.
    # Any in-flight tasks that captured a counter ref from this worker will still
    # release via atomic_sub! on the (now-orphaned) Atomic object — harmless.
    lock(state.worker_time_pressure) do wtp; delete!(wtp, proc.pid); end
    lock(state.worker_storage_pressure) do wsp; delete!(wsp, proc.pid); end
    lock(state.worker_storage_capacity) do wsc; delete!(wsc, proc.pid); end
    lock(state.worker_loadavg) do wla; delete!(wla, proc.pid); end
    delete!(state.worker_chans, proc.pid)
end

function finish_task!(ctx, state, node, thunk_failed, ready::Vector{Thunk})
    @dagdebug node :finish "Finishing with $(thunk_failed ? "error" : "result")"
    @atomic node.running = false
    node.running_on = nothing
    if thunk_failed
        # The result (error) was already stored by the scheduler loop via
        # store_result! before this function was called.  We cannot delegate to
        # set_failed! here because set_failed! guards against double-processing
        # with `has_result(state, thunk) && return`, which would be true at this
        # point and would skip fill_registered_futures!, leaving DTask futures
        # permanently unresolved.  Handle the three steps directly instead.
        fill_registered_futures!(state, node, true)
        node.sch_accessible = false
        delete_unused_task!(state, node)
        schedule_dependents!(state, node, true, ready)
    else
        # Success path: seal dependents (collecting newly-ready ones into
        # `ready` for the caller to schedule outside state.lock), fulfill
        # futures, mark no longer needed.
        schedule_dependents!(state, node, false, ready)
        fill_registered_futures!(state, node, false)
        node.sch_accessible = false
        delete_unused_task!(state, node)
    end
    # Decrement `running_count` for `node` *last*, only after
    # `schedule_dependents!` has already credited any newly-freed dependents
    # (see the comments there). Since increments for freed dependents always
    # happen first (while `node` itself is still counted), this final
    # decrement is the only operation that can bring the counter to its true
    # new value, and it does so in one atomic step — no lock-free reader can
    # ever observe an intermediate (spuriously low) count.
    Threads.atomic_sub!(state.running_count, 1)
    #evict_all_chunks!(ctx, to_evict)
end

function delete_unused_task!(state, thunk)
    if has_result(state, thunk) && !thunk.eager_accessible && !thunk.sch_accessible
        # Will not be accessed further, delete all cached data
        task_delete!(state, thunk)
        return true
    else
        return false
    end
end
function task_delete!(state, thunk)
    clear_result!(state, thunk)
    @atomic thunk.valid = false
    @atomic thunk.errored = false
    lock(state.thunk_dict) do d
        delete!(d, thunk.id)
    end
    # Release the scheduler's strong reference (see `ComputeState.strong_thunks`).
    delete!(state.strong_thunks, thunk)
end

function evict_all_chunks!(ctx, options, to_evict)
    if !isempty(to_evict)
        @sync for w in map(p->p.pid, procs_to_use(ctx, options))
            Threads.@spawn remote_do(evict_chunks!, w, ctx.log_sink, to_evict)
        end
    end
end
function evict_chunks!(log_sink, chunks::Set{Chunk})
    # Need worker id or else Context might use Processors which user does not want us to use.
    # In particular workers which have not yet run using Dagger will cause the call below to throw an exception
    ctx = Context([myid()]; log_sink)
    for chunk in chunks
        lock(TASK_SYNC) do
            @maybelog ctx timespan_start(ctx, :evict, (;worker=myid()), (;data=chunk))
            haskey(CHUNK_CACHE, chunk) && delete!(CHUNK_CACHE, chunk)
            @maybelog ctx timespan_finish(ctx, :evict, (;worker=myid()), (;data=chunk))
        end
    end
    nothing
end

"A serializable description of a `Thunk` to be executed."
struct TaskSpec
    thunk_id::Int
    est_time_util::UInt64
    est_alloc_util::UInt64
    est_occupancy::UInt32
    scope::Dagger.AbstractScope
    Tf::Type
    data::Vector{Argument}
    options::Options
    ctx_vars::NamedTuple
    sch_handle::SchedulerHandle
    sch_uid::UInt64
end
Base.hash(task::TaskSpec, h::UInt) = hash(task.thunk_id, hash(TaskSpec, h))

@reuse_scope function fire_tasks!(ctx, task_loc::ScheduleTaskLocation, task_specs::Vector{ScheduleTaskSpec}, state, ready::Vector{Thunk})
    gproc, proc = task_loc.gproc, task_loc.proc
    to_send = @reusable_vector :fire_tasks!_to_send Union{TaskSpec,Nothing} nothing 1024
    to_send_cleanup = @reuse_defer_cleanup empty!(to_send)
    for task_spec in task_specs
        thunk = task_spec.task
        @atomic thunk.running = true
        thunk.running_on = gproc
        # N.B. `running_count` was already incremented for `thunk` when it was
        # placed into a `ready_out` buffer (see `schedule_dependents!` /
        # `reschedule_syncdeps!` / `start_state`) — not here — so that the
        # increment always happens-before the decrement of whatever finishing
        # thunk made `thunk` ready, closing a race where `running_count` could
        # transiently read as 0 while `thunk` was still awaiting firing.
        @assert !has_result(state, thunk)
        if thunk.options.restore !== nothing
            try
                result = @invokelatest thunk.options.restore(thunk)
                if result isa Chunk
                    store_result!(state, thunk, result)
                    # Restore completed the task immediately; collect any newly-
                    # ready dependents into `ready` for the caller to schedule
                    # outside state.lock.
                    finish_task!(ctx, state, thunk, false, ready)
                    # The task finished via restore — release the pressure
                    # that was reserved in schedule_one! without a TaskResult
                    # ever arriving from the worker.
                    entry = lock(state.running_pressure_refs) do rpr
                        pop!(rpr, thunk.id, nothing)
                    end
                    if entry !== nothing
                        counter_ref, est = entry
                        Threads.atomic_sub!(counter_ref, est)
                    end
                    continue
                elseif result !== nothing
                    throw(ArgumentError("Invalid restore return type: $(typeof(result))"))
                end
            catch err
                report_catch_error(err, "Thunk restore failed")
            end
        end

        options = copy(thunk.options)
        
        if !isnothing(options.syncdeps)
            # Safely extract IDs, filtering out any that are `nothing`
            valid_ids = [Dagger.unwrap_weak_checked(s.thunk).id for s in options.syncdeps]
            
            # Rebuild the Set with only the valid ThunkSyncdeps
            options.syncdeps = Set(map(id -> Dagger.ThunkSyncdep(Dagger.ThunkID(id)), valid_ids))
        end

        # Unwrap any weak arguments
        args = map(copy, thunk.inputs)
        for arg in args
            # TODO: Only for non-delayed: @assert Dagger.isweak(Dagger.value(arg)) "Non-weak argument: $(arg)"
            arg.value = unwrap_weak_checked(Dagger.value(arg))
        end
        Tf = chunktype(first(args))

        @assert (options.single === nothing) || (gproc.pid == options.single)
        # TODO: Set `sch_handle.tid.ref` to the right `DRef`
        sch_handle = SchedulerHandle(ThunkID(thunk.id, nothing), state.worker_chans[gproc.pid]...)

        # TODO: De-dup common fields (log_sink, uid, etc.)
        push!(to_send, TaskSpec(
            thunk.id,
            task_spec.est_time_util, task_spec.est_alloc_util, task_spec.est_occupancy,
            task_spec.scope, Tf, args, options,
            (log_sink=ctx.log_sink, profile=ctx.profile),
            sch_handle, state.uid))
    end

    if !isempty(to_send)
        if Dagger.root_worker_id(gproc) == myid()
            @reusable_tasks :fire_tasks!_task_cache 32 _->nothing "fire_tasks!" FireTaskSpec(proc, state.chan, to_send)
        else
            # N.B. We don't batch these because we might get a deserialization
            # error due to something not being defined on the worker, and then we don't
            # know which task failed.
            for task_spec in to_send
                @reusable_tasks :fire_tasks!_task_cache 32 _->nothing "fire_tasks!" FireTaskSpec(proc, state.chan, task_spec)
            end
        end
    end
    to_send_cleanup()
end

struct FireTaskSpec
    init_proc::Processor
    return_chan::RemoteChannel
    tasks::Vector{TaskSpec}
end
FireTaskSpec(init_proc::Processor, return_chan::RemoteChannel, task::TaskSpec) =
    FireTaskSpec(init_proc, return_chan, [task])
function (ets::FireTaskSpec)()
    tasks = ets.tasks
    first_task = first(tasks)
    ctx_vars = first_task.ctx_vars
    ctx = Context(Processor[]; log_sink=ctx_vars.log_sink, profile=ctx_vars.profile)
    uid = first_task.sch_uid

    proc = ets.init_proc
    chan = ets.return_chan
    pid = Dagger.root_worker_id(proc)

    @maybelog ctx timespan_start(ctx, :fire, (;uid, worker=pid), nothing)
    try
        if pid == myid()
            do_tasks(proc, chan, tasks)
        else
            remotecall_wait(do_tasks, pid, proc, chan, tasks);
        end
    catch err
        bt = catch_backtrace()
        # FIXME: Catch the correct task ID
        thunk_id = first_task.thunk_id
        if isopen(chan)
            put!(chan, TaskResult(pid, proc, thunk_id, CapturedException(err, bt), nothing))
        end
    finally
        @maybelog ctx timespan_finish(ctx, :fire, (;uid, worker=pid), nothing)
    end
    return
end

@static if VERSION >= v"1.9"
const Doorbell = Base.Event
else
# We need a sticky, resetable signal
mutable struct Doorbell
    waiter::Union{Task,Nothing}
    @atomic sleeping::Int
    Doorbell() = new(nothing, 0)
end
function Base.wait(db::Doorbell)
    db.waiter = current_task()
    while true
        _, succ = @atomicreplace db.sleeping 0 => 1
        if succ
            # No messages, wait for someone to wake us
            wait()
        end
        _, succ = @atomicreplace db.sleeping 2 => 0
        if succ
            # We had a notification
            return
        end
    end
end
function Base.notify(db::Doorbell)
    while true
        if (@atomic db.sleeping) == 2
            # Doorbell already rung
            return
        end

        _, succ = @atomicreplace db.sleeping 0 => 2
        if succ
            # Task was definitely busy, we're done
            return
        end

        _, succ = @atomicreplace db.sleeping 1 => 2
        if succ
            # Task was sleeping, wake it and wait for it to awaken
            waiter = db.waiter
            @assert waiter !== nothing
            waiter::Task
            schedule(waiter)
            while true
                sleep_value = @atomic db.sleeping
                if sleep_value == 0 || sleep_value == 2
                    return
                end
                #if waiter._state === Base.task_state_runnable && t.queue === nothing
                #    schedule(waiter)
                #else
                    yield()
                #end
            end
        end
    end
end
end

struct ProcessorInternalState
    ctx::Context
    proc::Processor
    return_queue::RemoteChannel
    queue::LockedObject{PriorityQueue{TaskSpec, UInt32, Base.Order.ForwardOrdering}}
    reschedule::Doorbell
    tasks::Dict{Int,Task}
    task_specs::Dict{Int,TaskSpec}
    proc_occupancy::Base.RefValue{UInt32}
    time_pressure::Base.RefValue{UInt64}
    cancelled::Set{Int}
    cancel_tokens::Dict{Int,Dagger.CancelToken}
    done::Base.RefValue{Bool}
end
struct ProcessorState
    state::ProcessorInternalState
    runner::Task
end

const PROCESSOR_TASK_STATE_LOCK = MemPool.ReadWriteLock()
struct ProcessorStateDict
    lock::MemPool.ReadWriteLock
    dict::Dict{Processor,ProcessorState}
    # Thunk IDs cancelled by the fallback before do_tasks ran.
    # do_tasks and the proc runner check this set and skip the task
    # (without incrementing proc_occupancy) if the ID is present.
    pre_cancelled::Set{Int}
    pre_cancelled_lock::ReentrantLock
    ProcessorStateDict() = new(MemPool.ReadWriteLock(), Dict{Processor,ProcessorState}(),
                               Set{Int}(), ReentrantLock())
end
const PROCESSOR_TASK_STATE = Dict{UInt64,ProcessorStateDict}()

function proc_states(uid::UInt64=Dagger.get_tls().sch_uid)
    states = MemPool.lock_read(PROCESSOR_TASK_STATE_LOCK) do
        if haskey(PROCESSOR_TASK_STATE, uid)
            return PROCESSOR_TASK_STATE[uid]
        end
        return nothing
    end
    if states === nothing
        states = MemPool.lock(PROCESSOR_TASK_STATE_LOCK) do
            dict = ProcessorStateDict()
            PROCESSOR_TASK_STATE[uid] = dict
            return dict
        end
    end
    return states
end
function proc_states_values(uid::UInt64=Dagger.get_tls().sch_uid)
    states = proc_states(uid)
    return MemPool.lock_read(states.lock) do
        return collect(values(states.dict))
    end
end
function proc_state!(f, uid::UInt64, proc::Processor)
    states = proc_states(uid)
    state = MemPool.lock_read(states.lock) do
        return get(states.dict, proc, nothing)
    end
    if state === nothing
        state = MemPool.lock(states.lock) do
            existing = get(states.dict, proc, nothing)
            existing !== nothing && return existing
            new_state = f()::ProcessorState
            states.dict[proc] = new_state
            return new_state
        end
    end
    return state
end
proc_state!(f, proc::Processor) = proc_state!(f, Dagger.get_tls().sch_uid, proc)
function maybe_proc_state(uid::UInt64, proc::Processor)
    states = proc_states(uid)
    return MemPool.lock_read(states.lock) do
        return get(states.dict, proc, nothing)
    end
end
maybe_proc_state(proc::Processor) = maybe_proc_state(Dagger.get_tls().sch_uid, proc)
proc_state(uid::UInt64, proc::Processor) = something(maybe_proc_state(uid, proc))
proc_state(proc::Processor) = proc_state(Dagger.get_tls().sch_uid, proc)

task_tid_for_processor(::Processor) = nothing
task_tid_for_processor(proc::Dagger.ThreadProc) = proc.tid

stealing_permitted(::Processor) = true
stealing_permitted(proc::Dagger.ThreadProc) = proc.owner != 1 || proc.tid != 1

proc_has_occupancy(proc_occupancy, task_occupancy) =
    UInt64(task_occupancy) + UInt64(proc_occupancy) <= typemax(UInt32)

function start_processor_runner!(istate::ProcessorInternalState, uid::UInt64, return_queue::RemoteChannel, start_event::Base.Event)
    to_proc = istate.proc
    proc_run_task = @task begin
        # Wait for our ProcessorState to be configured
        wait(start_event)

        # FIXME: Context changes aren't noticed over time
        ctx = istate.ctx
        tasks = istate.tasks
        proc_occupancy = istate.proc_occupancy
        time_pressure = istate.time_pressure

        wid = get_parent(to_proc).pid
        work_to_do = false
        while isopen(return_queue)
            # Wait for new tasks
            if !work_to_do
                @dagdebug nothing :processor "Waiting for tasks"
                @maybelog ctx timespan_start(ctx, :proc_run_wait, (;uid, worker=wid, processor=to_proc), nothing)
                wait(istate.reschedule)
                @static if VERSION >= v"1.9"
                    reset(istate.reschedule)
                end
                @maybelog ctx timespan_finish(ctx, :proc_run_wait, (;uid, worker=wid, processor=to_proc), nothing)
                if istate.done[]
                    return
                end
            end

            # Fetch a new task to execute
            @dagdebug nothing :processor "Trying to dequeue"
            @maybelog ctx timespan_start(ctx, :proc_run_fetch, (;uid, worker=wid, processor=to_proc), nothing)
            work_to_do = false
            task_and_occupancy = lock(istate.queue) do queue
                # Only steal if there are multiple queued tasks, to prevent
                # ping-pong of tasks between empty queues
                if length(queue) == 0
                    @dagdebug nothing :processor "Nothing to dequeue"
                    return nothing
                end
                _, occupancy = first(queue)
                if !proc_has_occupancy(proc_occupancy[], occupancy)
                    @dagdebug nothing :processor "Insufficient occupancy" proc_occupancy=proc_occupancy[] task_occupancy=occupancy
                    return nothing
                end
                queue_result = popfirst!(queue)
                work_to_do = length(queue) > 0
                return queue_result
            end
            if task_and_occupancy === nothing
                @maybelog ctx timespan_finish(ctx, :proc_run_fetch, (;uid, worker=wid, processor=to_proc), nothing)

                @dagdebug nothing :processor "Failed to dequeue"

                if !stealing_permitted(to_proc)
                    continue
                end

                if proc_occupancy[] == typemax(UInt32)
                    continue
                end

                @dagdebug nothing :processor "Trying to steal"

                # Try to steal a task
                @maybelog ctx timespan_start(ctx, :proc_steal_local, (;uid, worker=wid, processor=to_proc), nothing)

                # Try to steal from local queues randomly
                # TODO: Prioritize stealing from busiest processors
                states = proc_states_values(uid)
                # TODO: Try to pre-allocate this
                P = randperm(length(states))
                for state in getindex.(Ref(states), P)
                    other_istate = state.state
                    if other_istate.proc === to_proc
                        continue
                    end
                    # FIXME: We need to lock two queues to compare occupancies
                    proc_occupancy_cached = lock(istate.queue) do _
                        proc_occupancy[]
                    end
                    task_and_occupancy = lock(other_istate.queue) do queue
                        if length(queue) == 0
                            return nothing
                        end
                        task, occupancy = first(queue)
                        scope = task.scope
                        if Dagger.proc_in_scope(to_proc, scope)
                           typemax(UInt32) - proc_occupancy_cached >= occupancy
                            # Compatible, steal this task
                            return popfirst!(queue)
                        end
                        return nothing
                    end
                    if task_and_occupancy !== nothing
                        from_proc = other_istate.proc
                        thunk_id = task.thunk_id
                        @dagdebug thunk_id :processor "Stolen from $from_proc by $to_proc"
                        @maybelog ctx timespan_finish(ctx, :proc_steal_local, (;uid, worker=wid, processor=to_proc), (;from_proc, thunk_id))
                        # TODO: Keep stealing until we hit full occupancy?
                        @goto execute
                    end
                end
                @maybelog ctx timespan_finish(ctx, :proc_steal_local, (;uid, worker=wid, processor=to_proc), nothing)

                # TODO: Try to steal from remote queues

                continue
            end

            @label execute
            task, task_occupancy = task_and_occupancy
            thunk_id = task.thunk_id
            time_util = task.est_time_util
            @maybelog ctx timespan_finish(ctx, :proc_run_fetch, (;uid, worker=wid, processor=to_proc), (;thunk_id, proc_occupancy=proc_occupancy[], task_occupancy))
            @dagdebug thunk_id :processor "Dequeued task"

            # Skip tasks cancelled by the fallback (which may have fired
            # after do_tasks enqueued the task but before the proc runner reached
            # here). Checking here — before proc_occupancy is incremented — ensures
            # we never hold occupancy for a task that already finished.
            let states_ref = proc_states(uid)
                is_pre_cancelled = lock(states_ref.pre_cancelled_lock) do
                    if thunk_id in states_ref.pre_cancelled
                        delete!(states_ref.pre_cancelled, thunk_id)
                        true
                    else
                        false
                    end
                end
                if is_pre_cancelled
                    @dagdebug thunk_id :processor "Skipping pre-cancelled task in proc runner"
                    # Clean up TASKS_RUNNING so the ID doesn't leak
                    # (do_tasks may have added it before the fallback fired).
                    lock(TASK_SYNC) do
                        pop!(TASKS_RUNNING, thunk_id, nothing)
                    end
                    # Re-check for more work immediately instead of waiting on the doorbell.
                    work_to_do = true
                    continue
                end
            end

            # Set up cancellation and update task accounting.
            # task_specs is populated here (same lock as cancel_tokens and
            # proc_occupancy) so that the pre-running cancel path in
            # cancellation.jl can look up est_occupancy/est_time_util to
            # correctly decrement proc_occupancy even before the task is
            # recorded in istate.tasks at step 5.
            cancel_token = Dagger.CancelToken()
            lock(istate.queue) do _
                istate.cancel_tokens[thunk_id] = cancel_token
                istate.task_specs[thunk_id] = task
                proc_occupancy[] += task_occupancy
                time_pressure[] += time_util
            end

            # Launch the task
            t = @reusable_tasks :start_processor_runner!_task_cache 32 t->begin
                tid = task_tid_for_processor(to_proc)
                if tid !== nothing
                    Dagger.set_task_tid!(t, tid)
                else
                    t.sticky = false
                end
            end "thunk $thunk_id" DoTaskSpec(to_proc, return_queue, task, cancel_token)

            # Record the launched task
            lock(istate.queue) do _
                tasks[thunk_id] = t
            end
        end
    end
    tid = task_tid_for_processor(to_proc)
    if tid !== nothing
        Dagger.set_task_tid!(proc_run_task, tid)
    else
        proc_run_task.sticky = false
    end
    return errormonitor_tracked("processor $to_proc", schedule(proc_run_task))
end
struct DoTaskSpec
    to_proc::Processor
    chan::RemoteChannel
    task::TaskSpec
    cancel_token::Dagger.CancelToken
end
function (dts::DoTaskSpec)()
    to_proc = dts.to_proc
    task = dts.task
    tid = task.thunk_id
    Dagger.DTASK_CANCEL_TOKEN[] = dts.cancel_token

    # Execute the task and return its result
    was_cancelled = false
    result, metadata = try
        do_task(to_proc, task)
    catch err
        bt = catch_backtrace()
        (CapturedException(err, bt), nothing)
    finally
        state = maybe_proc_state(task.sch_uid, to_proc)
        # state will be nothing if processor was removed due to scheduler exit
        if state !== nothing
            istate = state.state
            while true
                # Wait until the task has been recorded in the processor state
                done = lock(istate.queue) do _
                    if haskey(istate.tasks, tid)
                        delete!(istate.tasks, tid)
                        delete!(istate.task_specs, tid)
                        if !(tid in istate.cancelled)
                            istate.proc_occupancy[] -= task.est_occupancy
                            istate.time_pressure[] -= task.est_time_util
                        else
                            # Task was cancelled, so occupancy and pressure are
                            # already reduced
                            pop!(istate.cancelled, tid)
                            delete!(istate.cancel_tokens, tid)
                            was_cancelled = true
                        end
                        return true
                    end
                    return false
                end
                done && break
                sleep(0.1)
            end
            notify(istate.reschedule)
        end

        # Ensure that any spawned tasks get cleaned up
        Dagger.cancel!(dts.cancel_token)

        # Reset TLS so that reusable tasks don't inherit stale Dagger context.
        Dagger.DTASK_TLS[] = nothing
        Dagger.DTASK_CANCEL_TOKEN[] = nothing
    end
    if was_cancelled
        # A result was already posted to the return queue
        return
    end

    # Local fast-path: if this result was produced in-process (the
    # common multithreaded case), finish it directly on this worker thread via
    # handle_result! instead of round-tripping through state.chan and
    # serializing on the single scheduler_run task. This lets N worker threads
    # finish N tasks (and schedule their freed dependents) concurrently.
    #
    # We only take the fast-path for *successful* results: task errors and
    # worker-death signals must travel over the channel so the master's
    # scheduler_run can apply batch-mode error/fault semantics centrally.
    if Dagger.root_worker_id(to_proc) == myid() && !(result isa Exception)
        state = lookup_scheduler_state(task.sch_uid)
        if state !== nothing
            try
                handle_result!(state.ctx, state, myid(), to_proc, tid, result, metadata)
            catch err
                @dagdebug tid :execute "Local result handling failed" exception=(err, catch_backtrace())
                rethrow()
            end
            return
        end
        # No registered state (scheduler torn down): fall through to the channel.
    end

    return_queue = dts.chan
    try
        put!(return_queue, TaskResult(myid(), to_proc, tid, result, metadata))
    catch err
        if unwrap_nested_exception(err) isa InvalidStateException || !isopen(return_queue)
            @dagdebug tid :execute "Return queue is closed, failing to put result" chan=return_queue exception=(err, catch_backtrace())
        else
            rethrow()
        end
    end
    return
end

"""
    do_tasks(to_proc, return_queue, tasks)

Executes a batch of tasks on `to_proc`, returning their results through
`return_queue`.
"""
function do_tasks(to_proc, return_queue, tasks)
    @dagdebug nothing :processor "Enqueuing task batch" batch_size=length(tasks)

    ctx_vars = first(tasks).ctx_vars
    ctx = Context(Processor[]; log_sink=ctx_vars.log_sink, profile=ctx_vars.profile)
    uid = first(tasks).sch_uid
    start_event = nothing
    state = proc_state!(uid, to_proc) do
        # Initialize the processor state and runner
        queue = PriorityQueue{TaskSpec, UInt32}()
        queue_locked = LockedObject(queue)
        reschedule = Doorbell()
        istate = ProcessorInternalState(ctx, to_proc, return_queue,
                                        queue_locked, reschedule,
                                        Dict{Int,Task}(),
                                        Dict{Int,Vector{Any}}(),
                                        Ref(UInt32(0)), Ref(UInt64(0)),
                                        Set{Int}(),
                                        Dict{Int,Dagger.CancelToken}(),
                                        Ref(false))
        start_event = Base.Event()
        runner = start_processor_runner!(istate, uid, return_queue, start_event)
        @static if VERSION < v"1.9"
            reschedule.waiter = runner
        end
        return ProcessorState(istate, runner)
    end
    if start_event !== nothing
        notify(start_event)
    end
    istate = state.state
    states = proc_states(uid)
    lock(istate.queue) do queue
        for task in tasks
            thunk_id = task.thunk_id
            occupancy = task.est_occupancy
            @maybelog ctx timespan_start(ctx, :enqueue, (;uid, processor=to_proc, thunk_id), nothing)

            # Skip tasks cancelled by the fallback before do_tasks ran.
            # The fallback marks thunk IDs in states.pre_cancelled so we don't
            # increment proc_occupancy for an already-finished task.
            is_pre_cancelled = lock(states.pre_cancelled_lock) do
                if thunk_id in states.pre_cancelled
                    delete!(states.pre_cancelled, thunk_id)
                    true
                else
                    false
                end
            end
            if is_pre_cancelled
                @dagdebug thunk_id :processor "Skipping pre-cancelled task in do_tasks"
                continue
            end

            should_launch = lock(TASK_SYNC) do
                # Already running; don't try to re-launch
                if !(thunk_id in TASKS_RUNNING)
                    push!(TASKS_RUNNING, thunk_id)
                    true
                else
                    false
                end
            end
            should_launch || continue
            push!(queue, task => occupancy)
            @maybelog ctx timespan_finish(ctx, :enqueue, (;uid, processor=to_proc, thunk_id), nothing)
            @dagdebug thunk_id :processor "Enqueued task"
        end
    end
    notify(istate.reschedule)

    # Kick other processors to make them steal
    # TODO: Alternatively, automatically balance work instead of blindly enqueueing
    states = proc_states_values(uid)
    P = randperm(length(states))
    for other_state in getindex.(Ref(states), P)
        other_istate = other_state.state
        if other_istate.proc === to_proc
            continue
        end
        notify(other_istate.reschedule)
    end
    @dagdebug nothing :processor "Kicked processors"
end

"""
    do_task(to_proc, task::TaskSpec) -> Any

Executes a single task specified by `task` on `to_proc`.
"""
@reuse_scope function do_task(to_proc, task::TaskSpec)
    thunk_id = task.thunk_id

    ctx_vars = task.ctx_vars
    ctx = Context(Processor[]; log_sink=ctx_vars.log_sink, profile=ctx_vars.profile)

    from_proc = OSProc()
    data = task.data
    Tf = task.Tf
    f = isdefined(Tf, :instance) ? Tf.instance : nothing

    # Wait for required resources to become available
    options = task.options
    propagated = get_propagated_options(options)
    to_storage = options.storage !== nothing ? fetch(options.storage) : MemPool.GLOBAL_DEVICE[]
    #to_storage_name = nameof(typeof(to_storage))
    #storage_cap = storage_capacity(to_storage)

    est_time_util = task.est_time_util
    est_alloc_util = task.est_alloc_util
    real_time_util = Ref{UInt64}(0)
    real_alloc_util = UInt64(0)
    #= FIXME: Serialize on over-memory situation
    @maybelog ctx timespan_start(ctx, :storage_wait, (;thunk_id, processor=to_proc), (;f, device=typeof(to_storage)))
    if !meta
        # Factor in the memory costs for our lazy arguments
        for arg in data[2:end]
            if Dagger.valuetype(arg) <: Chunk
                est_alloc_util += Dagger.value(arg).handle.size
            end
        end
    end
    debug_storage(msg::String) = @debug begin
        let est_alloc_util=Base.format_bytes(est_alloc_util),
            real_alloc_util=Base.format_bytes(real_alloc_util),
            storage_cap=Base.format_bytes(storage_cap)
            "[$(myid()), $thunk_id] $f($Tdata) $msg: $est_alloc_util | $real_alloc_util/$storage_cap"
        end
    end
    lock(TASK_SYNC) do
        while true
            # Get current time utilization for the selected processor
            time_dict = get!(()->Dict{Processor,Ref{UInt64}}(), PROCESSOR_TIME_UTILIZATION, task.sch_uid)
            real_time_util = get!(()->Ref{UInt64}(UInt64(0)), time_dict, to_proc)

            # Get current allocation utilization and capacity
            real_alloc_util = storage_utilized(to_storage)
            storage_cap = storage_capacity(to_storage)

            # Check if we'll go over memory capacity from running this thunk
            # Waits for free storage, if necessary
            # TODO: Implement a priority queue, ordered by est_alloc_util
            #if est_alloc_util > storage_cap
            #    debug_storage("WARN: Estimated utilization above storage capacity on $to_storage_name, proceeding anyway")
            #    break
            #end
            #if est_alloc_util + real_alloc_util > storage_cap
            #    if MemPool.externally_varying(to_storage)
            #        debug_storage("WARN: Insufficient space and allocation behavior is externally varying on $to_storage_name, proceeding anyway")
            #        break
            #    end
            #    if length(TASKS_RUNNING) <= 2 # This task + eager submission task
            #        debug_storage("WARN: Insufficient space and no other running tasks on $to_storage_name, proceeding anyway")
            #        break
            #    end
            #    # Fully utilized, wait and re-check
            #    debug_storage("Waiting for free $to_storage_name")
            #    wait(TASK_SYNC)
            #else
            #    # Sufficient free storage is available, prepare for execution
            #    debug_storage("Using available $to_storage_name")
            #    break
            #end
            # FIXME
            break
        end
    end
    @maybelog ctx timespan_finish(ctx, :storage_wait, (;thunk_id, processor=to_proc), (;f, device=typeof(to_storage)))
    =#

    @dagdebug thunk_id :execute "Moving data"

    # Initiate data transfers for function and arguments
    transfer_time = Threads.Atomic{UInt64}(0)
    transfer_size = Threads.Atomic{UInt64}(0)
    _data = if something(options.meta, false)
        Argument[first(data)] # always fetch function
    else
        data
    end
    fetch_tasks = map(_data) do arg
        #=FIXME:REALLOC_TASKS=#
        Threads.@spawn begin
            value = Dagger.value(arg)
            position = arg.pos
            @maybelog ctx timespan_start(ctx, :move, (;thunk_id, position, processor=to_proc), (;f, data=value))
            #= FIXME: This isn't valid if x is written to
            x = if x isa Chunk
                value = lock(TASK_SYNC) do
                    if haskey(CHUNK_CACHE, x)
                        Some{Any}(get!(CHUNK_CACHE[x], to_proc) do
                            # Convert from cached value
                            # TODO: Choose "closest" processor of same type first
                            some_proc = first(keys(CHUNK_CACHE[x]))
                            some_x = CHUNK_CACHE[x][some_proc]
                            @dagdebug thunk_id :move "Cache hit for argument $id at $some_proc: $some_x"
                            @invokelatest move(some_proc, to_proc, some_x)
                        end)
                    else
                        nothing
                    end
                end

                if value !== nothing
                    something(value)
                else
                    # Fetch it
                    time_start = time_ns()
                    from_proc = processor(x)
                    _x = @invokelatest move(from_proc, to_proc, x)
                    time_finish = time_ns()
                    if x.handle.size !== nothing
                        Threads.atomic_add!(transfer_time, time_finish - time_start)
                        Threads.atomic_add!(transfer_size, x.handle.size)
                    end

                    @dagdebug thunk_id :move "Cache miss for argument $id at $from_proc"

                    # Update cache
                    lock(TASK_SYNC) do
                        CHUNK_CACHE[x] = Dict{Processor,Any}()
                        CHUNK_CACHE[x][to_proc] = _x
                    end

                    _x
                end
            else
            =#
            new_value = @invokelatest move(to_proc, value)
            #end
            if new_value !== value
                @dagdebug thunk_id :move "Moved argument @ $position to $to_proc: $(typeof(value)) -> $(typeof(new_value))"
            end
            @maybelog ctx timespan_finish(ctx, :move, (;thunk_id, position, processor=to_proc), (;f, data=new_value); tasks=[Base.current_task()])
            arg.value = new_value
            return
        end
    end
    for task in fetch_tasks
        fetch_report(task)
    end

    f = Dagger.value(first(data))
    @assert !(f isa Chunk) "Failed to unwrap thunk function"
    fetched_args = @reusable_vector :do_task_fetched_args Any nothing 32
    fetched_args_cleanup = @reuse_defer_cleanup empty!(fetched_args)
    fetched_kwargs = @reusable_vector :do_task_fetched_kwargs Pair{Symbol,Any} :NULL=>nothing 32
    fetched_kwargs_cleanup = @reuse_defer_cleanup empty!(fetched_kwargs)
    for idx in 2:length(data)
        arg = data[idx]
        if Dagger.ispositional(arg)
            push!(fetched_args, Dagger.value(arg))
        else
            push!(fetched_kwargs, Dagger.pos_kw(arg) => Dagger.value(arg))
        end
    end

    #= FIXME: If MaxUtilization, stop processors and wait
    if (est_time_util isa MaxUtilization) && (real_time_util > 0)
        # FIXME: Stop processors
        # FIXME: Wait on processors to stop
        est_time_util = count(c->typeof(c)===typeof(to_proc), children(from_proc))
    end
    =#

    real_time_util[] += est_time_util
    @maybelog ctx timespan_start(ctx, :compute, (;thunk_id, processor=to_proc), (;f))

    task_sig = signature(first(data), @view data[2:end]).sig

    local_metrics_cache = MT.MetricsCache()
    mspec = execute_metrics_spec()

    @dagdebug thunk_id :execute "Executing $(typeof(f))"

    logging_enabled = !(ctx.log_sink isa TimespanLogging.NoOpLog)

    result_meta = try
        Dagger.set_tls!((;
            sch_uid=task.sch_uid,
            sch_handle=task.sch_handle,
            processor=to_proc,
            task_spec=task,
            cancel_token=Dagger.DTASK_CANCEL_TOKEN[],
            logging_enabled,
            metrics_cache=local_metrics_cache,
        ))

        result = Dagger.with_options(propagated) do
            @with TASK_SIGNATURE => task_sig TASK_PROCESSOR => to_proc TASK_WORKER => myid() TASK_TRANSFER_SIZE => transfer_size[] TASK_TRANSFER_TIME => transfer_time[] begin
                MT.with_metrics(mspec, Dagger, :execute!, thunk_id, MT.SyncInto(local_metrics_cache)) do
                    execute!(to_proc, f, fetched_args...; fetched_kwargs...)
                end
            end
        end

        # Check if result is safe to store
        # FIXME: Move here and below *after* timespan_finish for :compute
        device = nothing
        if !(result isa Chunk)
            @maybelog ctx timespan_start(ctx, :storage_safe_scan, (;thunk_id, processor=to_proc), (;T=typeof(result)))
            device = if walk_storage_safe(result)
                to_storage
            else
                MemPool.CPURAMDevice()
            end
            @maybelog ctx timespan_finish(ctx, :storage_safe_scan, (;thunk_id, processor=to_proc), (;T=typeof(result)))
        end

        # Construct result
        result_meta = if something(options.get_result, false) || something(options.meta, false)
            result
        else
            # TODO: Cache this Chunk locally in CHUNK_CACHE right now
            tochunk(result, to_proc, @something(options.result_scope, AnyScope());
                    device,
                    tag=options.storage_root_tag,
                    leaf_tag=something(options.storage_leaf_tag, MemPool.Tag()),
                    retain=something(options.storage_retain, false))
        end
    catch ex
        bt = catch_backtrace()
        RemoteException(myid(), CapturedException(ex, bt))
    finally
        fetched_args_cleanup()
        fetched_kwargs_cleanup()
    end

    @maybelog ctx timespan_finish(ctx, :compute, (;thunk_id, processor=to_proc), (;f, result=result_meta))

    lock(TASK_SYNC) do
        real_time_util[] -= est_time_util
        pop!(TASKS_RUNNING, thunk_id)
        notify(TASK_SYNC)
    end

    @dagdebug thunk_id :execute "Returning"

    collected_metrics = extract_collected_metrics(local_metrics_cache, thunk_id)

    metadata = (
        time_pressure=real_time_util[],
        metrics=collected_metrics,
    )
    return (result_meta, metadata)
end

end # module Sch
