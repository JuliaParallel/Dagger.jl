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
import ..Dagger: Context, Processor, SchedulerOptions, Options, Thunk, WeakThunk, ThunkFuture, ThunkID, TaskID, TASKID_ZERO, DTaskFailedException, Chunk, WeakChunk, OSProc, AnyScope, DefaultScope, InvalidScope, LockedObject, Argument, Signature, ConcurrentDict, ConcurrentSet, ConcurrentVector
import ..Dagger: order, dependents, noffspring, istask, inputs, unwrap_weak_checked, wrap_weak, affinity, tochunk, timespan_start, timespan_finish, procs, move, chunktype, default_enabled, processor, get_processors, get_parent, execute!, rmprocs!, task_processor, constrain, cputhreadtime, maybe_take_or_alloc!
import ..Dagger: @dagdebug, @safe_lock_spin1, @maybelog, @take_or_alloc!
import DataStructures: PriorityQueue, enqueue!, dequeue_pair!, peek

import ..Dagger: ReusableCache, ReusableLinkedList, ReusableDict
import ..Dagger: @reusable, @reusable_dict, @reusable_vector, @reusable_tasks, @reuse_scope, @reuse_defer_cleanup

import TimespanLogging

import TaskLocalValues: TaskLocalValue
import ScopedValues: @with

const OneToMany = Dict{Thunk, Set{Thunk}}

include("util.jl")
include("fault-handler.jl")
include("dynamic.jl")

struct TaskResult
    pid::Int
    proc::Processor
    thunk_id::TaskID
    result::Any
    metadata::Union{NamedTuple,Nothing}
end

"""
    TaskCompletionNotification

Sent to registered watchers when a task completes on any worker.
Carries the thunk ID, whether the task failed, and the result, enabling
cross-worker coordination without centralized scheduling.
"""
struct TaskCompletionNotification
    thunk_id::TaskID
    failed::Bool
    result::Any
end

const AnyTaskResult = Union{RescheduleSignal, TaskResult, TaskCompletionNotification}

@enum ThunkStateValue begin
    THUNK_WAITING
    THUNK_READY
    THUNK_RUNNING
    THUNK_FINISHED
end
mutable struct ThunkState
    @atomic value::ThunkStateValue
    ThunkState() = new(THUNK_WAITING)
end
Base.getindex(state::ThunkState) = state.value
Base.setindex!(state::ThunkState, value::ThunkStateValue) = (@atomic state.value = value)
try_transition!(state::ThunkState, old_value::ThunkStateValue, new_value::ThunkStateValue) =
    (@atomicreplace state.value old_value => new_value).success

struct RunningState
    running::Set{Thunk}
    running_on::Dict{Thunk,OSProc}
end

"""
    ComputeState

The internal state-holding struct of the scheduler.

Fields:
- `uid::UInt64` - Unique identifier for this scheduler instance
- `valid::WeakKeyDict{Thunk, Nothing}` - Tracks all `Thunk`s that are in a valid scheduling state
- `thunk_state::ConcurrentDict{Thunk, ThunkState}` - Tracks the state of each thunk
- `waiting::ConcurrentDict{Thunk, Set{Thunk}}` - The list of `Thunk`s that are waiting for dependencies to complete
- `waiting_data::ConcurrentDict{Union{Thunk,Chunk}, Set{Thunk}}` - The list of `Thunk`s that are waiting for dependencies to complete
- `ready::ConcurrentVector{Thunk}` - The list of `Thunk`s that are ready to execute
- `running_state::LockedObject{RunningState}` - State of the currently-running thunks
- `thunk_dict::Dict{TaskID, WeakThunk}` - Maps from thunk IDs to a `Thunk`
- `node_order::Any` - Function that returns the order of a thunk
- `equiv_chunks::WeakKeyDict{DRef,Chunk}` - Cache mapping from `DRef` to a `Chunk` which contains it
- `worker_time_pressure::Dict{Int,Dict{Processor,UInt64}}` - Maps from worker ID to processor pressure
- `worker_storage_pressure::Dict{Int,Dict{Union{StorageResource,Nothing},UInt64}}` - Maps from worker ID to storage resource pressure
- `worker_storage_capacity::Dict{Int,Dict{Union{StorageResource,Nothing},UInt64}}` - Maps from worker ID to storage resource capacity
- `worker_loadavg::Dict{Int,NTuple{3,Float64}}` - Worker load average
- `worker_chans::Dict{Int, Tuple{RemoteChannel,RemoteChannel}}` - Communication channels between the scheduler and each worker
- `signature_time_cost::Dict{Signature,UInt64}` - Cache of estimated CPU time (in nanoseconds) required to compute calls with the given signature
- `signature_alloc_cost::Dict{Signature,UInt64}` - Cache of estimated CPU RAM (in bytes) required to compute calls with the given signature
- `transfer_rate::Ref{UInt64}` - Estimate of the network transfer rate in bytes per second
- `halt::Base.Event` - Event indicating that the scheduler is halting
- `lock::ReentrantLock` - Lock around operations which modify the state
- `futures::Dict{Thunk, Vector{ThunkFuture}}` - Futures registered for waiting on the result of a thunk.
- `errored::WeakKeyDict{Thunk,Bool}` - Indicates if a thunk's result is an error.
- `thunks_to_delete::Set{Thunk}` - The list of `Thunk`s ready to be deleted upon completion.
- `chan::RemoteChannel{Channel{AnyTaskResult}}` - Channel for receiving completed thunks and other messages.
- `completion_watchers::ConcurrentDict{Int, Vector{RemoteChannel}}` - Per-thunk-ID channels to notify on completion, enabling cross-worker coordination.
"""
struct ComputeState
    uid::UInt64
    valid::ConcurrentDict{Thunk, Nothing}
    thunk_state::ConcurrentDict{Thunk, ThunkState}
    waiting::ConcurrentDict{Thunk, Set{Thunk}}
    waiting_data::ConcurrentDict{Union{Thunk,Chunk}, Set{Thunk}}
    ready::ConcurrentVector{Thunk}
    running_state::LockedObject{RunningState}
    thunk_dict::ConcurrentDict{TaskID, WeakThunk}
    node_order::Any
    equiv_chunks::LockedObject{WeakKeyDict{DRef,Chunk}}
    worker_time_pressure::ConcurrentDict{Int,ConcurrentDict{Processor,UInt64}}
    worker_storage_pressure::ConcurrentDict{Int,Dict{Union{StorageResource,Nothing},UInt64}}
    worker_storage_capacity::ConcurrentDict{Int,Dict{Union{StorageResource,Nothing},UInt64}}
    worker_loadavg::ConcurrentDict{Int,NTuple{3,Float64}}
    worker_chans::ConcurrentDict{Int,Tuple{RemoteChannel,RemoteChannel}}
    signature_time_cost::ConcurrentDict{Signature,UInt64}
    signature_alloc_cost::ConcurrentDict{Signature,UInt64}
    transfer_rate::Ref{UInt64}
    halt::Base.Event
    lock::ReentrantLock
    futures::ConcurrentDict{Thunk, Vector{ThunkFuture}}
    thunks_to_delete::ConcurrentSet{Thunk}
    chan::RemoteChannel{Channel{AnyTaskResult}}
    completion_watchers::ConcurrentDict{Thunk, Vector{RemoteChannel}}
end

const UID_COUNTER = Threads.Atomic{UInt64}(1)

function start_state(deps::Dict, node_order, chan)
    waiting_data = ConcurrentDict{Union{Thunk,Chunk},Set{Thunk}}()
    lock(waiting_data) do wd
        merge!(wd, deps)
    end
    state = ComputeState(Threads.atomic_add!(UID_COUNTER, UInt64(1)),
                         ConcurrentDict{Thunk, Nothing}(),
                         ConcurrentDict{Thunk, ThunkState}(),
                         ConcurrentDict{Thunk, Set{Thunk}}(),
                         waiting_data,
                         ConcurrentVector{Thunk}(),
                         LockedObject(RunningState(Set{Thunk}(), Dict{Thunk,OSProc}())),
                         ConcurrentDict{TaskID, WeakThunk}(),
                         node_order,
                         LockedObject(WeakKeyDict{DRef,Chunk}()),
                         ConcurrentDict{Int,ConcurrentDict{Processor,UInt64}}(),
                         ConcurrentDict{Int,Dict{Union{StorageResource,Nothing},UInt64}}(),
                         ConcurrentDict{Int,Dict{Union{StorageResource,Nothing},UInt64}}(),
                         ConcurrentDict{Int,NTuple{3,Float64}}(),
                         ConcurrentDict{Int,Tuple{RemoteChannel,RemoteChannel}}(),
                         ConcurrentDict{Signature,UInt64}(),
                         ConcurrentDict{Signature,UInt64}(),
                         Ref{UInt64}(1_000_000),
                         Base.Event(),
                         ReentrantLock(),
                         ConcurrentDict{Thunk, Vector{ThunkFuture}}(),
                         ConcurrentSet{Thunk}(),
                         chan,
                         ConcurrentDict{Thunk, Vector{RemoteChannel}}())

    for k in sort(collect(keys(deps)), by=node_order)
        if istask(k)
            thunk_state = lock(state.thunk_state) do thunk_states
                thunk_states[k] = ThunkState()
            end
            waiting = Set{Thunk}(Dagger.syncdeps_iterator(k))
            if isempty(waiting)
                @assert try_transition!(thunk_state, THUNK_WAITING, THUNK_READY)
                push!(state.ready, k)
            else
                state.waiting[k] = waiting
            end
            state.valid[k] = nothing
        end
    end
    return state
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
    lock(state.lock) do
        state.worker_time_pressure[p.pid] = ConcurrentDict{Processor,UInt64}()

        state.worker_storage_pressure[p.pid] = Dict{Union{StorageResource,Nothing},UInt64}()
        state.worker_storage_capacity[p.pid] = Dict{Union{StorageResource,Nothing},UInt64}()
        #= FIXME
        for storage in get_storage_resources(gproc)
            pressure, capacity = remotecall_fetch(gproc.pid, storage) do storage
                storage_pressure(storage), storage_capacity(storage)
            end
            state.worker_storage_pressure[p.pid][storage] = pressure
            state.worker_storage_capacity[p.pid][storage] = capacity
        end
        =#

        state.worker_loadavg[p.pid] = (0.0, 0.0, 0.0)
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
                                    put!(d[uid], TaskResult(wid, OSProc(wid), TASKID_ZERO, ProcessExitedException(wid), nothing))
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
const TASKS_RUNNING = Set{TaskID}()

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
    state = start_state(deps, node_order, chan)

    master = OSProc(myid())

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
    for node in filter(istask, keys(deps))
        state.thunk_dict[node.id] = WeakThunk(node)
        for dep in deps[node]
            state.thunk_dict[dep.id] = WeakThunk(dep)
        end
    end

    # Initialize workers
    @sync for p in procs_to_use(ctx, options)
        Threads.@spawn begin
            try
                init_proc(state, p, ctx.log_sink)
            catch err
                @error "Error initializing worker $p" exception=(err,catch_backtrace())
                remove_dead_proc!(ctx, state, p, options)
            end
        end
    end

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

function scheduler_run(ctx, state::ComputeState, d::Thunk, options::SchedulerOptions)
    @dagdebug nothing :global "Initializing scheduler" uid=state.uid

    safepoint(state)

    ntasks = Threads.nthreads()
    in_flight = Threads.Atomic{Int}(0)
    done = Threads.Atomic{Bool}(false)

    function try_schedule!()
        if !isempty(state.ready)
            check_workers_available(ctx, options)
            @invokelatest schedule!(ctx, state, options)
        end
    end

    function signal_done!()
        done[] = true
        for _ in 1:(ntasks-1)
            try
                put!(state.chan, RescheduleSignal())
            catch
            end
        end
    end

    Threads.atomic_add!(in_flight, 1)
    try
        try_schedule!()
    finally
        Threads.atomic_sub!(in_flight, 1)
    end

    #= FIXME: REMOVE ME
    errormonitor(Threads.@spawn begin
        sleep(10)
        while !done[]
            sleep(1)
            iob = IOBuffer()
            println(iob, "[$(myid())] SCH STATUS")
            print_sch_status(iob, state, d)
            println(iob, "[$(myid())] SCH STATUS END")
            seek(iob, 0)
            write(stderr, iob)
        end
    end)=#

    worker_tasks = Vector{Task}(undef, ntasks)
    for i in 1:ntasks
        worker_tasks[i] = Threads.@spawn begin
            try
                while !done[]
                    @maybelog ctx timespan_start(ctx, :take, (;uid=state.uid), nothing)
                    @dagdebug nothing :take "Task $i waiting for results"
                    tresult = try
                        take!(state.chan)
                    catch
                        return
                    end
                    @maybelog ctx timespan_finish(ctx, :take, (;uid=state.uid), nothing)
                    done[] && return

                    Threads.atomic_add!(in_flight, 1)
                    try
                        if tresult isa RescheduleSignal
                            try_schedule!()
                            continue
                        elseif tresult isa TaskCompletionNotification
                            thunk_id = tresult.thunk_id
                            @assert !Dagger.is_task_local(thunk_id)
                            thunk_failed = tresult.failed

                            @dagdebug thunk_id :take "Got task completion notification"
                            thunk = unwrap_weak_checked(state.thunk_dict[thunk_id])::Thunk
                            @assert !has_result(thunk)
                            store_result!(thunk, tresult.result; error=thunk_failed)
                            @lock state.lock finish_task!(ctx, state, thunk, thunk_failed)

                            try_schedule!()
                            continue
                        end

                        tresult::TaskResult
                        pid = tresult.pid
                        proc = tresult.proc
                        thunk_id = tresult.thunk_id
                        res = tresult.result

                        @dagdebug thunk_id :take "Got finished task"
                        gproc = OSProc(pid)
                        safepoint(state)

                        thunk_failed = false
                        if res isa Exception
                            if unwrap_nested_exception(res) isa ProcessExitedException
                                @warn "Worker $(pid) died, rescheduling work"

                                @lock state.lock begin
                                    @maybelog ctx timespan_start(ctx, :remove_procs, (;uid=state.uid, worker=pid), nothing)
                                    remove_dead_proc!(ctx, state, gproc, options)
                                    @maybelog ctx timespan_finish(ctx, :remove_procs, (;uid=state.uid, worker=pid), nothing)

                                    @maybelog ctx timespan_start(ctx, :handle_fault, (;uid=state.uid, worker=pid), nothing)
                                    handle_fault(ctx, state, gproc)
                                    @maybelog ctx timespan_finish(ctx, :handle_fault, (;uid=state.uid, worker=pid), nothing)
                                end

                                try_schedule!()
                                continue
                            else
                                if something(options.allow_errors, false)
                                    thunk_failed = true
                                else
                                    throw(res)
                                end
                            end
                        end
                        thunk = unwrap_weak_checked(state.thunk_dict[thunk_id])::Thunk
                        metadata = tresult.metadata
                        if metadata !== nothing
                            state.worker_time_pressure[pid][proc] = metadata.time_pressure
                            #to_storage = fetch(thunk.options.storage)
                            #state.worker_storage_pressure[pid][to_storage] = metadata.storage_pressure
                            #state.worker_storage_capacity[pid][to_storage] = metadata.storage_capacity
                            #state.worker_loadavg[pid] = metadata.loadavg
                            sig = signature(state, thunk)
                            lock(state.signature_time_cost) do stc
                                stc[sig] = (metadata.threadtime + get(stc, sig, UInt64(0))) ÷ 2
                            end
                            lock(state.signature_alloc_cost) do sac
                                sac[sig] = (metadata.gc_allocd + get(sac, sig, UInt64(0))) ÷ 2
                            end
                            if metadata.transfer_rate !== nothing
                                state.transfer_rate[] = (state.transfer_rate[] + metadata.transfer_rate) ÷ 2
                            end
                        end
                        if res isa Chunk
                            lock(state.equiv_chunks) do equiv_chunks
                                handle = res.handle::DRef
                                if !haskey(equiv_chunks, handle)
                                    equiv_chunks[handle] = res
                                end
                            end
                        end
                        store_result!(thunk, res; error=thunk_failed)
                        if thunk.options !== nothing && thunk.options.checkpoint !== nothing
                            try
                                @invokelatest thunk.options.checkpoint(thunk, res)
                            catch err
                                report_catch_error(err, "Thunk checkpoint failed")
                            end
                        end
                        @maybelog ctx timespan_start(ctx, :finish, (;uid=state.uid, thunk_id), (;thunk_id, result=res))
                        @lock state.lock finish_task!(ctx, state, thunk, thunk_failed)
                        @maybelog ctx timespan_finish(ctx, :finish, (;uid=state.uid, thunk_id), (;thunk_id, result=res))

                        # Allow result to be GC'd
                        thunk = nothing
                        tresult = nothing
                        res = nothing

                        try_schedule!()
                    finally
                        Threads.atomic_sub!(in_flight, 1)
                    end

                    safepoint(state)

                    no_tasks = isempty(state.ready) && lock(state.running_state) do running_state
                        isempty(running_state.running)
                    end && in_flight[] == 0
                    if no_tasks
                        signal_done!()
                        return
                    end
                end
            catch
                signal_done!()
                rethrow()
            end
        end
    end

    for t in worker_tasks
        wait(t)
    end

    # Final value is ready
    value = load_result(d)
    errored = has_error(d)
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

        # Notify any waiting tasks
        for (_, futures) in state.futures
            for future in futures
                put!(future, SchedulingException("Scheduler exited"); error=true)
            end
        end
        empty!(state.futures)

        # Notify and clean up all completion watchers
        lock(state.completion_watchers) do cw
            for (thunk, watchers) in cw
                thunk_id = thunk.id
                result = load_result(thunk)
                notification = TaskCompletionNotification(thunk_id, has_error(thunk), result)
                for chan in watchers
                    try
                        put!(chan, notification)
                    catch
                        # Not worth reporting this
                    end
                end
            end
            empty!(cw)
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
@reuse_scope function schedule!(ctx, state, sch_options, procs=procs_to_use(ctx, sch_options))
    safepoint(state)

    @assert length(procs) > 0

    # Remove processors that aren't yet initialized
    procs = filter(p -> haskey(state.worker_chans, Dagger.root_worker_id(p)), procs)

    # Reusable collections
    to_fire = @reusable_dict :schedule!_to_fire ScheduleTaskLocation Vector{ScheduleTaskSpec} ScheduleTaskLocation(OSProc(), OSProc()) ScheduleTaskSpec[] 1024
    to_fire_cleanup = @reuse_defer_cleanup empty!(to_fire)
    failed_scheduling = @reusable_vector :schedule!_failed_scheduling Union{Thunk,Nothing} nothing 32
    failed_scheduling_cleanup = @reuse_defer_cleanup empty!(failed_scheduling)

    # Select a new task and get its options
    task = nothing
    @label pop_task
    if task !== nothing
        @dagdebug task :schedule "Finished scheduling task"
        @maybelog ctx timespan_finish(ctx, :schedule, (;uid=state.uid, thunk_id=task.id), (;thunk_id=task.id))
    end
    result = Dagger.try_popfirst!(state.ready)
    result === nothing && @goto fire_tasks
    task = something(result)
    Dagger.is_task_local(task) || @goto pop_task
    thunk_state = lock(state.thunk_state) do thunk_states
        thunk_states[task]
    end
    try_transition!(thunk_state, THUNK_READY, THUNK_RUNNING) || @goto pop_task
    @dagdebug task :schedule "Scheduling task"
    @maybelog ctx timespan_start(ctx, :schedule, (;uid=state.uid, thunk_id=task.id), (;thunk_id=task.id))
    if has_result(task)
        if has_error(task)
            # An error was eagerly propagated to this task
            @dagdebug task :schedule "Task received upstream error, finishing"
            set_failed!(state, task)
        else
            # This shouldn't have happened
            @dagdebug task :schedule "Scheduling inconsistency: Task being scheduled is already cached!"
            iob = IOBuffer()
            println(iob, "Scheduling inconsistency: Task being scheduled is already cached!")
            println(iob, "  Task: $(task.id)")
            println(iob, "  Result: $(typeof(load_result(task)))")
            throw(SchedulingException(String(take!(iob))))
        end
        @goto pop_task
    end

    # Load task inputs
    collect_task_inputs!(state, task)

    # Calculate signature
    sig = signature(state, task)

    # Merge scheduler options and populate defaults
    options = task.options
    Dagger.options_merge!(options, sch_options)
    Dagger.populate_defaults!(options, sig)

    # Calculate scope
    if options.exec_scope !== nothing
        # Bypass scope calculation if it's been done for us already
        scope = options.exec_scope
        @goto scope_computed
    end
    scope = constrain(@something(options.compute_scope, options.scope, DefaultScope()),
                      @something(options.result_scope, AnyScope()))
    if scope isa InvalidScope
        @assert try_transition!(thunk_state, THUNK_RUNNING, THUNK_FINISHED)
        ex = SchedulingException("compute_scope and result_scope are not compatible: $(scope.x), $(scope.y)")
        set_failed!(state, task; ex)
        @goto pop_task
    end
    for arg in task.inputs
        value = unwrap_weak_checked(Dagger.value(arg))
        chunk = if istask(value)
            load_result(task)
        elseif value isa Chunk
            value
        else
            nothing
        end
        chunk isa Chunk || continue
        scope = constrain(scope, chunk.scope)
        if scope isa InvalidScope
            @assert try_transition!(thunk_state, THUNK_RUNNING, THUNK_FINISHED)
            ex = SchedulingException("Current scope and argument Chunk scope are not compatible: $(scope.x), $(scope.y)")
            set_failed!(state, task; ex)
            @goto pop_task
        end
    end
    @label scope_computed

    input_procs = @reusable_vector :schedule!_input_procs Processor OSProc() 32
    input_procs_cleanup = @reuse_defer_cleanup empty!(input_procs)
    for proc in Dagger.compatible_processors(scope, procs)
        if !(proc in input_procs)
            push!(input_procs, proc)
        end
    end

    sorted_procs = @reusable_vector :schedule!_sorted_procs Processor OSProc() 32
    sorted_procs_cleanup = @reuse_defer_cleanup empty!(sorted_procs)
    resize!(sorted_procs, length(input_procs))
    costs = @reusable_dict :schedule!_costs Processor Float64 OSProc() 0.0 32
    costs_cleanup = @reuse_defer_cleanup empty!(costs)
    estimate_task_costs!(sorted_procs, costs, state, input_procs, task; sig)
    input_procs_cleanup()
    scheduled = false

    for proc in sorted_procs
        gproc = get_parent(proc)
        can_use, scope = can_use_proc(state, task, gproc, proc, options, scope)
        if can_use
            has_cap, est_time_util, est_alloc_util, est_occupancy =
                has_capacity(state, proc, gproc.pid, options.time_util, options.alloc_util, options.occupancy, sig)
            if has_cap
                # Schedule task onto proc
                # FIXME: est_time_util = est_time_util isa MaxUtilization ? cap : est_time_util
                proc_tasks = get!(to_fire, ScheduleTaskLocation(gproc, proc)) do
                    #=FIXME:REALLOC_VEC=#
                    Vector{ScheduleTaskSpec}()
                end
                push!(proc_tasks, ScheduleTaskSpec(task, scope, est_time_util, est_alloc_util, est_occupancy))
                wtp = state.worker_time_pressure[gproc.pid]
                lock(wtp) do inner_wtp
                    inner_wtp[proc] = get(inner_wtp, proc, UInt64(0)) + est_time_util
                end
                @dagdebug task :schedule "Scheduling to $gproc -> $proc (cost: $(costs[proc]), pressure: $(get(wtp, proc, UInt64(0))))"
                sorted_procs_cleanup()
                costs_cleanup()
                @goto pop_task
            end
        end
    end

    ex = SchedulingException("No processors available, try widening scope")
    set_failed!(state, task; ex)
    @dagdebug task :schedule "No processors available, skipping"
    sorted_procs_cleanup()
    costs_cleanup()
    @goto pop_task

    # Fire all newly-scheduled tasks
    @label fire_tasks
    for (task_loc, task_spec) in to_fire
        fire_tasks!(ctx, task_loc, task_spec, state)
    end
    to_fire_cleanup()

    append!(state.ready, failed_scheduling)
    failed_scheduling_cleanup()
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
    delete!(state.worker_time_pressure, proc.pid)
    delete!(state.worker_storage_pressure, proc.pid)
    delete!(state.worker_storage_capacity, proc.pid)
    delete!(state.worker_loadavg, proc.pid)
    delete!(state.worker_chans, proc.pid)
end

function finish_task!(ctx, state, thunk, thunk_failed)
    @dagdebug thunk :finish "Finishing with $(thunk_failed ? "error" : "result")"
    thunk_state = lock(state.thunk_state) do thunk_states
        thunk_states[thunk]
    end
    thunk_state[] = THUNK_FINISHED
    if Dagger.is_task_local(thunk)
        lock(state.running_state) do running_state
            pop!(running_state.running, thunk)
            delete!(running_state.running_on, thunk)
        end
    end
    if thunk_failed
        set_failed!(state, thunk; ex=load_result(thunk))
    end
    schedule_dependents!(state, thunk, thunk_failed)
    fill_registered_futures!(state, thunk, thunk_failed)
    notify_completion_watchers!(state, thunk, thunk_failed)

    #to_evict = cleanup_syncdeps!(state, node)
    cleanup_syncdeps!(state, thunk)
    should_delete = lock(state.waiting_data) do wd
        if haskey(wd, thunk) && isempty(wd[thunk])
            delete!(wd, thunk)
        end
        return !haskey(wd, thunk)
    end
    if should_delete
        thunk.sch_accessible = false
        delete_unused_task!(state, thunk)
    end
    #evict_all_chunks!(ctx, to_evict)
end

function delete_unused_task!(state, thunk)
    if has_result(thunk) && !thunk.eager_accessible && !thunk.sch_accessible
        # Will not be accessed further, delete all cached data
        task_delete!(state, thunk)
        return true
    else
        return false
    end
end
function task_delete!(state, thunk)
    clear_result!(thunk)
    delete!(state.valid, thunk)
    delete!(state.thunk_state, thunk)
    delete!(state.thunk_dict, thunk.id)
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
    thunk_id::TaskID
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

@reuse_scope function fire_tasks!(ctx, task_loc::ScheduleTaskLocation, task_specs::Vector{ScheduleTaskSpec}, state)
    gproc, proc = task_loc.gproc, task_loc.proc
    to_send = @reusable_vector :fire_tasks!_to_send Union{TaskSpec,Nothing} nothing 1024
    to_send_cleanup = @reuse_defer_cleanup empty!(to_send)
    for task_spec in task_specs
        thunk = task_spec.task
        lock(state.running_state) do running_state
            push!(running_state.running, thunk)
            running_state.running_on[thunk] = gproc
        end
        if thunk.options.restore !== nothing
            try
                result = @invokelatest thunk.options.restore(thunk)
                if result isa Chunk
                    store_result!(thunk, result)
                    @lock state.lock finish_task!(ctx, state, thunk, false)
                    continue
                elseif result !== nothing
                    throw(ArgumentError("Invalid restore return type: $(typeof(result))"))
                end
            catch err
                report_catch_error(err, "Thunk restore failed")
            end
        end

        # Duplicate options and clear un-serializable fields
        options = copy(thunk.options)
        options.syncdeps = nothing

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
            #@reusable_tasks :fire_tasks!_task_cache 32 _->nothing "fire_tasks!" FireTaskSpec(proc, state.chan, to_send)
            fire_spec = FireTaskSpec(proc, state.chan, to_send)
            errormonitor_tracked("fire_tasks!", Threads.@spawn fire_spec())
        else
            # N.B. We don't batch these because we might get a deserialization
            # error due to something not being defined on the worker, and then we don't
            # know which task failed.
            for task_spec in to_send
                #@reusable_tasks :fire_tasks!_task_cache 32 _->nothing "fire_tasks!" FireTaskSpec(proc, state.chan, task_spec)
                fire_spec = FireTaskSpec(proc, state.chan, task_spec)
                errormonitor_tracked("fire_tasks!", Threads.@spawn fire_spec())
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
            if !walk_transfer_safe(err)
                @error "Error in task $thunk_id" exception=(err, bt)
                put!(chan, TaskResult(pid, proc, thunk_id, ErrorException("Error in task $thunk_id"), nothing))
            else
                put!(chan, TaskResult(pid, proc, thunk_id, CapturedException(err, bt), nothing))
            end
        else
            rethrow()
        end
    finally
        @maybelog ctx timespan_finish(ctx, :fire, (;uid, worker=pid), nothing)
    end
    return
end

struct ProcessorInternalState
    ctx::Context
    proc::Processor
    return_queue::RemoteChannel
    queue::LockedObject{PriorityQueue{TaskSpec, UInt32, Base.Order.ForwardOrdering}}
    reschedule::Base.Event
    tasks::Dict{TaskID,Task}
    task_specs::Dict{TaskID,TaskSpec}
    proc_occupancy::Base.RefValue{UInt32}
    time_pressure::Base.RefValue{UInt64}
    cancelled::Set{TaskID}
    cancel_tokens::Dict{TaskID,Dagger.CancelToken}
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
    ProcessorStateDict() = new(MemPool.ReadWriteLock(), Dict{Processor,ProcessorState}())
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
        state = f()::ProcessorState
        MemPool.lock(states.lock) do
            states.dict[proc] = state
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
                _, occupancy = peek(queue)
                if !proc_has_occupancy(proc_occupancy[], occupancy)
                    @dagdebug nothing :processor "Insufficient occupancy" proc_occupancy=proc_occupancy[] task_occupancy=occupancy
                    return nothing
                end
                queue_result = dequeue_pair!(queue)
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
                        task, occupancy = peek(queue)
                        scope = task.scope
                        if Dagger.proc_in_scope(to_proc, scope) &&
                           typemax(UInt32) - proc_occupancy_cached >= occupancy
                            # Compatible, steal this task
                            return dequeue_pair!(queue)
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

            # Set up cancellation and update task accounting
            cancel_token = Dagger.CancelToken()
            lock(istate.queue) do _
                istate.cancel_tokens[thunk_id] = cancel_token
                proc_occupancy[] += task_occupancy
                time_pressure[] += time_util
            end

            # Launch the task
            #=t = @reusable_tasks :start_processor_runner!_task_cache 32 t->begin
                tid = task_tid_for_processor(to_proc)
                if tid !== nothing
                    Dagger.set_task_tid!(t, tid)
                else
                    t.sticky = false
                end
            end "thunk $thunk_id" DoTaskSpec(to_proc, return_queue, task, cancel_token)=#
            do_spec = DoTaskSpec(to_proc, return_queue, task, cancel_token)
            t = @task do_spec()
            tid = task_tid_for_processor(to_proc)
            if tid !== nothing
                Dagger.set_task_tid!(t, tid)
            else
                t.sticky = false
            end
            errormonitor_tracked("thunk $thunk_id", schedule(t))

            # Update task accounting
            lock(istate.queue) do _
                tasks[thunk_id] = t
                istate.task_specs[thunk_id] = task
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
    end
    if was_cancelled
        # A result was already posted to the return queue
        return
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
        reschedule = Base.Event()
        istate = ProcessorInternalState(ctx, to_proc, return_queue,
                                        queue_locked, reschedule,
                                        Dict{TaskID,Task}(),
                                        Dict{TaskID,TaskSpec}(),
                                        Ref(UInt32(0)), Ref(UInt64(0)),
                                        Set{TaskID}(),
                                        Dict{TaskID,Dagger.CancelToken}(),
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
    lock(istate.queue) do queue
        for task in tasks
            thunk_id = task.thunk_id
            occupancy = task.est_occupancy
            @maybelog ctx timespan_start(ctx, :enqueue, (;uid, processor=to_proc, thunk_id), nothing)
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
            enqueue!(queue, task, occupancy)
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

    # Start counting time and GC allocations
    threadtime_start = cputhreadtime()
    # FIXME
    #gcnum_start = Base.gc_num()

    @dagdebug thunk_id :execute "Executing $(typeof(f))"

    logging_enabled = !(ctx.log_sink isa TimespanLogging.NoOpLog)

    result_meta = try
        # Set TLS variables
        Dagger.set_tls!((;
            sch_uid=task.sch_uid,
            sch_handle=task.sch_handle,
            processor=to_proc,
            task_spec=task,
            cancel_token=Dagger.DTASK_CANCEL_TOKEN[],
            logging_enabled,
        ))

        result = Dagger.with_options(propagated) do
            # Execute
            execute!(to_proc, f, fetched_args...; fetched_kwargs...)
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

    threadtime = cputhreadtime() - threadtime_start
    # FIXME: This is not a realistic measure of max. required memory
    #gc_allocd = min(max(UInt64(Base.gc_num().allocd) - UInt64(gcnum_start.allocd), UInt64(0)), UInt64(1024^4))
    @maybelog ctx timespan_finish(ctx, :compute, (;thunk_id, processor=to_proc), (;f, result=result_meta))

    lock(TASK_SYNC) do
        real_time_util[] -= est_time_util
        pop!(TASKS_RUNNING, thunk_id)
        notify(TASK_SYNC)
    end

    @dagdebug thunk_id :execute "Returning"

    # TODO: debug_storage("Releasing $to_storage_name")
    metadata = (
        time_pressure=real_time_util[],
        #storage_pressure=real_alloc_util,
        #storage_capacity=storage_cap,
        #loadavg=((Sys.loadavg()...,) ./ Sys.CPU_THREADS),
        threadtime=threadtime,
        # FIXME: Add runtime allocation tracking
        #gc_allocd=(isa(result_meta, Chunk) ? result_meta.handle.size : 0),
        gc_allocd=0,
        transfer_rate=(transfer_size[] > 0 && transfer_time[] > 0) ? round(UInt64, transfer_size[] / (transfer_time[] / 10^9)) : nothing,
    )
    return (result_meta, metadata)
end

end # module Sch
