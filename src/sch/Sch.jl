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
import ..Dagger: Context, Processor, Thunk, WeakThunk, ThunkFuture, DTaskFailedException, Chunk, WeakChunk, OSProc, AnyScope, DefaultScope, LockedObject, Argument, Signature
import ..Dagger: order, dependents, noffspring, istask, inputs, unwrap_weak_checked, wrap_weak, affinity, tochunk, timespan_start, timespan_finish, procs, move, chunktype, processor, get_processors, get_parent, execute!, rmprocs!, task_processor, constrain, cputhreadtime, maybe_take_or_alloc!
import ..Dagger: @dagdebug, @safe_lock_spin1, @maybelog, @take_or_alloc!
import DataStructures: PriorityQueue, enqueue!, dequeue_pair!, peek

import ..Dagger: ReusableCache, ReusableLinkedList, ReusableDict
import ..Dagger: @reusable, @reusable_dict, @reusable_vector, @reusable_tasks

import TimespanLogging

import TaskLocalValues: TaskLocalValue

const OneToMany = Dict{Thunk, Set{Thunk}}

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
- `waiting::OneToMany` - Map from downstream `Thunk` to upstream `Thunk`s that still need to execute
- `waiting_data::Dict{Union{Thunk,Chunk},Set{Thunk}}` - Map from input `Chunk`/upstream `Thunk` to all unfinished downstream `Thunk`s, to retain caches
- `ready::Vector{Thunk}` - The list of `Thunk`s that are ready to execute
- `cache::WeakKeyDict{Thunk, Any}` - Maps from a finished `Thunk` to it's cached result, often a DRef
- `valid::WeakKeyDict{Thunk, Nothing}` - Tracks all `Thunk`s that are in a valid scheduling state
- `running::Set{Thunk}` - The set of currently-running `Thunk`s
- `running_on::Dict{Thunk,OSProc}` - Map from `Thunk` to the OS process executing it
- `thunk_dict::Dict{Int, WeakThunk}` - Maps from thunk IDs to a `Thunk`
- `node_order::Any` - Function that returns the order of a thunk
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
- `chan::RemoteChannel{Channel{AnyTaskResult}}` - Channel for receiving completed thunks.
"""
struct ComputeState
    uid::UInt64
    waiting::OneToMany
    waiting_data::Dict{Union{Thunk,Chunk},Set{Thunk}}
    ready::Vector{Thunk}
    cache::WeakKeyDict{Thunk, Any}
    valid::WeakKeyDict{Thunk, Nothing}
    running::Set{Thunk}
    running_on::Dict{Thunk,OSProc}
    thunk_dict::Dict{Int, WeakThunk}
    node_order::Any
    worker_time_pressure::Dict{Int,Dict{Processor,UInt64}}
    worker_storage_pressure::Dict{Int,Dict{Union{StorageResource,Nothing},UInt64}}
    worker_storage_capacity::Dict{Int,Dict{Union{StorageResource,Nothing},UInt64}}
    worker_loadavg::Dict{Int,NTuple{3,Float64}}
    worker_chans::Dict{Int, Tuple{RemoteChannel,RemoteChannel}}
    signature_time_cost::Dict{Signature,UInt64}
    signature_alloc_cost::Dict{Signature,UInt64}
    transfer_rate::Ref{UInt64}
    halt::Base.Event
    lock::ReentrantLock
    futures::Dict{Thunk, Vector{ThunkFuture}}
    errored::WeakKeyDict{Thunk,Bool}
    thunks_to_delete::Set{Thunk}
    chan::RemoteChannel{Channel{AnyTaskResult}}
end

const UID_COUNTER = Threads.Atomic{UInt64}(1)

function start_state(deps::Dict, node_order, chan)
    state = ComputeState(Threads.atomic_add!(UID_COUNTER, UInt64(1)),
                         OneToMany(),
                         deps,
                         Vector{Thunk}(undef, 0),
                         WeakKeyDict{Thunk, Any}(),
                         WeakKeyDict{Thunk, Nothing}(),
                         Set{Thunk}(),
                         Dict{Thunk,OSProc}(),
                         Dict{Int, WeakThunk}(),
                         node_order,
                         Dict{Int,Dict{Processor,UInt64}}(),
                         Dict{Int,Dict{Union{StorageResource,Nothing},UInt64}}(),
                         Dict{Int,Dict{Union{StorageResource,Nothing},UInt64}}(),
                         Dict{Int,NTuple{3,Float64}}(),
                         Dict{Int, Tuple{RemoteChannel,RemoteChannel}}(),
                         Dict{Signature,UInt64}(),
                         Dict{Signature,UInt64}(),
                         Ref{UInt64}(1_000_000),
                         Base.Event(),
                         ReentrantLock(),
                         Dict{Thunk, Vector{ThunkFuture}}(),
                         WeakKeyDict{Thunk,Bool}(),
                         Set{Thunk}(),
                         chan)

    for k in sort(collect(keys(deps)), by=node_order)
        if istask(k)
            waiting = Set{Thunk}(Iterators.filter(istask, k.syncdeps))
            if isempty(waiting)
                push!(state.ready, k)
            else
                state.waiting[k] = waiting
            end
            state.valid[k] = nothing
        end
    end
    state
end

"""
    SchedulerOptions

Stores DAG-global options to be passed to the Dagger.Sch scheduler.

# Arguments
- `single::Int=0`: (Deprecated) Force all work onto worker with specified id.
  `0` disables this option.
- `proclist=nothing`: (Deprecated) Force scheduler to use one or more
  processors that are instances/subtypes of a contained type. Alternatively, a
  function can be supplied, and the function will be called with a processor as
  the sole argument and should return a `Bool` result to indicate whether or not
  to use the given processor. `nothing` enables all default processors.
- `allow_errors::Bool=true`: Allow thunks to error without affecting
  non-dependent thunks.
- `checkpoint=nothing`: If not `nothing`, uses the provided function to save
  the final result of the current scheduler invocation to persistent storage, for
  later retrieval by `restore`.
- `restore=nothing`: If not `nothing`, uses the provided function to return the
  (cached) final result of the current scheduler invocation, were it to execute.
  If this returns a `Chunk`, all thunks will be skipped, and the `Chunk` will be
  returned.  If `nothing` is returned, restoring is skipped, and the scheduler
  will execute as usual. If this function throws an error, restoring will be
  skipped, and the error will be displayed.
"""
Base.@kwdef struct SchedulerOptions
    single::Union{Int,Nothing} = nothing
    proclist = nothing
    allow_errors::Union{Bool,Nothing} = false
    checkpoint = nothing
    restore = nothing
end

"""
    ThunkOptions

Stores Thunk-local options to be passed to the Dagger.Sch scheduler.

# Arguments
- `single::Int=0`: (Deprecated) Force thunk onto worker with specified id. `0`
  disables this option.
- `proclist=nothing`: (Deprecated) Force thunk to use one or more processors
  that are instances/subtypes of a contained type. Alternatively, a function can
  be supplied, and the function will be called with a processor as the sole
  argument and should return a `Bool` result to indicate whether or not to use
  the given processor. `nothing` enables all default processors.
- `time_util::Dict{Type,Any}`: Indicates the maximum expected time utilization
  for this thunk. Each keypair maps a processor type to the utilization, where
  the value can be a real (approximately the number of nanoseconds taken), or
  `MaxUtilization()` (utilizes all processors of this type). By default, the
  scheduler assumes that this thunk only uses one processor.
- `alloc_util::Dict{Type,UInt64}`: Indicates the maximum expected memory
  utilization for this thunk. Each keypair maps a processor type to the
  utilization, where the value is an integer representing approximately the
  maximum number of bytes allocated at any one time.
- `occupancy::Dict{Type,Real}`: Indicates the maximum expected processor
  occupancy for this thunk. Each keypair maps a processor type to the
  utilization, where the value can be a real between 0 and 1 (the occupancy
  ratio, where 1 is full occupancy). By default, the scheduler assumes that this
  thunk has full occupancy.
- `allow_errors::Bool=true`: Allow this thunk to error without affecting
  non-dependent thunks.
- `checkpoint=nothing`: If not `nothing`, uses the provided function to save
  the result of the thunk to persistent storage, for later retrieval by
  `restore`.
- `restore=nothing`: If not `nothing`, uses the provided function to return the
  (cached) result of this thunk, were it to execute.  If this returns a `Chunk`,
  this thunk will be skipped, and its result will be set to the `Chunk`.  If
  `nothing` is returned, restoring is skipped, and the thunk will execute as
  usual. If this function throws an error, restoring will be skipped, and the
  error will be displayed.
- `storage::Union{Chunk,Nothing}=nothing`: If not `nothing`, references a
  `MemPool.StorageDevice` which will be passed to `MemPool.poolset` internally
  when constructing `Chunk`s (such as when constructing the return value). The
  device must support `MemPool.CPURAMResource`. When `nothing`, uses
  `MemPool.GLOBAL_DEVICE[]`.
- `storage_root_tag::Any=nothing`: If not `nothing`,
  specifies the MemPool storage leaf tag to associate with the thunk's result.
  This tag can be used by MemPool's storage devices to manipulate their behavior,
  such as the file name used to store data on disk."
- `storage_leaf_tag::MemPool.Tag,Nothing}=nothing`: If not `nothing`,
  specifies the MemPool storage leaf tag to associate with the thunk's result.
  This tag can be used by MemPool's storage devices to manipulate their behavior,
  such as the file name used to store data on disk."
- `storage_retain::Bool=false`: The value of `retain` to pass to
  `MemPool.poolset` when constructing the result `Chunk`.
"""
Base.@kwdef struct ThunkOptions
    single::Union{Int,Nothing} = nothing
    proclist = nothing
    time_util::Union{Dict{Type,Any},Nothing} = nothing
    alloc_util::Union{Dict{Type,UInt64},Nothing} = nothing
    occupancy::Union{Dict{Type,Real},Nothing} = nothing
    allow_errors::Union{Bool,Nothing} = nothing
    checkpoint = nothing
    restore = nothing
    storage::Union{Chunk,Nothing} = nothing
    storage_root_tag = nothing
    storage_leaf_tag::Union{MemPool.Tag,Nothing} = nothing
    storage_retain::Bool = false
end

"""
    Base.merge(sopts::SchedulerOptions, topts::ThunkOptions) -> ThunkOptions

Combine `SchedulerOptions` and `ThunkOptions` into a new `ThunkOptions`.
"""
function Base.merge(sopts::SchedulerOptions, topts::ThunkOptions)
    select_option = (sopt, topt) -> isnothing(topt) ? sopt : topt

    single = select_option(sopts.single, topts.single)
    allow_errors = select_option(sopts.allow_errors, topts.allow_errors)
    proclist = select_option(sopts.proclist, topts.proclist)
    ThunkOptions(single,
                 proclist,
                 topts.time_util,
                 topts.alloc_util,
                 topts.occupancy,
                 allow_errors,
                 topts.checkpoint,
                 topts.restore,
                 topts.storage,
                 topts.storage_root_tag,
                 topts.storage_leaf_tag,
                 topts.storage_retain)
end
Base.merge(sopts::SchedulerOptions, ::Nothing) =
    ThunkOptions(sopts.single,
                 sopts.proclist,
                 nothing,
                 nothing,
                 sopts.allow_errors)
"""
    populate_defaults(opts::ThunkOptions, sig::Signature) -> ThunkOptions

Returns a `ThunkOptions` with default values filled in for a function with type
signature `sig`, if the option was previously unspecified in `opts`.
"""
function populate_defaults(opts::ThunkOptions, sig::Signature)
    ThunkOptions(
        maybe_default(opts, Val{:single}(), sig),
        maybe_default(opts, Val{:proclist}(), sig),
        maybe_default(opts, Val{:time_util}(), sig),
        maybe_default(opts, Val{:alloc_util}(), sig),
        maybe_default(opts, Val{:occupancy}(), sig),
        maybe_default(opts, Val{:allow_errors}(), sig),
        maybe_default(opts, Val{:checkpoint}(), sig),
        maybe_default(opts, Val{:restore}(), sig),
        maybe_default(opts, Val{:storage}(), sig),
        maybe_default(opts, Val{:storage_root_tag}(), sig),
        maybe_default(opts, Val{:storage_leaf_tag}(), sig),
        maybe_default(opts, Val{:storage_retain}(), sig),
    )
end
function maybe_default(opts, ::Val{opt}, sig::Signature) where opt
    old_opt = getfield(opts, opt)
    if old_opt !== nothing
        return old_opt
    else
        @warn "SIGNATURE_DEFAULT_CACHE should use an LRU" maxlog=1
        return get!(SIGNATURE_DEFAULT_CACHE[], (sig.hash_nokw, opt)) do
            Dagger.default_option(Val{opt}(), sig.sig_nokw...)
        end
    end
end
const SIGNATURE_DEFAULT_CACHE = TaskLocalValue{Dict{Tuple{UInt,Symbol},Any}}(()->Dict{Tuple{UInt,Symbol},Any}())

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
        state.worker_time_pressure[p.pid] = Dict{Processor,UInt64}()

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
    proc_states(uid) do states
        for (proc, state) in states
            istate = state.state
            istate.done[] = true
            notify(istate.reschedule)
        end
        empty!(states)
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

function compute_dag(ctx, d::Thunk; options=SchedulerOptions())
    if options === nothing
        options = SchedulerOptions()
    end
    ctx.options = options
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

function scheduler_init(ctx, state::ComputeState, d::Thunk, options, deps)
    # setup thunk_dict mappings
    for node in filter(istask, keys(deps))
        state.thunk_dict[node.id] = WeakThunk(node)
        for dep in deps[node]
            state.thunk_dict[dep.id] = WeakThunk(dep)
        end
    end

    # Initialize workers
    @sync for p in procs_to_use(ctx)
        Threads.@spawn begin
            try
                init_proc(state, p, ctx.log_sink)
            catch err
                @error "Error initializing worker $p" exception=(err,catch_backtrace())
                remove_dead_proc!(ctx, state, p)
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
            monitor_procs_changed!(ctx, state)
        catch err
            @error "Error assigning workers" exception=(err,catch_backtrace())
        end
    end
end

function scheduler_run(ctx, state::ComputeState, d::Thunk, options)
    @dagdebug nothing :global "Initializing scheduler" uid=state.uid

    safepoint(state)

    # Loop while we still have thunks to execute
    while !isempty(state.ready) || !isempty(state.running)
        if !isempty(state.ready)
            # Nothing running, so schedule up to N thunks, 1 per N workers
            @invokelatest schedule!(ctx, state)
        end

        check_integrity(ctx)

        isempty(state.running) && continue
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
        gproc = OSProc(pid)
        safepoint(state)
        lock(state.lock) do
            thunk_failed = false
            if res isa Exception
                if unwrap_nested_exception(res) isa ProcessExitedException
                    @warn "Worker $(pid) died, rescheduling work"

                    # Remove dead worker from procs list
                    @maybelog ctx timespan_start(ctx, :remove_procs, (;uid=state.uid, worker=pid), nothing)
                    remove_dead_proc!(ctx, state, gproc)
                    @maybelog ctx timespan_finish(ctx, :remove_procs, (;uid=state.uid, worker=pid), nothing)

                    @maybelog ctx timespan_start(ctx, :handle_fault, (;uid=state.uid, worker=pid), nothing)
                    handle_fault(ctx, state, gproc)
                    @maybelog ctx timespan_finish(ctx, :handle_fault, (;uid=state.uid, worker=pid), nothing)
                    return # effectively `continue`
                else
                    if something(ctx.options.allow_errors, false) ||
                       something(unwrap_weak_checked(state.thunk_dict[thunk_id]).options.allow_errors, false)
                        thunk_failed = true
                    else
                        throw(res)
                    end
                end
            end
            node = unwrap_weak_checked(state.thunk_dict[thunk_id])::Thunk
            metadata = tresult.metadata
            if metadata !== nothing
                state.worker_time_pressure[pid][proc] = metadata.time_pressure
                #to_storage = fetch(node.options.storage)
                #state.worker_storage_pressure[pid][to_storage] = metadata.storage_pressure
                #state.worker_storage_capacity[pid][to_storage] = metadata.storage_capacity
                #state.worker_loadavg[pid] = metadata.loadavg
                sig = signature(state, node)
                state.signature_time_cost[sig] = (metadata.threadtime + get(state.signature_time_cost, sig, 0)) ÷ 2
                state.signature_alloc_cost[sig] = (metadata.gc_allocd + get(state.signature_alloc_cost, sig, 0)) ÷ 2
                if metadata.transfer_rate !== nothing
                    state.transfer_rate[] = (state.transfer_rate[] + metadata.transfer_rate) ÷ 2
                end
            end
            store_result!(state, node, res; error=thunk_failed)
            if node.options !== nothing && node.options.checkpoint !== nothing
                try
                    @invokelatest node.options.checkpoint(node, res)
                catch err
                    report_catch_error(err, "Thunk checkpoint failed")
                end
            end

            @maybelog ctx timespan_start(ctx, :finish, (;uid=state.uid, thunk_id), (;thunk_id, result=res))
            finish_task!(ctx, state, node, thunk_failed)
            @maybelog ctx timespan_finish(ctx, :finish, (;uid=state.uid, thunk_id), (;thunk_id, result=res))
        end

        # Allow data to be GC'd
        tresult = nothing
        res = nothing

        safepoint(state)
    end

    # Final value is ready
    value = load_result(state, d)
    errored = get(state.errored, d, false)
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
function scheduler_exit(ctx, state::ComputeState, options)
    @dagdebug nothing :global "Tearing down scheduler" uid=state.uid

    @sync for p in procs_to_use(ctx)
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
    end

    # Let the context procs handler clean itself up
    lock(ctx.proc_notify) do
        notify(ctx.proc_notify)
    end

    @dagdebug nothing :global "Tore down scheduler" uid=state.uid
end

function procs_to_use(ctx, options=ctx.options)
    return if options.single !== nothing
        @assert options.single in vcat(1, workers()) "Sch option `single` must specify an active worker ID."
        OSProc[OSProc(options.single)]
    else
        procs(ctx)
    end
end

check_integrity(ctx) = @assert !isempty(procs_to_use(ctx)) "No suitable workers available in context."

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
function schedule!(ctx, state, procs=procs_to_use(ctx))
    lock(state.lock) do
        safepoint(state)
        @assert length(procs) > 0

        # Remove processors that aren't yet initialized
        procs = filter(p -> haskey(state.worker_chans, Dagger.root_worker_id(p)), procs)

        # Schedule tasks
        to_fire = @reusable_dict :schedule!_to_fire ScheduleTaskLocation Vector{ScheduleTaskSpec} ScheduleTaskLocation(OSProc(), OSProc()) ScheduleTaskSpec[] 1024
        failed_scheduling = @reusable_vector :schedule!_failed_scheduling Union{Thunk,Nothing} nothing 32

        # Select a new task and get its options
        task = nothing
        @label pop_task
        if task !== nothing
            @maybelog ctx timespan_finish(ctx, :schedule, (;uid=state.uid, thunk_id=task.id), (;thunk_id=task.id))
        end
        if isempty(state.ready)
            @goto fire_tasks
        end
        task = pop!(state.ready)
        @maybelog ctx timespan_start(ctx, :schedule, (;uid=state.uid, thunk_id=task.id), (;thunk_id=task.id))
        if has_result(state, task)
            if haskey(state.errored, task)
                # An error was eagerly propagated to this task
                finish_failed!(state, task)
            else
                # This shouldn't have happened
                iob = IOBuffer()
                println(iob, "Scheduling inconsistency: Task being scheduled is already cached!")
                println(iob, "  Task: $(task.id)")
                println(iob, "  Cache Entry: $(typeof(something(task.cache_ref)))")
                ex = SchedulingException(String(take!(iob)))
                store_result!(state, task, ex; error=true)
            end
            @goto pop_task
        end

        # Load task inputs
        collect_task_inputs!(state, task)

        # Calculate signature
        sig = signature(state, task)

        # Merge options and fill defaults
        opts = merge(ctx.options, task.options)
        opts = populate_defaults(opts, sig)

        # Calculate scope
        f = Dagger.value(task.inputs[1])
        scope = if f isa Chunk
            f.scope
        else
            if task.options.proclist !== nothing
                # proclist overrides scope selection
                AnyScope()
            else
                DefaultScope()
            end
        end
        for arg in task.inputs
            value = unwrap_weak_checked(Dagger.value(arg))
            chunk = value isa Chunk ? value : nothing
            chunk isa Chunk || continue
            scope = constrain(scope, chunk.scope)
            if scope isa Dagger.InvalidScope
                ex = SchedulingException("Scopes are not compatible: $(scope.x), $(scope.y)")
                store_result!(state, task, ex; error=true)
                set_failed!(state, task)
                @goto pop_task
            end
        end

        # FIXME: Use compatible_processors
        input_procs = @reusable_vector :schedule!_input_procs Processor OSProc() 32
        for gp in procs
            subprocs = get_processors(gp)
            for proc in subprocs
                if !(proc in input_procs)
                    push!(input_procs, proc)
                end
            end
        end

        sorted_procs = @reusable_vector :schedule!_sorted_procs Processor OSProc() 32
        resize!(sorted_procs, length(input_procs))
        costs = @reusable_dict :schedule!_costs Processor Float64 OSProc() 0.0 32
        estimate_task_costs!(sorted_procs, costs, state, input_procs, task)
        empty!(costs) # We don't use costs here
        empty!(input_procs)
        scheduled = false

        # Move our corresponding ThreadProc to be the last considered
        if length(sorted_procs) > 1
            sch_threadproc = Dagger.ThreadProc(myid(), Threads.threadid())
            sch_thread_idx = findfirst(proc->proc==sch_threadproc, sorted_procs)
            if sch_thread_idx !== nothing
                deleteat!(sorted_procs, sch_thread_idx)
                push!(sorted_procs, sch_threadproc)
            end
        end

        for proc in sorted_procs
            gproc = get_parent(proc)
            can_use, scope = can_use_proc(state, task, gproc, proc, opts, scope)
            if can_use
                has_cap, est_time_util, est_alloc_util, est_occupancy =
                    has_capacity(state, proc, gproc.pid, opts.time_util, opts.alloc_util, opts.occupancy, sig)
                if has_cap
                    # Schedule task onto proc
                    # FIXME: est_time_util = est_time_util isa MaxUtilization ? cap : est_time_util
                    proc_tasks = get!(to_fire, ScheduleTaskLocation(gproc, proc)) do
                        #=FIXME:REALLOC_VEC=#
                        Vector{ScheduleTaskSpec}()
                    end
                    push!(proc_tasks, ScheduleTaskSpec(task, scope, est_time_util, est_alloc_util, est_occupancy))
                    state.worker_time_pressure[gproc.pid][proc] =
                        get(state.worker_time_pressure[gproc.pid], proc, 0) +
                        est_time_util
                    @dagdebug task :schedule "Scheduling to $gproc -> $proc"
                    empty!(sorted_procs)
                    @goto pop_task
                end
            end
        end
        ex = SchedulingException("No processors available, try widening scope")
        store_result!(state, task, ex; error=true)
        set_failed!(state, task)
        empty!(sorted_procs)
        @goto pop_task

        # Fire all newly-scheduled tasks
        @label fire_tasks
        for (task_loc, task_spec) in to_fire
            fire_tasks!(ctx, task_loc, task_spec, state)
        end
        empty!(to_fire)

        append!(state.ready, failed_scheduling)
        empty!(failed_scheduling)
    end
end

"""
Monitors for workers being added/removed to/from `ctx`, sets up or tears down
per-worker state, and notifies the scheduler so that work can be reassigned.
"""
function monitor_procs_changed!(ctx, state)
    # Load current set of procs
    old_ps = procs_to_use(ctx)

    while !state.halt.set
        # Wait for the notification that procs have changed
        lock(ctx.proc_notify) do
            wait(ctx.proc_notify)
        end

        @maybelog ctx timespan_start(ctx, :assign_procs, (;uid=state.uid), nothing)

        # Load new set of procs
        new_ps = procs_to_use(ctx)

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

function remove_dead_proc!(ctx, state, proc, options=ctx.options)
    @assert options.single !== proc.pid "Single worker failed, cannot continue."
    rmprocs!(ctx, [proc])
    delete!(state.worker_time_pressure, proc.pid)
    delete!(state.worker_storage_pressure, proc.pid)
    delete!(state.worker_storage_capacity, proc.pid)
    delete!(state.worker_loadavg, proc.pid)
    delete!(state.worker_chans, proc.pid)
end

function finish_task!(ctx, state, node, thunk_failed)
    pop!(state.running, node)
    delete!(state.running_on, node)
    if thunk_failed
        set_failed!(state, node)
    end
    schedule_dependents!(state, node, thunk_failed)
    fill_registered_futures!(state, node, thunk_failed)

    #=
    to_evict = cleanup_syncdeps!(state, node)
    if node.f isa Chunk
        # FIXME: Check the graph for matching chunks
        #push!(to_evict, node.f)
    end
    =#
    cleanup_syncdeps!(state, node)
    if haskey(state.waiting_data, node) && isempty(state.waiting_data[node])
        delete!(state.waiting_data, node)
    end
    if !haskey(state.waiting_data, node)
        node.sch_accessible = false
        delete_unused_task!(state, node)
    end
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
    delete!(state.valid, thunk)
    delete!(state.thunk_dict, thunk.id)
end

function evict_all_chunks!(ctx, to_evict)
    if !isempty(to_evict)
        @sync for w in map(p->p.pid, procs_to_use(ctx))
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
    # TODO: Get these from options
    get_result::Bool
    persist::Bool
    cache::Bool
    meta::Bool
    options#::Options
    propagated::NamedTuple
    ctx_vars::NamedTuple
    sch_handle::SchedulerHandle
    sch_uid::UInt64
end
Base.hash(task::TaskSpec, h::UInt) = hash(task.thunk_id, hash(TaskSpec, h))

function fire_tasks!(ctx, task_loc::ScheduleTaskLocation, task_specs::Vector{ScheduleTaskSpec}, state)
    gproc, proc = task_loc.gproc, task_loc.proc
    to_send = @reusable_vector :fire_tasks!_to_send Union{TaskSpec,Nothing} nothing 1024
    for task_spec in task_specs
        thunk = task_spec.task
        push!(state.running, thunk)
        state.running_on[thunk] = gproc
        if has_result(state, thunk)
            # the result is already cached
            thunk_failed = get(state.errored, thunk, false)
            finish_task!(ctx, state, thunk, thunk_failed)
            continue
        end
        if thunk.options !== nothing && thunk.options.restore !== nothing
            try
                result = @invokelatest thunk.options.restore(thunk)
                if result isa Chunk
                    store_result!(state, thunk, result)
                    finish_task!(ctx, state, thunk, false)
                    continue
                elseif result !== nothing
                    throw(ArgumentError("Invalid restore return type: $(typeof(result))"))
                end
            catch err
                report_catch_error(err, "Thunk restore failed")
            end
        end

        args = map(copy, thunk.inputs)
        for arg in args
            # Unwrap any weak arguments
            # TODO: Only for non-delayed: @assert Dagger.isweak(Dagger.value(arg)) "Non-weak argument: $(arg)"
            arg.value = unwrap_weak_checked(Dagger.value(arg))
        end
        Tf = chunktype(first(args))

        toptions = thunk.options !== nothing ? thunk.options : ThunkOptions()
        options = merge(ctx.options, toptions)
        propagated = get_propagated_options(thunk)
        @assert (options.single === nothing) || (gproc.pid == options.single)
        # TODO: Set `sch_handle.tid.ref` to the right `DRef`
        sch_handle = SchedulerHandle(ThunkID(thunk.id, nothing), state.worker_chans[gproc.pid]...)

        # TODO: De-dup common fields (log_sink, uid, etc.)
        push!(to_send, TaskSpec(
            thunk.id,
            task_spec.est_time_util, task_spec.est_alloc_util, task_spec.est_occupancy,
            task_spec.scope, Tf, args,
            thunk.get_result, thunk.persist, thunk.cache, thunk.meta, options,
            propagated,
            (log_sink=ctx.log_sink, profile=ctx.profile),
            sch_handle, state.uid))
    end

    # N.B. We don't batch these because we might get a deserialization
    # error due to something not being defined on the worker, and then we don't
    # know which task failed.
    for (idx, task_spec) in enumerate(to_send)
        @reusable_tasks :fire_tasks!_task_cache 32 _->nothing "fire_tasks!" FireTaskSpec(proc, state.chan, task_spec)
    end
    empty!(to_send)
end

struct FireTaskSpec
    init_proc::Processor
    return_chan::RemoteChannel
    task::TaskSpec
end
function (ets::FireTaskSpec)()
    task = ets.task
    ctx_vars = task.ctx_vars
    ctx = Context(Processor[]; log_sink=ctx_vars.log_sink, profile=ctx_vars.profile)
    uid = task.sch_uid

    proc = ets.init_proc
    chan = ets.return_chan
    pid = Dagger.root_worker_id(proc)

    @maybelog ctx timespan_start(ctx, :fire, (;uid, worker=pid), nothing)
    try
        remotecall_wait(do_tasks, pid, proc, chan, [task]);
    catch err
        bt = catch_backtrace()
        thunk_id = task.thunk_id
        put!(chan, TaskResult(pid, proc, thunk_id, CapturedException(err, bt), nothing))
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

const PROCESSOR_TASK_STATE = LockedObject(Dict{UInt64,Dict{Processor,ProcessorState}}())

function proc_states(f::Base.Callable, uid::UInt64)
    lock(PROCESSOR_TASK_STATE) do all_states
        if !haskey(all_states, uid)
            all_states[uid] = Dict{Processor,ProcessorState}()
        end
        our_states = all_states[uid]
        return f(our_states)
    end
end
proc_states(f::Base.Callable) =
    proc_states(f, Dagger.get_tls().sch_uid)

task_tid_for_processor(::Processor) = nothing
task_tid_for_processor(proc::Dagger.ThreadProc) = proc.tid

stealing_permitted(::Processor) = true
stealing_permitted(proc::Dagger.ThreadProc) = proc.owner != 1 || proc.tid != 1

proc_has_occupancy(proc_occupancy, task_occupancy) =
    UInt64(task_occupancy) + UInt64(proc_occupancy) <= typemax(UInt32)

function start_processor_runner!(istate::ProcessorInternalState, uid::UInt64, return_queue::RemoteChannel)
    to_proc = istate.proc
    proc_run_task = @task begin
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
                states = proc_states(all_states->collect(values(all_states)), uid)
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
                        if !isa(constrain(scope, Dagger.ExactScope(to_proc)),
                                Dagger.InvalidScope) &&
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
            t = @reusable_tasks :start_processor_runner!_task_cache 32 t->begin
                tid = task_tid_for_processor(to_proc)
                if tid !== nothing
                    Dagger.set_task_tid!(t, tid)
                else
                    t.sticky = false
                end
            end "thunk $thunk_id" DoTaskSpec(to_proc, return_queue, task, cancel_token)

            # Update task accounting
            # FIXME: This can race with the task
            @warn "Fix this race" maxlog=1
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
        istate = proc_states(task.sch_uid) do states
            states[to_proc].state
        end
        lock(istate.queue) do _
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
        end
        notify(istate.reschedule)

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
            rethrow(err)
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
    state = proc_states(uid) do states
        get!(states, to_proc) do
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
            runner = start_processor_runner!(istate, uid, return_queue)
            @static if VERSION < v"1.9"
                reschedule.waiter = runner
            end
            return ProcessorState(istate, runner)
        end
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
    states = collect(proc_states(values, uid))
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
function do_task(to_proc, task::TaskSpec)
    thunk_id = task.thunk_id

    ctx_vars = task.ctx_vars
    ctx = Context(Processor[]; log_sink=ctx_vars.log_sink, profile=ctx_vars.profile)

    from_proc = OSProc()
    #Tdata = Any[]
    data = task.data
    #for arg in data
    #    push!(Tdata, chunktype(Dagger.value(arg)))
    #end
    Tf = task.Tf
    f = isdefined(Tf, :instance) ? Tf.instance : nothing

    # Wait for required resources to become available
    options = task.options
    to_storage = options.storage !== nothing ? fetch(options.storage) : MemPool.GLOBAL_DEVICE[]
    #to_storage_name = nameof(typeof(to_storage))
    #storage_cap = storage_capacity(to_storage)

    @maybelog ctx timespan_start(ctx, :storage_wait, (;thunk_id, processor=to_proc), (;f, device=typeof(to_storage)))
    est_time_util = task.est_time_util
    est_alloc_util = task.est_alloc_util
    real_time_util = Ref{UInt64}(0)
    real_alloc_util = UInt64(0)
    #= TODO
    if !meta
        # Factor in the memory costs for our lazy arguments
        for arg in data[2:end]
            if Dagger.valuetype(arg) <: Chunk
                est_alloc_util += Dagger.value(arg).handle.size
            end
        end
    end
    =#

    #= FIXME: Serialize on over-memory situation
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
    _data = if task.meta
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
    fetched_kwargs = @reusable_vector :do_task_fetched_kwargs Pair{Symbol,Any} :NULL=>nothing 32
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

        result = Dagger.with_options(task.propagated) do
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
        result_meta = if task.get_result || task.meta
            result
        else
            # TODO: We should cache this locally
            cache = task.persist || task.cache
            tochunk(result, to_proc; device, task.persist, cache,
                    tag=options.storage_root_tag,
                    leaf_tag=something(options.storage_leaf_tag, MemPool.Tag()),
                    retain=options.storage_retain)
        end
    catch ex
        bt = catch_backtrace()
        RemoteException(myid(), CapturedException(ex, bt))
    finally
        empty!(fetched_args)
        empty!(fetched_kwargs)
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
