module Sch

using Distributed
import MemPool
import MemPool: DRef, StorageResource
import MemPool: poolset, storage_available, storage_capacity, storage_utilized, externally_varying
import Statistics: mean
import Random: randperm
import Base: @invokelatest
using ScopedValues
using TaskLocalValues

import ..Dagger
import ..Dagger: Context, Processor, ThunkID, Thunk, ThunkRef, WeakThunk, ThunkFuture, ThunkFailedException, Chunk, WeakChunk, OSProc, AnyScope, DefaultScope, LockedObject
import ..Dagger: order, dependents, noffspring, istask, inputs, unwrap_weak_checked, affinity, tochunk, timespan_start, timespan_finish, procs, move, chunktype, processor, default_enabled, get_processors, get_parent, execute!, rmprocs!, addprocs!, thunk_processor, constrain, cputhreadtime
import ..Dagger: @dagdebug, @lock1, @safe_lock_spin1
import DataStructures: PriorityQueue, enqueue!, dequeue_pair!, peek
using MultiThreadedCaches

import ..Dagger

# Any referencable thunk
const AnyThunk = Union{Thunk, ThunkRef}

# Any referencable data
const AnyDataRef = Union{AnyThunk, Chunk}

# A function signature
const Signature = Vector{Any}

include("util.jl")
include("fault-handler.jl")
include("dynamic.jl")

include("metrics.jl")
include("analysis.jl")
include("aggregate.jl")
include("decision.jl")

"""
    ComputeState

The internal state-holding struct of the scheduler.

Fields:
- `uid::UInt64` - Unique identifier for this scheduler instance
- `waiting::Dict{Thunk, Set{AnyThunk}}` - Map from downstream `Thunk` to upstream `Thunk`s that still need to execute
- `waiting_data::Dict{AnyDataRef, Set{AnyThunk}}` - Map from input `Chunk`/upstream `Thunk` to all unfinished downstream `Thunk`s, to retain caches
- `cache::WeakKeyDict{Thunk, Any}` - Maps from a finished `Thunk` to it's cached result, often a `Chunk`.
- `errored::WeakKeyDict{Thunk,Bool}` - Indicates if a thunk's result is an error.
- `valid::WeakKeyDict{Thunk, Nothing}` - Tracks all `Thunk`s that are in a valid scheduling state
- `waiting_remote::Dict{ThunkRef, Set{Thunk}}` - The set of all local thunks utilizing the key (a remote thunk).
- `cache_remote::Dict{ThunkRef, Any}` - Maps from a remote finished `Thunk` to it's cached result, often a `Chunk`.
- `errored_remote::Dict{ThunkRef, Bool}` - Indicates if a remote thunk's result is an error.
- `ready::Vector{Thunk}` - The list of `Thunk`s that are ready to execute
- `running::Set{Thunk}` - The set of currently-running `Thunk`s
- `running_on::Dict{Thunk,OSProc}` - Map from `Thunk` to the OS process executing it
- `thunk_dict::Dict{ThunkID, WeakThunk}` - Maps from thunk IDs to a `Thunk`
- `thunks_to_delete::Set{Thunk}` - The list of `Thunk`s ready to be deleted upon completion.
- `node_order::Any` - Function that returns the order of a thunk
- `worker_chans::Dict{Int, Tuple{RemoteChannel,RemoteChannel}}` - Communication channels between the scheduler and each worker
- `metrics::MetricsCacheLocked` - For a given (context, operation) pair, for a given object (:global, worker, processor, task signature), the values of the metric
- `halt::Base.Event` - Event indicating that the scheduler is halting
- `lock::ReentrantLock` - Lock around operations which modify the state
- `futures::Dict{Thunk, Vector{ThunkFuture}}` - Futures registered for waiting on the result of a thunk.
- `chan::RemoteChannel{Channel{Any}}` - Channel for receiving completed thunks.
- `schedule_model::AbstractDecision` - The model to use for all configurable decisions.
"""
struct ComputeState
    uid::UInt64
    waiting::Dict{Thunk, Set{AnyThunk}}
    waiting_data::Dict{AnyDataRef, Set{AnyThunk}}
    cache::WeakKeyDict{Thunk, Any}
    errored::WeakKeyDict{Thunk, Bool}
    valid::WeakKeyDict{Thunk, Nothing}
    waiting_remote::Dict{ThunkRef, Set{Thunk}}
    cache_remote::Dict{ThunkRef, Any}
    errored_remote::Dict{ThunkRef, Bool}
    ready::Vector{Thunk}
    running::Set{Thunk}
    running_on::Dict{Thunk,OSProc}
    thunk_dict::Dict{ThunkID, WeakThunk}
    thunks_to_delete::Set{Thunk}
    node_order::Any
    worker_chans::Dict{Int, Tuple{RemoteChannel,RemoteChannel}}
    metrics::MetricsCacheLocked
    halt::Base.Event
    lock::ReentrantLock
    futures::Dict{Thunk, Vector{ThunkFuture}}
    chan::RemoteChannel{Channel{Any}}
    schedule_model::AbstractDecision
end

const UID_COUNTER = Threads.Atomic{UInt64}(1)

function start_state(deps::Dict, node_order, chan, options)
    state = ComputeState(Threads.atomic_add!(UID_COUNTER, UInt64(1)),
                         Dict{Thunk, Set{AnyThunk}}(),
                         deps,
                         WeakKeyDict{Thunk, Any}(),
                         WeakKeyDict{Thunk,Bool}(),
                         WeakKeyDict{Thunk, Nothing}(),
                         Dict{ThunkRef, Set{Thunk}}(),
                         Dict{ThunkRef, Any}(),
                         Dict{ThunkRef, Bool}(),
                         Vector{Thunk}(undef, 0),
                         Set{Thunk}(),
                         Dict{Thunk,OSProc}(),
                         Dict{ThunkID, WeakThunk}(),
                         Set{Thunk}(),
                         node_order,
                         Dict{Int, Tuple{RemoteChannel,RemoteChannel}}(),
                         create_global_metrics_cache(),
                         Base.Event(),
                         ReentrantLock(),
                         Dict{Thunk, Vector{ThunkFuture}}(),
                         chan,
                         options.schedule_model)

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
- `allow_errors::Union{Bool,Nothing}=nothing`: Allow thunks to error without
affecting non-dependent thunks. Defaults to `true`.
- `checkpoint=nothing`: If not `nothing`, uses the provided function to save
the final result of the current scheduler invocation to persistent storage, for
later retrieval by `restore`.
- `restore=nothing`: If not `nothing`, uses the provided function to return the
(cached) final result of the current scheduler invocation, were it to execute.
If this returns a `Chunk`, all thunks will be skipped, and the `Chunk` will be
returned.  If `nothing` is returned, restoring is skipped, and the scheduler
will execute as usual. If this function throws an error, restoring will be
skipped, and the error will be displayed.
- `schedule_model::AbstractDecision=SchDefaultModel()`: Which decision model to
use for scheduling each thunk.
"""
Base.@kwdef struct SchedulerOptions
    single::Union{Int,Nothing} = nothing
    proclist = nothing
    allow_errors::Union{Bool,Nothing} = nothing
    checkpoint = nothing
    restore = nothing
    schedule_model::AbstractDecision = SchDefaultModel()
end

"""
    ThunkOptions

Stores Thunk-local options to be passed to the Dagger.Sch scheduler.

# Arguments
- `propagates::NTuple{N,Symbol} where N`: The set of options that will be propagated by this
task to tasks that it spawns.
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
- `storage_leaf_tag::Union{MemPool.Tag,Nothing}=nothing`: If not `nothing`,
specifies the MemPool storage leaf tag to associate with the thunk's result.
This tag can be used by MemPool's storage devices to manipulate their behavior,
such as the file name used to store data on disk."
- `storage_retain::Union{Bool,Nothing}=nothing`: The value of `retain` to pass to
`MemPool.poolset` when constructing the result `Chunk`. `nothing` defaults to
`false`.
"""
Base.@kwdef struct ThunkOptions
    propagates::Union{NTuple{N,Symbol} where N,Nothing} = nothing
    single::Union{Int,Nothing} = nothing
    proclist = nothing
    time_util::Union{Dict{Type,Any},Nothing} = nothing
    alloc_util::Union{Dict{Type,UInt64},Nothing} = nothing
    occupancy::Union{Dict{Type,Real},Nothing} = nothing
    checkpoint = nothing
    restore = nothing
    storage::Union{Chunk,Nothing} = nothing
    storage_root_tag = nothing
    storage_leaf_tag::Union{MemPool.Tag,Nothing} = nothing
    storage_retain::Union{Bool,Nothing} = nothing
end

"""
    Base.merge(sopts::SchedulerOptions, topts::ThunkOptions) -> ThunkOptions

Combine `SchedulerOptions` and `ThunkOptions` into a new `ThunkOptions`.
"""
function Base.merge(sopts::SchedulerOptions, topts::ThunkOptions)
    single = topts.single !== nothing ? topts.single : sopts.single
    proclist = topts.proclist !== nothing ? topts.proclist : sopts.proclist
    ThunkOptions(topts.propagates,
                 single,
                 proclist,
                 topts.time_util,
                 topts.alloc_util,
                 topts.occupancy,
                 topts.checkpoint,
                 topts.restore,
                 topts.storage,
                 topts.storage_root_tag,
                 topts.storage_leaf_tag,
                 topts.storage_retain)
end

"""
    populate_defaults(opts::ThunkOptions, sig::Vector{DataType}) -> ThunkOptions

Returns a `ThunkOptions` with default values filled in for a function call with
signature `sig`, if the option was previously unspecified in `opts`.
"""
function populate_defaults(opts::ThunkOptions, sig)
    function maybe_default(opts, opt::Symbol)
        if opts !== nothing && (old_opt = getproperty(opts, opt)) !== nothing
            return old_opt
        else
            return Dagger.default_option(Val(opt), sig...)
        end
    end
    if all_fields_nothing(opts)
        return get!(DEFAULT_OPTIONS_FOR_SIGNATURE, sig) do
            ThunkOptions(
                maybe_default(nothing, :propagates),
                maybe_default(nothing, :single),
                maybe_default(nothing, :proclist),
                maybe_default(nothing, :time_util),
                maybe_default(nothing, :alloc_util),
                maybe_default(nothing, :occupancy),
                maybe_default(nothing, :checkpoint),
                maybe_default(nothing, :restore),
                maybe_default(nothing, :storage),
                maybe_default(nothing, :storage_root_tag),
                maybe_default(nothing, :storage_leaf_tag),
                maybe_default(nothing, :storage_retain),
            )
        end
    end
    ThunkOptions(
        maybe_default(opts, :propagates),
        maybe_default(opts, :single),
        maybe_default(opts, :proclist),
        maybe_default(opts, :time_util),
        maybe_default(opts, :alloc_util),
        maybe_default(opts, :occupancy),
        maybe_default(opts, :checkpoint),
        maybe_default(opts, :restore),
        maybe_default(opts, :storage),
        maybe_default(opts, :storage_root_tag),
        maybe_default(opts, :storage_leaf_tag),
        maybe_default(opts, :storage_retain),
    )
end
const DEFAULT_OPTIONS_FOR_SIGNATURE = MultiThreadedCache{Vector{DataType},ThunkOptions}()
@generated function all_fields_nothing(x)
    ex = nothing
    for field in fieldnames(x)
        field_check = :(isnothing(getfield(x, $(QuoteNode(field)))))
        if ex !== nothing
            ex = Expr(:(&&), ex, field_check)
        else
            ex = field_check
        end
    end
    return ex
end

# Eager scheduling
include("eager.jl")

const WORKER_MONITOR_LOCK = Threads.ReentrantLock()
const WORKER_MONITOR_TASKS = Dict{Int,Task}()
const WORKER_MONITOR_CHANS = Dict{Int,Dict{UInt64,RemoteChannel}}()
function init_proc(state, p, log_sink)
    ctx = Context(Int[]; log_sink)
    timespan_start(ctx, :init_proc, (;worker=p.pid), nothing)
    # Initialize pressure and capacity
    gproc = OSProc(p.pid)
    if p.pid != myid()
        lock(WORKER_MONITOR_LOCK) do
            wid = p.pid
            if !haskey(WORKER_MONITOR_TASKS, wid)
                t = @async begin
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
                                    put!(d[uid], (wid, nothing, nothing, true, ProcessExitedException(wid), nothing))
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

    timespan_finish(ctx, :init_proc, (;worker=p.pid), nothing)
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
    timespan_start(ctx, :cleanup_proc, (;worker=wid), nothing)
    lock(WORKER_MONITOR_LOCK) do
        if haskey(WORKER_MONITOR_CHANS, wid)
            delete!(WORKER_MONITOR_CHANS[wid], state.uid)
        end
    end
    try
        remote_do(_cleanup_proc, wid, state.uid, log_sink)
    catch err
        @debug "Failed to clean-up worker $wid" exception=(err,catch_backtrace())
    end
    timespan_finish(ctx, :cleanup_proc, (;worker=wid), nothing)
end

"Process-local condition variable (and lock) indicating task completion."
const TASK_SYNC = Threads.Condition()

"Process-local set of running task IDs."
const TASKS_RUNNING = Set{ThunkID}()

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

    chan = RemoteChannel(()->Channel(typemax(Int)))
    deps = dependents(d)
    ord = order(d, noffspring(deps))

    node_order = x -> -get(ord, x, 0)
    state = start_state(deps, node_order, chan, options)

    master = OSProc(myid())

    timespan_start(ctx, :scheduler_init, nothing, master)
    try
        scheduler_init(ctx, state, d, options, deps)
    finally
        timespan_finish(ctx, :scheduler_init, nothing, master)
    end

    value, errored = try
        scheduler_run(ctx, state, d, options)
    finally
        # Always try to tear down the scheduler
        timespan_start(ctx, :scheduler_exit, nothing, master)
        try
            scheduler_exit(ctx, state, options)
        catch err
            @error "Error when tearing down scheduler" exception=(err,catch_backtrace())
        finally
            timespan_finish(ctx, :scheduler_exit, nothing, master)
        end
    end

    if errored
        throw(value)
    end
    return value
end

function sch_interrupt_handler()
    cond = nothing
    @lock Base.INTERRUPT_HANDLERS_LOCK begin
        for (mod, handlers) in Base.INTERRUPT_HANDLERS
            for (other_handler, _cond) in handlers
                if current_task() === other_handler
                    cond = _cond
                    break
                end
            end
        end
    end
    @assert cond !== nothing
    while true
        try
            #Base.wait_for_interrupt()
            @lock cond wait(cond)
        catch err
            err isa InterruptException || rethrow()
        end
        state = Dagger.Sch.EAGER_STATE[]
        state !== nothing || continue
        try
            #=
            println("Scheduler:")
            @lock state.lock print_sch_status(state)
            println()
            proc_states(state.uid) do states
                for proc in keys(states)
                    print_worker_status(states[proc], proc)
                end
            end
            =#

            cancel!()
        catch err
            @error "Error in interrupt handler" exception=(err,catch_backtrace())
        end
    end
end
function cancel!()
    # Cancel all ready or waiting tasks
    for task in state.ready
        cache_store!(state, task, InterruptException(), true)
        set_failed!(state, task)
    end
    empty!(state.ready)
    for task in keys(state.waiting)
        cache_store!(state, task, InterruptException(), true)
        set_failed!(state, task)
    end
    empty!(state.waiting)

    # FIXME: Request cancel for all running tasks (except eager_thunk)
end

function scheduler_init(ctx, state::ComputeState, d::Thunk, options, deps)
    # Setup thunk_dict mappings
    for node in filter(istask, keys(deps))
        state.thunk_dict[node.id] = WeakThunk(node)
        for dep in deps[node]
            state.thunk_dict[dep.id] = WeakThunk(dep)
        end
    end

    # Initialize workers
    @sync for p in procs_to_use(ctx)
        @async begin
            try
                init_proc(state, p, ctx.log_sink)
            catch err
                @error "Error initializing worker $p" exception=(err,catch_backtrace())
                remove_dead_proc!(ctx, state, p)
            end
        end
    end

    # Listen for new workers
    @async begin
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
            schedule!(ctx, state)
        end

        check_integrity(ctx)

        isempty(state.running) && continue
        timespan_start(ctx, :take, nothing, nothing)
        @dagdebug nothing :take "Waiting for results"
        chan_value = take!(state.chan) # get result of completed thunk
        timespan_finish(ctx, :take, nothing, nothing)
        if chan_value isa RescheduleSignal
            continue
        end
        pid, proc, thunk_id, errored, result, new_metrics = chan_value
        gproc = OSProc(pid)
        safepoint(state)
        @lock state.lock begin
            thunk_failed = false
            if errored
                true_ex = unwrap_nested_exception(result)
                if true_ex isa ProcessExitedException
                    @dagdebug nothing :take "Worker $pid died"
                    if thunk_id !== nothing
                        @warn "Worker $pid died, rescheduling work"
                    end

                    # Tear down dead worker
                    timespan_start(ctx, :remove_procs, (;worker=pid), nothing)
                    remove_dead_proc!(ctx, state, gproc)
                    timespan_finish(ctx, :remove_procs, (;worker=pid), nothing)

                    if thunk_id !== nothing
                        # Recreate any lost tasks/data
                        timespan_start(ctx, :handle_fault, (;worker=pid), nothing)
                        handle_fault(ctx, state, gproc)
                        timespan_finish(ctx, :handle_fault, (;worker=pid), nothing)
                    end
                    return # effectively `continue`
                elseif true_ex isa SchedulerHaltedException
                    @dagdebug nothing :take "Got halt request, exiting"
                    throw(true_ex)
                else
                    if something(ctx.options.allow_errors, false)
                        thunk_failed = true
                    else
                        throw(result)
                    end
                end
            end

            if thunk_id !== nothing
                @dagdebug thunk_id :take "Got finished task"
                thunk = unwrap_weak_checked(state.thunk_dict[thunk_id])
                if new_metrics !== nothing
                    # Copy returned metric values to cache
                    merge_remote_metrics!(state.metrics, new_metrics)
                end
                cache_store!(state, thunk, result, thunk_failed)
                if thunk.options !== nothing && thunk.options.checkpoint !== nothing
                    try
                        @invokelatest thunk.options.checkpoint(thunk, result)
                    catch err
                        report_catch_error(err, "Thunk checkpoint failed")
                    end
                end

                timespan_start(ctx, :finish, (;thunk_id), (;thunk_id))
                finish_task!(ctx, state, thunk, thunk_failed)
                timespan_finish(ctx, :finish, (;thunk_id), (;thunk_id))

                delete_unused_tasks!(state)
            end
        end

        safepoint(state)
    end

    # Final value is ready
    value = cache_lookup_checked(state, d)[1]
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
        @async cleanup_proc(state, p, ctx.log_sink)
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

function schedule!(ctx, state, procs=procs_to_use(ctx))
    lock(state.lock) do
        safepoint(state)
        @assert length(procs) > 0

        # Tasks to schedule
        to_fire = Dict{Tuple{OSProc,<:Processor},Vector{Tuple{Thunk,<:Any,<:Any,UInt64,UInt32}}}()

        # Select a new task and get its options
        task = nothing
        @label pop_task
        if task !== nothing
            timespan_finish(ctx, :schedule, (;thunk_id=task.id), (;thunk_id=task.id))
        end
        if isempty(state.ready)
            @goto fire_tasks
        end
        task = pop!(state.ready)

        timespan_start(ctx, :schedule, (;thunk_id=task.id), (;thunk_id=task.id))

        # Check if this task already has a result
        if (result = cache_lookup(state, task)) !== nothing
            if haskey(state.errored, task)
                # An error was eagerly propagated to this task
                finish_failed!(state, task)
            else
                # This shouldn't have happened
                iob = IOBuffer()
                println(iob, "Scheduling inconsistency: Task being scheduled is already cached!")
                println(iob, "  Task: $(task.id)")
                println(iob, "  Cache Entry: $(typeof(result))")
                ex = SchedulingException(String(take!(iob)))
                cache_store!(state, task, ex, true)
            end
            @goto pop_task
        end

        # Fetch all inputs from cache
        inputs = collect_task_inputs(state, task)
        sig = signature(task, state)

        # Generate concrete options
        thunk_opts = merge(ctx.options, task.options)
        opts = populate_defaults(thunk_opts, sig)

        # Calculate initial task scope
        scope = if task.f isa Chunk
            task.f.scope
        else
            if opts.proclist !== nothing
                # proclist overrides scope selection
                AnyScope()
            else
                DefaultScope()
            end
        end

        # Filter out Chunks
        chunks = Chunk[]
        for input in Iterators.map(last, inputs)
            if input isa Chunk
                push!(chunks, input)
            end
        end

        # Refine scope, and validate that Chunk scopes are compatible
        for chunk in chunks
            scope = constrain(scope, chunk.scope)
            if scope isa Dagger.InvalidScope
                ex = SchedulingException("Scopes are not compatible: $(scope.x), $(scope.y)")
                cache_store!(state, task, ex, true)
                set_failed!(state, task)
                @goto pop_task
            end
        end

        # Collect all known processors
        all_procs = unique(vcat([collect(Dagger.get_processors(gp)) for gp in procs]...))

        # Decide on an ordered set of candidate processors to schedule on
        local_procs = make_decision(state.schedule_model, :signature, :schedule, sig, inputs, all_procs)

        # Select the first valid processor
        scheduled = false
        for proc in local_procs
            gproc = get_parent(proc)
            can_use, scope = can_use_proc(task, gproc, proc, opts, scope)
            if can_use
                #=has_cap,=# est_time_util, est_alloc_util, est_occupancy =
                    task_utilization(state, proc, opts, sig)
                if true#has_cap
                    # Schedule task onto proc
                    proc_tasks = get!(to_fire, (gproc, proc)) do
                        Vector{Tuple{Thunk,<:Any,<:Any,UInt64,UInt32}}()
                    end
                    push!(proc_tasks, (task, scope, est_time_util, est_alloc_util, est_occupancy))
                    @dagdebug task :schedule "Scheduling to $gproc -> $proc"
                    @goto pop_task
                end
            end
        end

        # Report that no processors were valid
        ex = SchedulingException("No processors available, try widening scope")
        cache_store!(state, task, ex, true)
        set_failed!(state, task)
        @goto pop_task

        # Done scheduling, fire all newly-scheduled tasks
        @label fire_tasks
        for gpp in keys(to_fire)
            fire_tasks!(ctx, to_fire[gpp], gpp, state)
        end
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

        timespan_start(ctx, :assign_procs, nothing, nothing)

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

        timespan_finish(ctx, :assign_procs, nothing, nothing)
        old_ps = new_ps
    end
end

function remove_dead_proc!(ctx, state, proc, options=ctx.options)
    @assert options.single !== proc.pid "Single worker failed, cannot continue."
    rmprocs!(ctx, [proc])
    delete_metrics_for!(state.metrics, proc)
    delete!(state.worker_chans, proc.pid)
end

function finish_task!(ctx, state, node, thunk_failed)
    pop!(state.running, node)
    delete!(state.running_on, node)
    if thunk_failed
        set_failed!(state, node)
    end
    if node.cache
        node.cache_ref = cache_lookup_checked(state, node)[1]
    end
    schedule_dependents!(state, node, thunk_failed)
    fill_registered_futures!(state, node, thunk_failed)

    to_evict = cleanup_syncdeps!(state, node)
    if node.f isa Chunk
        # FIXME: Check the graph for matching chunks
        push!(to_evict, node.f)
    end
    if haskey(state.waiting_data, node) && isempty(state.waiting_data[node])
        delete!(state.waiting_data, node)
    end
    #evict_all_chunks!(ctx, to_evict)
end

function delete_unused_tasks!(state)
    to_delete = Thunk[]
    for thunk in state.thunks_to_delete
        if task_unused(state, thunk)
            # Finished and nobody waiting on us, we can be deleted
            push!(to_delete, thunk)
        end
    end
    for thunk in to_delete
        # Delete all cached data
        task_delete!(state, thunk)

        pop!(state.thunks_to_delete, thunk)
    end
end
function delete_unused_task!(state, thunk)
    if task_unused(state, thunk)
        # Will not be accessed further, delete all cached data
        task_delete!(state, thunk)
        return true
    else
        return false
    end
end
task_unused(state, thunk) =
    haskey(state.cache, thunk) && !haskey(state.waiting_data, thunk)
function task_delete!(state, thunk)
    delete!(state.cache, thunk)
    delete!(state.errored, thunk)
    delete!(state.valid, thunk)
    delete!(state.thunk_dict, thunk.id)
end

function evict_all_chunks!(ctx, to_evict)
    if !isempty(to_evict)
        @sync for w in map(p->p.pid, procs_to_use(ctx))
            @async remote_do(evict_chunks!, w, ctx.log_sink, to_evict)
        end
    end
end
function evict_chunks!(log_sink, chunks::Set{Chunk})
    # Need worker id or else Context might use Processors which user does not want us to use.
    # In particular workers which have not yet run using Dagger will cause the call below to throw an exception
    ctx = Context([myid()]; log_sink)
    for chunk in chunks
        lock(TASK_SYNC) do
            timespan_start(ctx, :evict, (;worker=myid()), (;data=chunk))
            haskey(CHUNK_CACHE, chunk) && delete!(CHUNK_CACHE, chunk)
            timespan_finish(ctx, :evict, (;worker=myid()), (;data=chunk))
        end
    end
    nothing
end

fire_task!(ctx, thunk::Thunk, p, state; scope=AnyScope(), time_util=10^9, alloc_util=10^6, occupancy=typemax(UInt32)) =
    fire_task!(ctx, (thunk, scope, time_util, alloc_util, occupancy), p, state)
fire_task!(ctx, (thunk, scope, time_util, alloc_util, occupancy)::Tuple{Thunk,<:Any}, p, state) =
    fire_tasks!(ctx, [(thunk, scope, time_util, alloc_util, occupancy)], p, state)
function fire_tasks!(ctx, thunks::Vector{<:Tuple}, (gproc, proc), state)
    to_send = []
    for (thunk, scope, time_util, alloc_util, occupancy) in thunks
        task_spec = prepare_fire_task!(ctx, state, thunk, proc, scope, time_util, alloc_util, occupancy)
        task_spec === nothing && continue

        # TODO: De-dup common fields (log_sink, uid, etc.)
        push!(to_send, task_spec)
    end
    # N.B. We don't batch these because we might get a deserialization
    # error due to something not being defined on the worker, and then we don't
    # know which task failed.
    for ts in to_send
        errormonitor_tracked("fire tasks", @async begin
            timespan_start(ctx, :fire, (;worker=gproc.pid), nothing)
            try
                remotecall_wait(do_tasks, gproc.pid, proc, state.chan, [ts]);
            catch err
                bt = catch_backtrace()
                thunk_id = ts.thunk_id
                put!(state.chan, (gproc.pid, proc, thunk_id, true, CapturedException(err, bt), nothing))
            finally
                timespan_finish(ctx, :fire, (;worker=gproc.pid), nothing)
            end
        end)
    end
end
function prepare_fire_task!(ctx, state, thunk, proc, scope, time_util, alloc_util, occupancy)
    @assert islocked(state.lock)

    gproc = get_parent(proc)

    push!(state.running, thunk)
    state.running_on[thunk] = gproc

    if thunk.cache && thunk.cache_ref !== nothing
        # the result might be already cached
        data = thunk.cache_ref
        if data !== nothing
            # cache hit
            cache_store!(state, thunk, data)
            thunk_failed = get(state.errored, thunk, false)
            finish_task!(ctx, state, thunk, thunk_failed)
            return
        else
            # cache miss
            thunk.cache_ref = nothing
        end
    end

    if thunk.options !== nothing && thunk.options.restore !== nothing
        try
            result = @invokelatest thunk.options.restore(thunk)
            if result isa Chunk
                cache_store!(state, thunk, result)
                finish_task!(ctx, state, thunk, false)
                return
            elseif result !== nothing
                throw(ArgumentError("Invalid restore return type: $(typeof(result))"))
            end
        catch err
            report_catch_error(err, "Thunk restore failed")
        end
    end

    ids = Union{ThunkID,Int}[0]
    data = Any[thunk.f]
    positions = Union{Symbol,Nothing}[]
    for (idx, pos_x) in enumerate(thunk.inputs)
        pos, x = pos_x
        x = unwrap_weak_checked(x)
        push!(ids, istask(x) ? x.id : -idx)
        push!(data, istask(x) ? cache_lookup_checked(state, x)[1] : x)
        push!(positions, pos)
    end

    toptions = thunk.options !== nothing ? thunk.options : ThunkOptions()
    options = merge(ctx.options, toptions)
    propagated = get_propagated_options(toptions, thunk)
    @assert (options.single === nothing) || (gproc.pid == options.single)
    # TODO: Set `sch_handle.tid.ref` to the right `DRef`
    sch_handle = SchedulerHandle(ThunkRef(thunk), state.worker_chans[gproc.pid]...)

    return TaskSpec(thunk.id, time_util, alloc_util, occupancy,
                    scope, thunk.world, chunktype(thunk.f), data, ids, positions,
                    thunk.get_result, thunk.persist, thunk.cache, thunk.meta,
                    options, propagated,
                    (log_sink=ctx.log_sink, profile=ctx.profile),
                    sch_handle, state.uid, state.schedule_model)
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

"A serializable description of a `Thunk` to be executed."
struct TaskSpec
    thunk_id::ThunkID
    est_time_util::UInt64
    est_alloc_util::UInt64
    est_occupancy::UInt32
    scope::Dagger.AbstractScope
    world::UInt64
    Tf::Type
    data::Vector{Any}
    ids::Vector{Union{ThunkID,Int}}
    positions::Vector{Union{Symbol,Nothing}}
    send_result::Bool
    persist::Bool
    cache::Bool
    meta::Bool
    options::ThunkOptions
    propagated::NamedTuple
    ctx_vars::NamedTuple
    sch_handle::SchedulerHandle
    sch_uid::UInt64
    sch_model::AbstractDecision
end
Base.hash(task::TaskSpec, h::UInt) = hash(task.thunk_id, hash(TaskSpec, h))

struct ProcessorInternalState
    ctx::Context
    proc::Processor
    sch_model::AbstractDecision
    queue::LockedObject{PriorityQueue{TaskSpec, UInt32, Base.Order.ForwardOrdering}}
    reschedule::Doorbell
    tasks::Dict{ThunkID,Task}
    proc_occupancy::Base.RefValue{UInt32}
    time_pressure::Base.RefValue{UInt64}
    done::Base.RefValue{Bool}
end
struct ProcessorState
    state::ProcessorInternalState
    runner::Task
end

function print_worker_status(state, proc)
    println("Processor: $proc")
    istate = state.state
    lock(istate.queue) do queue
        println("- Queued: $(length(queue))")
        println("- Running: $(length(istate.tasks))")
        println("- Occupancy: $(istate.proc_occupancy[]÷typemax(UInt32))")
        println("- Pressure: $(istate.time_pressure[]÷typemax(UInt64))")
    end
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
    proc_states(f, task_local_storage(:_dagger_sch_uid)::UInt64)

task_tid_for_processor(::Processor) = nothing
task_tid_for_processor(proc::Dagger.ThreadProc) = proc.tid

stealing_permitted(::Processor) = true
stealing_permitted(proc::Dagger.ThreadProc) = proc.owner != 1 || proc.tid != 1

proc_has_occupancy(proc_occupancy, task_occupancy) =
    UInt64(task_occupancy) + UInt64(proc_occupancy) <= typemax(UInt32)

function start_processor_runner!(istate::ProcessorInternalState, uid::UInt64, return_queue::RemoteChannel)
    to_proc = istate.proc
    proc_run_task = @task begin
        ctx = istate.ctx
        sch_model = istate.sch_model
        tasks = istate.tasks
        proc_occupancy = istate.proc_occupancy
        time_pressure = istate.time_pressure

        wid = get_parent(to_proc).pid
        work_to_do = false
        while isopen(return_queue)
            # Wait for new tasks
            if !work_to_do
                @dagdebug nothing :processor "Waiting for tasks"
                timespan_start(ctx, :proc_run_wait, (;worker=wid, processor=to_proc), nothing)
                wait(istate.reschedule)
                @static if VERSION >= v"1.9"
                    reset(istate.reschedule)
                end
                timespan_finish(ctx, :proc_run_wait, (;worker=wid, processor=to_proc), nothing)
                if istate.done[]
                    return
                end
            end

            # Fetch a new task to execute
            @dagdebug nothing :processor "Trying to dequeue"
            timespan_start(ctx, :proc_run_fetch, (;worker=wid, processor=to_proc), nothing)
            work_to_do = false
            task_and_occupancy = lock(istate.queue) do queue
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
                timespan_finish(ctx, :proc_run_fetch, (;worker=wid, processor=to_proc), nothing)

                @dagdebug nothing :processor "Failed to dequeue"

                if !stealing_permitted(to_proc)
                    continue
                end

                if proc_occupancy[] == typemax(UInt32)
                    continue
                end

                @dagdebug nothing :processor "Trying to steal"

                # Try to steal a task
                timespan_start(ctx, :steal_local, (;worker=wid, processor=to_proc), nothing)

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
                        if !isa(constrain(task.scope, Dagger.ExactScope(to_proc)),
                                Dagger.InvalidScope)
                           typemax(UInt32) - proc_occupancy_cached >= occupancy
                            # Compatible, steal this task
                            # TODO: Steal from high-occupancy end
                            return dequeue_pair!(queue)
                        end
                        return nothing
                    end
                    if task_and_occupancy !== nothing
                        from_proc = other_istate.proc
                        thunk_id = task.thunk_id
                        @dagdebug thunk_id :processor "Stolen from $from_proc by $to_proc"
                        timespan_finish(ctx, :steal_local, (;worker=wid, processor=to_proc), (;from_proc, thunk_id))
                        # TODO: Keep stealing until we hit full occupancy?
                        @goto execute
                    end
                end
                timespan_finish(ctx, :steal_local, (;worker=wid, processor=to_proc), nothing)

                # TODO: Try to steal from remote queues

                continue
            end

            @label execute
            task, task_occupancy = task_and_occupancy
            thunk_id = task.thunk_id
            time_util = task.est_time_util
            # TODO: Don't take this lock only for logging
            current_proc_occupancy, current_time_pressure = lock(istate.queue) do _
                (proc_occupancy[], time_pressure[])
            end
            timespan_finish(ctx, :proc_run_fetch,
                            (;worker=wid, processor=to_proc),
                            (;thunk_id,
                              proc_occupancy=current_proc_occupancy, task_occupancy,
                              proc_pressure=current_time_pressure, task_pressure=time_util))
            @dagdebug thunk_id :processor "Dequeued task"

            # Execute the task and return its result
            t = @task begin
                processor_run_metrics = required_metrics_to_collect(sch_model, :processor, :run)
                errored = false
                result = nothing
                metrics = nothing
                try
                    setup_metric_supplement!((;occupancy=current_proc_occupancy,
                                               time_pressure=current_time_pressure))
                    with_metrics(processor_run_metrics, :processor, :run, to_proc) do
                        result = do_task(to_proc, task)
                    end
                catch err
                    bt = catch_backtrace()
                    errored = true
                    result = CapturedException(err, bt)
                end

                # Extract metrics to send to the core
                @warn "Only transfer metrics explicitly requested for cross-worker transfer" maxlog=1
                # FIXME: Copy relevant metrics from this worker's global metrics cache
                #= Grab all updated metrics
                function copy_worker_metrics(task::TaskSpec, metrics::MetricsCache)
                    copied_metrics = MetricsCache()
                    copied_metrics[(:processor, :run)] = Dict{AnalysisOrMetric,Any}()
                    for m in unique(keys(metrics[(:processor, :run)]))
                        copied_metrics[(:processor, :run)][m] = Dict{Processor,Any}()
                        copied_metrics[(:processor, :run)][m][to_proc] = metrics[(:processor, :run)][m][to_proc]
                    end
                    copied_metrics[(:chunk, :move)] = Dict{AnalysisOrMetric,Any}()
                    for m in unique(keys(metrics[(:chunk, :move)]))
                        copied_metrics[(:chunk, :move)][m] = Dict{Chunk,Any}()
                        for input in task.data
                            input isa Chunk || continue
                            copied_metrics[(:chunk, :move)][m][input] = metrics[(:chunk, :move)][m][input]
                        end
                    end
                    copied_metrics[(:signature, :execute)] = Dict{AnalysisOrMetric,Any}()
                    for m in unique(keys(metrics[(:signature, :execute)]))
                        copied_metrics[(:signature, :execute)][m] = Dict{Signature,Any}()
                        # TODO: This could be better
                        signature = first(keys(metrics[(:signature, :execute)][m]))
                        copied_metrics[(:signature, :execute)][m][signature] = metrics[(:signature, :execute)][m][signature]
                    end
                    return copied_metrics
                end
                metrics = copy_worker_metrics(task, metrics)
                =#
                metrics = lock(identity, EMPTY_METRICS)

                # Mark this task as done
                # Let the processor schedule more work
                lock(istate.queue) do _
                    delete!(tasks, thunk_id)
                    proc_occupancy[] -= task_occupancy
                    time_pressure[] -= time_util
                end
                notify(istate.reschedule)

                # Send the result and metrics to the core
                try
                    put!(return_queue, (myid(), to_proc, thunk_id, errored, result, metrics))
                catch err
                    if unwrap_nested_exception(err) isa InvalidStateException || !isopen(return_queue)
                        @dagdebug thunk_id :execute "Return queue is closed, failing to put result" chan=return_queue exception=(err, catch_backtrace())
                    else
                        rethrow(err)
                    end
                end
            end
            lock(istate.queue) do _
                tid = task_tid_for_processor(to_proc)
                if tid !== nothing
                    Dagger.set_task_tid!(t, tid)
                else
                    t.sticky = false
                end
                tasks[thunk_id] = errormonitor_tracked("thunk $thunk_id", schedule(t))
                proc_occupancy[] += task_occupancy
                time_pressure[] += time_util
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
function processor_queue(ctx, uid, proc, sch_model, return_queue)
    proc_states(uid) do states
        get!(states, proc) do
            queue = PriorityQueue{TaskSpec, UInt32}()
            queue_locked = LockedObject(queue)
            reschedule = Doorbell()
            istate = ProcessorInternalState(ctx, proc, sch_model,
                                            queue_locked, reschedule,
                                            Dict{Int,Task}(),
                                            Ref(UInt32(0)), Ref(UInt64(0)),
                                            Ref(false))
            runner = start_processor_runner!(istate, uid, return_queue)
            @static if VERSION < v"1.9"
                reschedule.waiter = runner
            end
            return ProcessorState(istate, runner)
        end
    end
end
function processor_enqueue!(ctx, state::ProcessorState, uid, to_proc::Processor, tasks::Vector{Vector{Any}})
    istate = state.state
    lock(istate.queue) do queue
        for task in tasks
            thunk_id = task.thunk_id
            timespan_start(ctx, :enqueue, (;processor=to_proc, thunk_id), nothing)
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
            enqueue!(queue, task, task.est_occupancy)
            timespan_finish(ctx, :enqueue, (;processor=to_proc, thunk_id), nothing)
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
    do_tasks(to_proc, return_queue, tasks::Vector{TaskSpec})

Executes a batch of tasks on `to_proc`, returning their results through
`return_queue`.
"""
function do_tasks(to_proc, return_queue, tasks::Vector{TaskSpec})
    @dagdebug nothing :processor "Enqueuing task batch" batch_size=length(tasks)

    # FIXME: Use global context
    ctx_vars = first(tasks).ctx_vars
    ctx = Context(Processor[]; log_sink=ctx_vars.log_sink, profile=ctx_vars.profile)
    uid = first(tasks).sch_uid
    sch_model = first(tasks).sch_model
    state = processor_queue(ctx, uid, to_proc, sch_model, return_queue)
    processor_enqueue!(ctx, state, uid, to_proc, tasks)
end

"""
    do_task(to_proc::Processor, task::TaskSpec) -> Any

Executes a single task specified by `task` on `to_proc`, and returns the task's
result.
"""
function do_task(to_proc::Processor, task::TaskSpec)
    ctx_vars = task.ctx_vars
    ctx = Context(Processor[]; log_sink=ctx_vars.log_sink, profile=ctx_vars.profile)

    from_proc = OSProc()
    Tdata = Any[]
    data = task.data
    for x in data
        push!(Tdata, chunktype(x))
    end
    f = isdefined(task.Tf, :instance) ? task.Tf.instance : nothing

    # Wait for required resources to become available
    options = task.options
    to_storage = options.storage !== nothing ? fetch(options.storage) : MemPool.GLOBAL_DEVICE[]
    to_storage_name = nameof(typeof(to_storage))
    storage_cap = storage_capacity(to_storage)

    thunk_id = task.thunk_id

    est_time_util, est_alloc_util = task.est_time_util, task.est_alloc_util
    if !task.meta
        # Factor in the memory costs for our lazy arguments
        for arg in data[2:end]
            if arg isa Chunk
                est_alloc_util += arg.handle.size
            end
        end
    end

    #= FIXME: Wait for storage space
    timespan_start(ctx, :storage_wait, (;thunk_id, processor=to_proc), (;f, device=typeof(to_storage)))
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
            if est_alloc_util > storage_cap
                debug_storage("WARN: Estimated utilization above storage capacity on $to_storage_name, proceeding anyway")
                break
            end
            if est_alloc_util + real_alloc_util > storage_cap
                if externally_varying(to_storage)
                    debug_storage("WARN: Insufficient space and allocation behavior is externally varying on $to_storage_name, proceeding anyway")
                    break
                end
                if length(TASKS_RUNNING) <= 2 # This task + eager submission task
                    debug_storage("WARN: Insufficient space and no other running tasks on $to_storage_name, proceeding anyway")
                    break
                end
                # Fully utilized, wait and re-check
                debug_storage("Waiting for free $to_storage_name")
                wait(TASK_SYNC)
            else
                # Sufficient free storage is available, prepare for execution
                debug_storage("Using available $to_storage_name")
                break
            end
        end
    end
    timespan_finish(ctx, :storage_wait, (;thunk_id, processor=to_proc), (;f, device=typeof(to_storage)))
    =#

    # Determine which metrics to collect for chunk move
    chunk_move_metrics = required_metrics_to_collect(task.sch_model, :chunk, :move)

    @dagdebug thunk_id :execute "Moving data"

    # Initiate data transfers for function and arguments
    ids = task.ids
    _data, _ids = if task.meta
        (Any[first(data)], Union{ThunkID,Int}[first(ids)]) # always fetch function
    else
        (data, ids)
    end
    fetch_tasks = map(Iterators.zip(_data,_ids)) do (chunk, id)
        @async begin
            timespan_start(ctx, :move, (;thunk_id, id, processor=to_proc), (;f, data=chunk))
            #= FIXME: This isn't valid if x is written to
            if chunk isa Chunk
                value = nothing
                is_chunk_cached = false
                lock(TASK_SYNC) do
                    if haskey(CHUNK_CACHE, chunk)
                        is_chunk_cached = true
                        chunk_proc_cache = CHUNK_CACHE[chunk]
                        if haskey(chunk_proc_cache, to_proc)
                            value = chunk_proc_cache[to_proc]
                        else
                            # Convert from cached value
                            # TODO: Choose "closest" processor of same type first
                            some_proc = first(keys(chunk_proc_cache))
                            some_value = chunk_proc_cache[some_proc]
                            @dagdebug thunk_id :move "Cache hit for argument $id at $some_proc: $(typeof(some_value))"
                            with_metrics(chunk_move_metrics, :chunk, :move, chunk) do
                                value = @invokelatest move(some_proc, to_proc, some_value)
                            end
                            chunk_proc_cache[to_proc] = value
                        end
                    end
                end

                if !is_chunk_cached
                    # Fetch it
                    from_proc = processor(chunk)
                    with_metrics(chunk_move_metrics, :chunk, :move, chunk) do
                        value = @invokelatest move(from_proc, to_proc, chunk)
                    end

                    @dagdebug thunk_id :move "Cache miss for argument $id at $from_proc"

                    # Update cache
                    lock(TASK_SYNC) do
                        CHUNK_CACHE[chunk] = Dict{Processor,Any}(to_proc=>value)
                    end
                end
            elseif chunk isa Chunk
                # Is a Chunk, but being written to
                value = nothing
                with_metrics(chunk_move_metrics, :chunk, :move, chunk) do
                    value = @invokelatest move(to_proc, chunk)
                end
            else
                # Not a Chunk
                # FIXME: Collect metrics
                value = @invokelatest move(to_proc, chunk)
            end
            =#
            if chunk isa Chunk
                value = with_metrics(chunk_move_metrics, :chunk, :move, chunk) do
                    @invokelatest move(to_proc, chunk)
                end
            else
                value = @invokelatest move(to_proc, chunk)
            end
            @dagdebug thunk_id :move "Moved argument $id to $to_proc: $(typeof(value))"
            timespan_finish(ctx, :move, (;thunk_id, id, processor=to_proc), (;f, data=value); tasks=[Base.current_task()])
            return value
        end
    end
    fetched = Any[]
    for data_task in fetch_tasks
        push!(fetched, fetch_report(data_task))
    end
    if task.meta
        append!(fetched, data[2:end])
    end

    f = popfirst!(fetched)
    @assert !(f isa Chunk) "Failed to unwrap thunk function"
    fetched_args = Any[]
    fetched_kwargs = Pair{Symbol,Any}[]
    for (idx, x) in enumerate(fetched)
        pos = task.positions[idx]
        if pos === nothing
            push!(fetched_args, x)
        else
            push!(fetched_kwargs, pos => x)
        end
    end

    #= FIXME: If MaxUtilization, stop processors and wait
    if (est_time_util isa MaxUtilization) && (real_time_util > 0)
        # FIXME: Stop processors
        # FIXME: Wait on processors to stop
        est_time_util = count(c->typeof(c)===typeof(to_proc), children(from_proc))
    end
    =#

    #real_time_util[] += est_time_util
    timespan_start(ctx, :compute, (;thunk_id, processor=to_proc), (;f))

    # Determine which metrics to collect for task execution
    signature_metrics = Dict{AnalysisOrMetric,Any}()
    signature_execute_metrics = required_metrics_to_collect(task.sch_model, :signature, :execute)

    # Calculate the task's signature
    signature = Any[]
    for input in data
        push!(signature, chunktype(input))
    end

    @dagdebug thunk_id :execute "Executing $(typeof(f))"

    result_meta = nothing
    try
        # Set TLS variables
        Dagger.set_tls!((
            sch_uid=task.sch_uid,
            sch_handle=task.sch_handle,
            processor=to_proc,
            task_spec=task,
        ))

        result = nothing
        Dagger.with_options(task.propagated) do
            with_metrics(signature_execute_metrics, :signature, :execute, signature) do
                # Execute the task
                result = execute!(to_proc, task.world, f, fetched_args...; fetched_kwargs...)
            end
        end

        # Check if result is safe to store
        device = nothing
        if !(result isa Chunk)
            timespan_start(ctx, :storage_safe_scan, (;thunk_id, processor=to_proc), (;T=typeof(result)))
            device = if walk_storage_safe(result)
                to_storage
            else
                MemPool.CPURAMDevice()
            end
            timespan_finish(ctx, :storage_safe_scan, (;thunk_id, processor=to_proc), (;T=typeof(result)))
        end

        # Construct result
        result_meta = if task.send_result || task.meta
            result
        else
            # TODO: Cache this Chunk locally in CHUNK_CACHE right now
            persist, cache = task.persist, task.cache
            tochunk(result, to_proc;
                    device, persist, cache=persist ? true : cache,
                    tag=options.storage_root_tag,
                    leaf_tag=something(options.storage_leaf_tag, MemPool.Tag()),
                    retain=something(options.storage_retain, false))
        end
    catch ex
        bt = catch_backtrace()
        result_meta = RemoteException(myid(), CapturedException(ex, bt))
    end

    timespan_finish(ctx, :compute, (;thunk_id, processor=to_proc), (;f))
    lock(TASK_SYNC) do
        #real_time_util[] -= est_time_util
        pop!(TASKS_RUNNING, thunk_id)
        notify(TASK_SYNC)
    end

    @dagdebug thunk_id :execute "Returning"

    # TODO: debug_storage("Releasing $to_storage_name")

    return result_meta
end

function __init__()
    init_cache!(DEFAULT_OPTIONS_FOR_SIGNATURE)

    if ccall(:jl_generating_output, Cint, ()) == 0
        # Register interrupt handler
        if isdefined(Base, :register_interrupt_handler)
            interrupt_task = errormonitor_tracked("interrupt handler", Threads.@spawn sch_interrupt_handler())
            Base.register_interrupt_handler(Dagger, interrupt_task)
            atexit() do
                # Unregister interrupt handler
                Base.unregister_interrupt_handler(Dagger, interrupt_task)
            end
        end
    end
end

end # module Sch
