module Sch

using Distributed
import MemPool
import MemPool: DRef, StorageResource
import MemPool: poolset, storage_available, storage_capacity, storage_utilized, externally_varying
import Statistics: mean
import Random: randperm
import Base: @invokelatest

import ..Dagger
import ..Dagger: Context, Processor, Thunk, WeakThunk, ThunkFuture, ThunkFailedException, Chunk, WeakChunk, OSProc, DefaultScope
import ..Dagger: order, dependents, noffspring, istask, inputs, unwrap_weak_checked, affinity, tochunk, timespan_start, timespan_finish, procs, move, chunktype, processor, default_enabled, get_processors, get_parent, execute!, rmprocs!, addprocs!, thunk_processor, constrain, cputhreadtime

const OneToMany = Dict{Thunk, Set{Thunk}}

include("util.jl")
include("fault-handler.jl")
include("dynamic.jl")

mutable struct ProcessorCacheEntry
    gproc::OSProc
    proc::Processor
    next::ProcessorCacheEntry

    ProcessorCacheEntry(gproc::OSProc, proc::Processor) = new(gproc, proc)
end
Base.isequal(p1::ProcessorCacheEntry, p2::ProcessorCacheEntry) =
    p1.proc === p2.proc
function Base.show(io::IO, entry::ProcessorCacheEntry)
    entries = 1
    next = entry.next
    while next !== entry
        entries += 1
        next = next.next
    end
    print(io, "ProcessorCacheEntry(pid $(entry.gproc.pid), $(entry.proc), $entries entries)")
end

const Signature = Vector{Any}

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
- `procs_cache_list::Base.RefValue{Union{ProcessorCacheEntry,Nothing}}` - Cached linked list of processors ready to be used
- `signature_time_cost::Dict{Signature,UInt64}` - Cache of estimated CPU time (in nanoseconds) required to compute calls with the given signature
- `signature_alloc_cost::Dict{Signature,UInt64}` - Cache of estimated CPU RAM (in bytes) required to compute calls with the given signature
- `transfer_rate::Ref{UInt64}` - Estimate of the network transfer rate in bytes per second
- `halt::Base.Event` - Event indicating that the scheduler is halting
- `lock::ReentrantLock` - Lock around operations which modify the state
- `futures::Dict{Thunk, Vector{ThunkFuture}}` - Futures registered for waiting on the result of a thunk.
- `errored::WeakKeyDict{Thunk,Bool}` - Indicates if a thunk's result is an error.
- `chan::RemoteChannel{Channel{Any}}` - Channel for receiving completed thunks.
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
    procs_cache_list::Base.RefValue{Union{ProcessorCacheEntry,Nothing}}
    signature_time_cost::Dict{Signature,UInt64}
    signature_alloc_cost::Dict{Signature,UInt64}
    transfer_rate::Ref{UInt64}
    halt::Base.Event
    lock::ReentrantLock
    futures::Dict{Thunk, Vector{ThunkFuture}}
    errored::WeakKeyDict{Thunk,Bool}
    chan::RemoteChannel{Channel{Any}}
end

function start_state(deps::Dict, node_order, chan)
    state = ComputeState(rand(UInt64),
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
                         Ref{Union{ProcessorCacheEntry,Nothing}}(nothing),
                         Dict{Signature,UInt64}(),
                         Dict{Signature,UInt64}(),
                         Ref{UInt64}(1_000_000),
                         Base.Event(),
                         ReentrantLock(),
                         Dict{Thunk, Vector{ThunkFuture}}(),
                         WeakKeyDict{Thunk,Bool}(),
                         chan)

    for k in sort(collect(keys(deps)), by=node_order)
        if istask(k)
            waiting = Set{Thunk}(Iterators.filter(istask, inputs(k)))
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
- `single::Int=0`: Force all work onto worker with specified id. `0` disables
this option.
- `proclist=nothing`: Force scheduler to use one or more processors that are
instances/subtypes of a contained type. Alternatively, a function can be
supplied, and the function will be called with a processor as the sole
argument and should return a `Bool` result to indicate whether or not to use
the given processor. `nothing` enables all default processors.
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
- `single::Int=0`: Force thunk onto worker with specified id. `0` disables this
option.
- `proclist=nothing`: Force thunk to use one or more processors that are
instances/subtypes of a contained type. Alternatively, a function can be
supplied, and the function will be called with a processor as the sole
argument and should return a `Bool` result to indicate whether or not to use
the given processor. `nothing` enables all default processors.
- `time_util::Dict{Type,Any}=Dict{Type,Any}()`: Indicates the maximum expected
time utilization for this thunk. Each keypair maps a processor type to the
utilization, where the value can be a real (approximately the number of
nanoseconds taken), or `MaxUtilization()` (utilizes all processors of this
type). By default, the scheduler assumes that this thunk only uses one
processor.
- `alloc_util::Dict{Type,UInt64}=Dict{Type,UInt64}()`: Indicates the maximum
expected memory utilization for this thunk. Each keypair maps a processor type
to the utilization, where the value is an integer representing approximately
the maximum number of bytes allocated at any one time.
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
"""
Base.@kwdef struct ThunkOptions
    single::Union{Int,Nothing} = nothing
    proclist = nothing
    time_util::Union{Dict{Type,Any},Nothing} = nothing
    alloc_util::Union{Dict{Type,UInt64},Nothing} = nothing
    allow_errors::Union{Bool,Nothing} = nothing
    checkpoint = nothing
    restore = nothing
    storage::Union{Chunk,Nothing} = nothing
end

# Eager scheduling
include("eager.jl")

"""
    Base.merge(sopts::SchedulerOptions, topts::ThunkOptions) -> ThunkOptions

Combine `SchedulerOptions` and `ThunkOptions` into a new `ThunkOptions`.
"""
function Base.merge(sopts::SchedulerOptions, topts::ThunkOptions)
    single = topts.single !== nothing ? topts.single : sopts.single
    allow_errors = topts.allow_errors !== nothing ? topts.allow_errors : sopts.allow_errors
    proclist = topts.proclist !== nothing ? topts.proclist : sopts.proclist
    ThunkOptions(single,
                 proclist,
                 topts.time_util,
                 topts.alloc_util,
                 allow_errors,
                 topts.checkpoint,
                 topts.restore,
                 topts.storage)
end
Base.merge(sopts::SchedulerOptions, ::Nothing) =
    ThunkOptions(sopts.single,
                 sopts.proclist,
                 nothing,
                 nothing,
                 sopts.allow_errors)
"""
    populate_defaults(opts::ThunkOptions, Tf, Targs) -> ThunkOptions

Returns a `ThunkOptions` with default values filled in for a function of type
`Tf` with argument types `Targs`, if the option was previously unspecified in
`opts`.
"""
function populate_defaults(opts::ThunkOptions, Tf, Targs)
    function maybe_default(opt::Symbol)
        old_opt = getproperty(opts, opt)
        if old_opt !== nothing
            return old_opt
        else
            return Dagger.default_option(Val(opt), Tf, Targs...)
        end
    end
    ThunkOptions(
        maybe_default(:single),
        maybe_default(:proclist),
        maybe_default(:time_util),
        maybe_default(:alloc_util),
        maybe_default(:allow_errors),
        maybe_default(:checkpoint),
        maybe_default(:restore),
        maybe_default(:storage),
    )
end

function cleanup(ctx)
end

const WORKER_MONITOR_LOCK = Threads.ReentrantLock()
const WORKER_MONITOR_TASKS = Dict{Int,Task}()
const WORKER_MONITOR_CHANS = Dict{Int,Dict{UInt64,RemoteChannel}}()
function init_proc(state, p, log_sink)
    ctx = Context(Int[]; log_sink)
    timespan_start(ctx, :init_proc, p.pid, 0)
    # Initialize pressure and capacity
    gproc = OSProc(p.pid)
    lock(state.lock) do
        state.worker_time_pressure[p.pid] = Dict{Processor,UInt64}()
        for proc in get_processors(gproc)
            state.worker_time_pressure[p.pid][proc] = 0
        end

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
    lock(WORKER_MONITOR_LOCK) do
        wid = p.pid
        if !haskey(WORKER_MONITOR_TASKS, wid)
            t = @async begin
                try
                    # Wait until this connection is terminated
                    remotecall_fetch(sleep, wid, typemax(UInt64))
                catch err
                    if err isa ProcessExitedException
                        lock(WORKER_MONITOR_LOCK) do
                            d = WORKER_MONITOR_CHANS[wid]
                            for uid in keys(d)
                                put!(d[uid], (wid, OSProc(wid), nothing, (ProcessExitedException(wid), nothing)))
                            end
                            empty!(d)
                            delete!(WORKER_MONITOR_CHANS, wid)
                        end
                    end
                end
            end
            WORKER_MONITOR_TASKS[wid] = t
            WORKER_MONITOR_CHANS[wid] = Dict{UInt64,RemoteChannel}()
        end
        WORKER_MONITOR_CHANS[wid][state.uid] = state.chan
    end

    # Setup worker-to-scheduler channels
    inp_chan = RemoteChannel(p.pid)
    out_chan = RemoteChannel(p.pid)
    lock(state.lock) do
        state.worker_chans[p.pid] = (inp_chan, out_chan)
    end

    # Setup dynamic listener
    dynamic_listener!(ctx, state, p.pid)

    timespan_finish(ctx, :init_proc, p.pid, 0)
end
function _cleanup_proc(uid, log_sink)
    empty!(CHUNK_CACHE) # FIXME: Should be keyed on uid!
end
function cleanup_proc(state, p, log_sink)
    ctx = Context(Int[]; log_sink)
    timespan_start(ctx, :cleanup_proc, p.pid, 0)
    lock(WORKER_MONITOR_LOCK) do
        wid = p.pid
        if haskey(WORKER_MONITOR_CHANS, wid)
            delete!(WORKER_MONITOR_CHANS[wid], state.uid)
            remote_do(_cleanup_proc, wid, state.uid, log_sink)
        end
    end
    timespan_finish(ctx, :cleanup_proc, p.pid, 0)
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

    chan = RemoteChannel(()->Channel(1024))
    deps = dependents(d)
    ord = order(d, noffspring(deps))

    node_order = x -> -get(ord, x, 0)
    state = start_state(deps, node_order, chan)

    master = OSProc(myid())

    timespan_start(ctx, :scheduler_init, 0, master)
    try
        scheduler_init(ctx, state, d, options, deps)
    finally
        timespan_finish(ctx, :scheduler_init, 0, master)
    end

    value, errored = try
        scheduler_run(ctx, state, d, options)
    finally
        # Always try to tear down the scheduler
        timespan_start(ctx, :scheduler_exit, 0, master)
        try
            scheduler_exit(ctx, state, options)
        catch err
            @error "Error when tearing down scheduler" exception=(err,catch_backtrace())
        finally
            timespan_finish(ctx, :scheduler_exit, 0, master)
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
    safepoint(state)

    # Loop while we still have thunks to execute
    while !isempty(state.ready) || !isempty(state.running)
        if !isempty(state.ready)
            # Nothing running, so schedule up to N thunks, 1 per N workers
            schedule!(ctx, state)
        end

        check_integrity(ctx)

        isempty(state.running) && continue
        timespan_start(ctx, :take, 0, 0)
        chan_value = take!(state.chan) # get result of completed thunk
        timespan_finish(ctx, :take, 0, 0)
        if chan_value isa RescheduleSignal
            continue
        end
        pid, proc, thunk_id, (res, metadata) = chan_value
        gproc = OSProc(pid)
        safepoint(state)
        lock(state.lock) do
            thunk_failed = false
            if res isa Exception
                if unwrap_nested_exception(res) isa ProcessExitedException
                    @warn "Worker $(pid) died, rescheduling work"

                    # Remove dead worker from procs list
                    timespan_start(ctx, :remove_procs, 0, 0)
                    remove_dead_proc!(ctx, state, gproc)
                    timespan_finish(ctx, :remove_procs, 0, 0)

                    timespan_start(ctx, :handle_fault, 0, 0)
                    handle_fault(ctx, state, gproc)
                    timespan_finish(ctx, :handle_fault, 0, 0)
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
            node = unwrap_weak_checked(state.thunk_dict[thunk_id])
            if metadata !== nothing
                state.worker_time_pressure[pid][proc] = metadata.time_pressure
                to_storage = node.options.storage
                state.worker_storage_pressure[pid][to_storage] = metadata.storage_pressure
                state.worker_storage_capacity[pid][to_storage] = metadata.storage_capacity
                state.worker_loadavg[pid] = metadata.loadavg
                sig = signature(node, state)
                state.signature_time_cost[sig] = (metadata.threadtime + get(state.signature_time_cost, sig, 0)) ÷ 2
                state.signature_alloc_cost[sig] = (metadata.gc_allocd + get(state.signature_alloc_cost, sig, 0)) ÷ 2
                if metadata.transfer_rate !== nothing
                    state.transfer_rate[] = (state.transfer_rate[] + metadata.transfer_rate) ÷ 2
                end
            end
            state.cache[node] = res
            state.errored[node] = thunk_failed
            if node.options !== nothing && node.options.checkpoint !== nothing
                try
                    @invokelatest node.options.checkpoint(node, res)
                catch err
                    report_catch_error(err, "Thunk checkpoint failed")
                end
            end

            timespan_start(ctx, :finish, thunk_id, (;thunk_id))
            finish_task!(ctx, state, node, thunk_failed)
            timespan_finish(ctx, :finish, thunk_id, (;thunk_id))
        end

        safepoint(state)
    end

    # Final value is ready
    value = state.cache[d]
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
    close(state.chan)
    notify(state.halt)
    @sync for p in procs_to_use(ctx)
        @async cleanup_proc(state, p, ctx.log_sink)
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

        populate_processor_cache_list!(state, procs)

        # Schedule tasks
        to_fire = Dict{Tuple{OSProc,<:Processor},Vector{Tuple{Thunk,<:Any,<:Any}}}()
        failed_scheduling = Thunk[]

        # Select a new task and get its options
        task = nothing
        @label pop_task
        if task !== nothing
            timespan_finish(ctx, :schedule, task.id, (;thunk_id=task.id))
        end
        if isempty(state.ready)
            @goto fire_tasks
        end
        task = pop!(state.ready)
        timespan_start(ctx, :schedule, task.id, (;thunk_id=task.id))
        if haskey(state.cache, task)
            if haskey(state.errored, task)
                # An error was eagerly propagated to this task
                finish_failed!(state, task)
            else
                # This shouldn't have happened
                iob = IOBuffer()
                println(iob, "Scheduling inconsistency: Task being scheduled is already cached!")
                println(iob, "  Task: $(task.id)")
                println(iob, "  Cache Entry: $(typeof(state.cache[task]))")
                ex = SchedulingException(String(take!(iob)))
                state.cache[task] = ex
                state.errored[task] = true
            end
            @goto pop_task
        end
        opts = merge(ctx.options, task.options)
        sig = signature(task, state)

        # Calculate scope
        scope = if task.f isa Chunk
            task.f.scope
        else
            DefaultScope()
        end
        for input in task.inputs
            input = unwrap_weak_checked(input)
            chunk = if istask(input)
                state.cache[input]
            elseif input isa Chunk
                input
            else
                nothing
            end
            chunk isa Chunk || continue
            scope = constrain(scope, chunk.scope)
            if scope isa Dagger.InvalidScope
                ex = SchedulingException("Scopes are not compatible: $(scope.x), $(scope.y)")
                state.cache[task] = ex
                state.errored[task] = true
                set_failed!(state, task)
                @goto pop_task
            end
        end

        fallback_threshold = 1024 # TODO: Parameterize this threshold
        if length(procs) > fallback_threshold
            @goto fallback
        end
        local_procs = unique(vcat([collect(Dagger.get_processors(gp)) for gp in procs]...))
        if length(local_procs) > fallback_threshold
            @goto fallback
        end

        inputs = collect_task_inputs(state, task)
        opts = populate_defaults(opts, chunktype(task.f), map(chunktype, inputs))
        local_procs, costs = estimate_task_costs(state, local_procs, task, inputs)
        scheduled = false

        # Move our corresponding ThreadProc to be the last considered
        if length(local_procs) > 1
            sch_threadproc = Dagger.ThreadProc(myid(), Threads.threadid())
            sch_thread_idx = findfirst(proc->proc==sch_threadproc, local_procs)
            if sch_thread_idx !== nothing
                deleteat!(local_procs, sch_thread_idx)
                push!(local_procs, sch_threadproc)
            end
        end

        for proc in local_procs
            gproc = get_parent(proc)
            if can_use_proc(task, gproc, proc, opts, scope)
                has_cap, est_time_util, est_alloc_util = has_capacity(state, proc, gproc.pid, opts.time_util, opts.alloc_util, sig)
                if has_cap
                    # Schedule task onto proc
                    # FIXME: est_time_util = est_time_util isa MaxUtilization ? cap : est_time_util
                    push!(get!(()->Vector{Tuple{Thunk,<:Any,<:Any}}(), to_fire, (gproc, proc)), (task, est_time_util, est_alloc_util))
                    state.worker_time_pressure[gproc.pid][proc] += est_time_util
                    @goto pop_task
                end
            end
        end
        state.cache[task] = SchedulingException("No processors available, try widening scope")
        state.errored[task] = true
        set_failed!(state, task)
        @goto pop_task

        # Fast fallback algorithm, used when the smarter cost model algorithm
        # would be too expensive
        @label fallback
        selected_entry = nothing
        entry = state.procs_cache_list[]
        cap, extra_util = nothing, nothing
        procs_found = false
        # N.B. if we only have one processor, we need to select it now
        if can_use_proc(task, entry.gproc, entry.proc, opts, scope)
            has_cap, est_time_util, est_alloc_util = has_capacity(state, entry.proc, entry.gproc.pid, opts.time_util, opts.alloc_util, sig)
            if has_cap
                selected_entry = entry
            else
                procs_found = true
                entry = entry.next
            end
        else
            entry = entry.next
        end
        while selected_entry === nothing
            if entry === state.procs_cache_list[]
                # Exhausted all procs
                if procs_found
                    push!(failed_scheduling, task)
                else
                    state.cache[task] = SchedulingException("No processors available, try widening scope")
                    state.errored[task] = true
                    set_failed!(state, task)
                end
                @goto pop_task
            end

            if can_use_proc(task, entry.gproc, entry.proc, opts, scope)
                has_cap, est_time_util, est_alloc_util = has_capacity(state, entry.proc, entry.gproc.pid, opts.time_util, opts.alloc_util, sig)
                if has_cap
                    # Select this processor
                    selected_entry = entry
                else
                    # We could have selected it otherwise
                    procs_found = true
                    entry = entry.next
                end
            else
                # Try next processor
                entry = entry.next
            end
        end
        @assert selected_entry !== nothing

        # Schedule task onto proc
        gproc, proc = entry.gproc, entry.proc
        est_time_util = est_time_util isa MaxUtilization ? cap : est_time_util
        push!(get!(()->Vector{Tuple{Thunk,<:Any,<:Any}}(), to_fire, (gproc, proc)), (task, est_time_util, est_alloc_util))

        # Proceed to next entry to spread work
        state.procs_cache_list[] = state.procs_cache_list[].next
        @goto pop_task

        # Fire all newly-scheduled tasks
        @label fire_tasks
        for gpp in keys(to_fire)
            fire_tasks!(ctx, to_fire[gpp], gpp, state)
        end

        append!(state.ready, failed_scheduling)
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

        timespan_start(ctx, :assign_procs, 0, 0)

        # Load new set of procs
        new_ps = procs_to_use(ctx)

        # Initialize new procs
        diffps = setdiff(new_ps, old_ps)
        for p in diffps
            init_proc(state, p, ctx.log_sink)

            # Empty the processor cache list and force reschedule
            lock(state.lock) do
                state.procs_cache_list[] = nothing
            end
            put!(state.chan, RescheduleSignal())
        end

        # Cleanup removed procs
        diffps = setdiff(old_ps, new_ps)
        for p in diffps
            cleanup_proc(state, p, ctx.log_sink)

            # Empty the processor cache list
            lock(state.lock) do
                state.procs_cache_list[] = nothing
            end
        end

        timespan_finish(ctx, :assign_procs, 0, 0)
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
    state.procs_cache_list[] = nothing
end

function finish_task!(ctx, state, node, thunk_failed)
    pop!(state.running, node)
    delete!(state.running_on, node)
    if thunk_failed
        set_failed!(state, node)
    end
    if node.cache
        node.cache_ref = state.cache[node]
    end
    schedule_dependents!(state, node, thunk_failed)
    fill_registered_futures!(state, node, thunk_failed)

    to_evict = cleanup_inputs!(state, node)
    if node.f isa Chunk
        # FIXME: Check the graph for matching chunks
        push!(to_evict, node.f)
    end
    if haskey(state.waiting_data, node) && isempty(state.waiting_data[node])
        delete!(state.waiting_data, node)
    end
    evict_all_chunks!(ctx, to_evict)
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
    ctx = Context([myid()];log_sink) 
    for chunk in chunks
        lock(TASK_SYNC) do
            timespan_start(ctx, :evict, myid(), (;data=chunk))
            haskey(CHUNK_CACHE, chunk) && delete!(CHUNK_CACHE, chunk)
            timespan_finish(ctx, :evict, myid(), (;data=chunk))
        end
    end
    nothing
end

fire_task!(ctx, thunk::Thunk, p, state; time_util=10^9, alloc_util=10^6) =
    fire_task!(ctx, (thunk, time_util, alloc_util), p, state)
fire_task!(ctx, (thunk, time_util, alloc_util)::Tuple{Thunk,<:Any}, p, state) =
    fire_tasks!(ctx, [(thunk, time_util, alloc_util)], p, state)
function fire_tasks!(ctx, thunks::Vector{<:Tuple}, (gproc, proc), state)
    to_send = []
    for (thunk, time_util, alloc_util) in thunks
        push!(state.running, thunk)
        state.running_on[thunk] = gproc
        if thunk.cache && thunk.cache_ref !== nothing
            # the result might be already cached
            data = thunk.cache_ref
            if data !== nothing
                # cache hit
                state.cache[thunk] = data
                thunk_failed = get(state.errored, thunk, false)
                finish_task!(ctx, state, thunk, thunk_failed)
                continue
            else
                # cache miss
                thunk.cache_ref = nothing
            end
        end
        if thunk.options !== nothing && thunk.options.restore !== nothing
            try
                result = @invokelatest thunk.options.restore(thunk)
                if result isa Chunk
                    state.cache[thunk] = result
                    state.errored[thunk] = false
                    finish_task!(ctx, state, thunk, false)
                    continue
                elseif result !== nothing
                    throw(ArgumentError("Invalid restore return type: $(typeof(result))"))
                end
            catch err
                report_catch_error(err, "Thunk restore failed")
            end
        end

        ids = Int[0]
        data = Any[thunk.f]
        for (idx, x) in enumerate(thunk.inputs)
            x = unwrap_weak_checked(x)
            push!(ids, istask(x) ? x.id : -idx)
            push!(data, istask(x) ? state.cache[x] : x)
        end
        toptions = thunk.options !== nothing ? thunk.options : ThunkOptions()
        options = merge(ctx.options, toptions)
        propagated = get_propagated_options(thunk)
        @assert (options.single === nothing) || (gproc.pid == options.single)
        # TODO: Set `sch_handle.tid.ref` to the right `DRef`
        sch_handle = SchedulerHandle(ThunkID(thunk.id, nothing), state.worker_chans[gproc.pid]...)

        # TODO: De-dup common fields (log_sink, uid, etc.)
        push!(to_send, Any[thunk.id, time_util, alloc_util, chunktype(thunk.f), data, thunk.get_result,
                           thunk.persist, thunk.cache, thunk.meta, options,
                           propagated, ids,
                           (log_sink=ctx.log_sink, profile=ctx.profile),
                           sch_handle, state.uid])
    end
    # N.B. We don't batch these because we might get a deserialization
    # error due to something not being defined on the worker, and then we don't
    # know which task failed.
    tasks = Task[]
    for ts in to_send
        # TODO: errormonitor
        @async begin
            timespan_start(ctx, :fire, gproc.pid, 0)
            try
                remotecall_wait(do_tasks, gproc.pid, proc, state.chan, [ts])
            catch err
                bt = catch_backtrace()
                thunk_id = ts[1]
                put!(state.chan, (gproc.pid, proc, thunk_id, (CapturedException(err, bt), nothing)))
            finally
                timespan_finish(ctx, :fire, gproc.pid, 0)
            end
        end
    end
end

"""
    do_tasks(to_proc, chan, tasks)

Executes a batch of tasks on `to_proc`.
"""
function do_tasks(to_proc, chan, tasks)
    for task in tasks
        thunk_id = task[1]
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
        @async begin
            try
                result = do_task(to_proc, task)
                put!(chan, (myid(), to_proc, thunk_id, result))
            catch ex
                bt = catch_backtrace()
                put!(chan, (myid(), to_proc, thunk_id, (CapturedException(ex, bt), nothing)))
            end
        end
    end
end
"Executes a single task on `to_proc`."
function do_task(to_proc, comm)
    thunk_id, est_time_util, est_alloc_util, Tf, data, send_result, persist, cache, meta, options, propagated, ids, ctx_vars, sch_handle, uid = comm
    ctx = Context(Processor[]; log_sink=ctx_vars.log_sink, profile=ctx_vars.profile)

    from_proc = OSProc()
    Tdata = Any[]
    for x in data
        push!(Tdata, chunktype(x))
    end
    f = isdefined(Tf, :instance) ? Tf.instance : nothing

    # Wait for required resources to become available
    to_storage = options.storage !== nothing ? fetch(options.storage) : MemPool.GLOBAL_DEVICE[]
    to_storage_name = nameof(typeof(to_storage))
    storage_cap = storage_capacity(to_storage)

    timespan_start(ctx, :storage_wait, thunk_id, (;f, to_proc, device=typeof(to_storage)))
    real_time_util = Ref{UInt64}(0)
    real_alloc_util = UInt64(0)
    if !meta
        # Factor in the memory costs for our lazy arguments
        for arg in data[2:end]
            if arg isa Chunk
                est_alloc_util += arg.handle.size
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
            time_dict = get!(()->Dict{Processor,Ref{UInt64}}(), PROCESSOR_TIME_UTILIZATION, uid)
            real_time_util = get!(()->Ref{UInt64}(UInt64(0)), time_dict, to_proc)

            # Get current allocation utilization and capacity
            real_alloc_util = storage_utilized(to_storage)
            storage_cap = storage_capacity(to_storage)

            # Check if we'll go over memory capacity from running this thunk
            # Waits for free storage, if necessary
            #= TODO: Implement a priority queue, ordered by est_alloc_util
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
            =#
            # FIXME
            break
        end
    end
    timespan_finish(ctx, :storage_wait, thunk_id, (;f, to_proc, device=typeof(to_storage)))

    # Initiate data transfers for function and arguments
    transfer_time = Threads.Atomic{UInt64}(0)
    transfer_size = Threads.Atomic{UInt64}(0)
    _data, _ids = if meta
        (Any[first(data)], Int[first(ids)]) # always fetch function
    else
        (data, ids)
    end
    fetch_tasks = map(Iterators.zip(_data,_ids)) do (x, id)
        @async begin
            timespan_start(ctx, :move, (;thunk_id, id), (;f, id, data=x))
            x = if x isa Chunk
                value = lock(TASK_SYNC) do
                    if haskey(CHUNK_CACHE, x)
                        Some{Any}(get!(CHUNK_CACHE[x], to_proc) do
                            # Convert from cached value
                            # TODO: Choose "closest" processor of same type first
                            some_proc = first(keys(CHUNK_CACHE[x]))
                            some_x = CHUNK_CACHE[x][some_proc]
                            move(some_proc, to_proc, some_x)
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
                    _x = move(to_proc, x)
                    time_finish = time_ns()
                    if x.handle.size !== nothing
                        Threads.atomic_add!(transfer_time, time_finish - time_start)
                        Threads.atomic_add!(transfer_size, x.handle.size)
                    end

                    # Update cache
                    lock(TASK_SYNC) do
                        CHUNK_CACHE[x] = Dict{Processor,Any}()
                        CHUNK_CACHE[x][to_proc] = _x
                    end

                    _x
                end
            else
                move(to_proc, x)
            end
            timespan_finish(ctx, :move, (;thunk_id, id), (;f, id, data=x); tasks=[Base.current_task()])
            return x
        end
    end
    fetched = Any[]
    for task in fetch_tasks
        push!(fetched, fetch_report(task))
    end
    if meta
        append!(fetched, data[2:end])
    end
    f = popfirst!(fetched)
    @assert !(f isa Chunk) "Failed to unwrap thunk function"

    #= FIXME: If MaxUtilization, stop processors and wait
    if (est_time_util isa MaxUtilization) && (real_time_util > 0)
        # FIXME: Stop processors
        # FIXME: Wait on processors to stop
        est_time_util = count(c->typeof(c)===typeof(to_proc), children(from_proc))
    end
    =#

    real_time_util[] += est_time_util
    timespan_start(ctx, :compute, thunk_id, (;f, to_proc))
    res = nothing

    # Start counting time and GC allocations
    threadtime_start = cputhreadtime()
    # FIXME
    #gcnum_start = Base.gc_num()

    result_meta = try
        # Set TLS variables
        Dagger.set_tls!((
            sch_uid=uid,
            sch_handle=sch_handle,
            processor=to_proc,
            time_utilization=est_time_util,
            alloc_utilization=est_alloc_util,
        ))

        res = Dagger.with_options(propagated) do
            # Execute
            execute!(to_proc, f, fetched...)
        end

        # Check if result is safe to store
        device = nothing
        if !(res isa Chunk)
            timespan_start(ctx, :storage_safe_scan, thunk_id, (;T=typeof(res)))
            device = if walk_storage_safe(res)
                to_storage
            else
                MemPool.CPURAMDevice()
            end
            timespan_finish(ctx, :storage_safe_scan, thunk_id, (;T=typeof(res)))
        end

        # Construct result
        # TODO: We should cache this locally
        send_result || meta ? res : tochunk(res, to_proc; device, persist, cache=persist ? true : cache)
    catch ex
        bt = catch_backtrace()
        RemoteException(myid(), CapturedException(ex, bt))
    end
    threadtime = cputhreadtime() - threadtime_start
    # FIXME: This is not a realistic measure of max. required memory
    #gc_allocd = min(max(UInt64(Base.gc_num().allocd) - UInt64(gcnum_start.allocd), UInt64(0)), UInt64(1024^4))
    timespan_finish(ctx, :compute, thunk_id, (;f, to_proc))
    lock(TASK_SYNC) do
        real_time_util[] -= est_time_util
        pop!(TASKS_RUNNING, thunk_id)
        notify(TASK_SYNC)
    end
    # TODO: debug_storage("Releasing $to_storage_name")
    metadata = (
        time_pressure=real_time_util[],
        storage_pressure=real_alloc_util,
        storage_capacity=storage_cap,
        loadavg=((Sys.loadavg()...,) ./ Sys.CPU_THREADS),
        threadtime=threadtime,
        # FIXME: Add runtime allocation tracking
        gc_allocd=(isa(result_meta, Chunk) ? result_meta.handle.size : 0),
        transfer_rate=(transfer_size[] > 0 && transfer_time[] > 0) ? round(UInt64, transfer_size[] / (transfer_time[] / 10^9)) : nothing,
    )
    (result_meta, metadata)
end

end # module Sch
