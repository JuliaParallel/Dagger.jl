module Sch

using Distributed
import MemPool: DRef, poolset
import Statistics: mean
import Random: randperm

import ..Dagger
import ..Dagger: Context, Processor, Thunk, WeakThunk, ThunkFuture, ThunkFailedException, Chunk, OSProc, AnyScope
import ..Dagger: order, dependents, noffspring, istask, inputs, unwrap_weak_checked, affinity, tochunk, timespan_start, timespan_finish, procs, move, capacity, chunktype, processor, default_enabled, get_processors, get_parent, execute!, rmprocs!, addprocs!, thunk_processor, constrain, cputhreadtime

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
- `worker_pressure::Dict{Int,Dict{Processor,UInt64}}` - Cache of worker pressure
- `worker_capacity::Dict{Int,Dict{Processor,UInt64}}` - Maps from worker ID to capacity
- `worker_loadavg::Dict{Int,NTuple{3,Float64}}` - Worker load average
- `worker_chans::Dict{Int, Tuple{RemoteChannel,RemoteChannel}}` - Communication channels between the scheduler and each worker
- `procs_cache_list::Base.RefValue{Union{ProcessorCacheEntry,Nothing}}` - Cached linked list of processors ready to be used
- `function_cost_cache::Dict{Signature,UInt64}` - Cache of estimated CPU time required to compute the given signature
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
    worker_pressure::Dict{Int,Dict{Processor,UInt64}}
    worker_capacity::Dict{Int,Dict{Processor,UInt64}}
    worker_loadavg::Dict{Int,NTuple{3,Float64}}
    worker_chans::Dict{Int, Tuple{RemoteChannel,RemoteChannel}}
    procs_cache_list::Base.RefValue{Union{ProcessorCacheEntry,Nothing}}
    function_cost_cache::Dict{Signature,UInt64}
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
                         Dict{Int,Dict{Type,UInt64}}(),
                         Dict{Int,Dict{Type,UInt64}}(),
                         Dict{Int,NTuple{3,Float64}}(),
                         Dict{Int, Tuple{RemoteChannel,RemoteChannel}}(),
                         Ref{Union{ProcessorCacheEntry,Nothing}}(nothing),
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
- `round_robin::Bool=false`: Whether to schedule in round-robin mode, which
spreads load instead of the default behavior of filling processors to capacity.
"""
Base.@kwdef struct SchedulerOptions
    single::Int = 0
    proclist = nothing
    allow_errors::Bool = false
    checkpoint = nothing
    restore = nothing
    round_robin::Bool = false
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
- `procutil::Dict{Type,Any}=Dict{Type,Any}()`: Indicates the maximum expected
processor utilization for this thunk. Each keypair maps a processor type to
the utilization, where the value can be a real (approx. the number of processors
of this type utilized), or `MaxUtilization()` (utilizes all processors of this
type). By default, the scheduler assumes that this thunk only uses one
processor.
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
"""
Base.@kwdef struct ThunkOptions
    single::Int = 0
    proclist = nothing
    procutil::Dict{Type,Any} = Dict{Type,Any}()
    allow_errors::Bool = false
    checkpoint = nothing
    restore = nothing
end

# Eager scheduling
include("eager.jl")

"""
    Base.merge(sopts::SchedulerOptions, topts::ThunkOptions) -> ThunkOptions

Combine `SchedulerOptions` and `ThunkOptions` into a new `ThunkOptions`.
"""
function Base.merge(sopts::SchedulerOptions, topts::ThunkOptions)
    single = topts.single != 0 ? topts.single : sopts.single
    allow_errors = sopts.allow_errors || topts.allow_errors
    proclist = topts.proclist !== nothing ? topts.proclist : sopts.proclist
    ThunkOptions(single, proclist, topts.procutil, allow_errors, topts.checkpoint, topts.restore)
end
Base.merge(sopts::SchedulerOptions, ::Nothing) =
    ThunkOptions(sopts.single, sopts.proclist, Dict{Type,Any}())

function isrestricted(task::Thunk, proc::OSProc)
    if (task.options !== nothing) && (task.options.single != 0) &&
       (task.options.single != proc.pid)
        return true
    end
    return false
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
        state.worker_pressure[p.pid] = Dict{Processor,UInt64}()
        #state.worker_capacity[p.pid] = Dict{Processor,UInt64}()
        state.worker_loadavg[p.pid] = (0.0, 0.0, 0.0)
        for proc in get_processors(gproc)
            state.worker_pressure[p.pid][proc] = 0
            #state.worker_capacity[p.pid][proc] = capacity(gproc, proc) * UInt64(1e9)
        end
        state.worker_pressure[p.pid][gproc] = 0
        #state.worker_capacity[p.pid][OSProc] = 0
    end
    #=
    cap = remotecall(capacity, p.pid)
    @async begin
        cap = fetch(cap) * UInt64(1e9)
        lock(state.lock) do
            state.worker_capacity[p.pid] = cap
        end
    end
    =#
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

"Process-local dictionary tracking per-processor total utilization."
const PROC_UTILIZATION = Dict{UInt64,Dict{Processor,Ref{UInt64}}}()

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
                    if ctx.options.allow_errors || unwrap_weak_checked(state.thunk_dict[thunk_id]).options.allow_errors
                        thunk_failed = true
                    else
                        throw(res)
                    end
                end
            end
            node = unwrap_weak_checked(state.thunk_dict[thunk_id])
            if metadata !== nothing
                state.worker_pressure[pid][proc] = metadata.pressure
                state.worker_loadavg[pid] = metadata.loadavg
                sig = signature(node, state)
                state.function_cost_cache[sig] = (metadata.threadtime + get(state.function_cost_cache, sig, 0)) ÷ 2
                if metadata.transfer_rate !== nothing
                    state.transfer_rate[] = (state.transfer_rate[] + metadata.transfer_rate) ÷ 2
                end
            end
            state.cache[node] = res
            state.errored[node] = thunk_failed
            if node.options !== nothing && node.options.checkpoint !== nothing
                try
                    node.options.checkpoint(node, res)
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
    return if options.single !== 0
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
        to_fire = Dict{Tuple{OSProc,<:Processor},Vector{Tuple{Thunk,<:Any}}}()
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
        @assert !haskey(state.cache, task)
        opts = merge(ctx.options, task.options)
        sig = signature(task, state)

        # Calculate scope
        scope = if task.f isa Chunk
            task.f.scope
        else
            AnyScope()
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
        local_procs = unique(vcat([Dagger.get_processors(gp) for gp in procs]...))
        if length(local_procs) > fallback_threshold
            @goto fallback
        end

        local_procs, costs = estimate_task_costs(state, local_procs, task)
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
                has_cap, cap, extra_util = has_capacity(state, proc, gproc.pid, opts.procutil, sig)
                if has_cap
                    # Schedule task onto proc
                    extra_util = extra_util isa MaxUtilization ? cap : extra_util
                    push!(get!(()->Vector{Tuple{Thunk,<:Any}}(), to_fire, (gproc, proc)), (task, extra_util))
                    state.worker_pressure[gproc.pid][proc] += extra_util
                    @goto pop_task
                end
            end
        end
        state.cache[task] = SchedulingException("No processors available, try making proclist more liberal")
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
            has_cap, cap, extra_util = has_capacity(state, entry.proc, entry.gproc.pid, opts.procutil, sig)
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
                    state.cache[task] = SchedulingException("No processors available, try making proclist more liberal")
                    state.errored[task] = true
                    set_failed!(state, task)
                end
                @goto pop_task
            end

            if can_use_proc(task, entry.gproc, entry.proc, opts, scope)
                has_cap, cap, extra_util = has_capacity(state, entry.proc, entry.gproc.pid, opts.procutil, sig)
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
        extra_util = extra_util isa MaxUtilization ? cap : extra_util
        push!(get!(()->Vector{Tuple{Thunk,<:Any}}(), to_fire, (gproc, proc)), (task, extra_util))

        # Proceed to next entry to spread work
        if !ctx.options.round_robin
            @warn "Round-robin mode is always on"
        end
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
    delete!(state.worker_pressure, proc.pid)
    #delete!(state.worker_capacity, proc.pid)
    delete!(state.worker_loadavg, proc.pid)
    delete!(state.worker_chans, proc.pid)
    state.procs_cache_list[] = nothing
end

function pop_with_affinity!(ctx, tasks, proc)
    # TODO: use the size
    parent_affinity_procs = Vector(undef, length(tasks))
    # parent_affinity_sizes = Vector(undef, length(tasks))
    for i=length(tasks):-1:1
        t = tasks[i]
        aff = affinity(t)
        aff_procs = first.(aff)
        if proc in aff_procs
            if !isrestricted(t,proc)
                deleteat!(tasks, i)
                return t
            end
        end
        parent_affinity_procs[i] = aff_procs
    end
    for i=length(tasks):-1:1
        # use up tasks without affinities
        # let the procs with the respective affinities pick up
        # other tasks
        aff_procs = parent_affinity_procs[i]
        if isempty(aff_procs)
            t = tasks[i]
            if !isrestricted(t,proc)
                deleteat!(tasks, i)
                return t
            end
        end
        if all(!(p in aff_procs) for p in procs(ctx))
            # no proc is ever going to ask for it
            t = tasks[i]
            if !isrestricted(t,proc)
                deleteat!(tasks, i)
                return t
            end
        end
    end
    return nothing
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
        timespan_start(ctx, :evict, myid(), (;data=chunk))
        haskey(CHUNK_CACHE, chunk) && delete!(CHUNK_CACHE, chunk)
        timespan_finish(ctx, :evict, myid(), (;data=chunk))
    end
    nothing
end

fire_task!(ctx, thunk::Thunk, p, state; util=10^9) =
    fire_task!(ctx, (thunk, util), p, state)
fire_task!(ctx, (thunk, util)::Tuple{Thunk,<:Any}, p, state) =
    fire_tasks!(ctx, [(thunk, util)], p, state)
function fire_tasks!(ctx, thunks::Vector{<:Tuple}, (gproc, proc), state)
    to_send = []
    for (thunk, util) in thunks
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
                result = thunk.options.restore(thunk)
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
        @assert (options.single == 0) || (gproc.pid == options.single)
        # TODO: Set `sch_handle.tid.ref` to the right `DRef`
        sch_handle = SchedulerHandle(ThunkID(thunk.id, nothing), state.worker_chans[gproc.pid]...)

        # TODO: De-dup common fields (log_sink, uid, etc.)
        push!(to_send, Any[util, thunk.id, fn_type(thunk.f), data, thunk.get_result,
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
                put!(state.chan, (gproc.pid, proc, ts[2], (CapturedException(err, bt), nothing)))
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
        should_launch = lock(TASK_SYNC) do
            # Already running; don't try to re-launch
            if !(task[2] in TASKS_RUNNING)
                push!(TASKS_RUNNING, task[2])
                true
            else
                false
            end
        end
        should_launch || continue
        @async begin
            try
                result = do_task(to_proc, task)
                put!(chan, (myid(), to_proc, task[2], result))
            catch ex
                bt = catch_backtrace()
                put!(chan, (myid(), to_proc, task[2], (CapturedException(ex, bt), nothing)))
            end
        end
    end
end
"Executes a single task on `to_proc`."
function do_task(to_proc, comm)
    extra_util, thunk_id, Tf, data, send_result, persist, cache, meta, options, propagated, ids, ctx_vars, sch_handle, uid = comm
    ctx = Context(Processor[]; log_sink=ctx_vars.log_sink, profile=ctx_vars.profile)

    from_proc = OSProc()
    Tdata = Any[]
    for x in data
        push!(Tdata, x isa Chunk ? chunktype(x) : x)
    end
    f = isdefined(Tf, :instance) ? Tf.instance : nothing
    f_chunk = first(data)
    scope = f_chunk isa Chunk ? f_chunk.scope : AnyScope()

    # Fetch inputs
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
                if haskey(CHUNK_CACHE, x)
                    get!(CHUNK_CACHE[x], to_proc) do
                        # TODO: Choose "closest" processor of same type first
                        some_proc = first(keys(CHUNK_CACHE[x]))
                        some_x = CHUNK_CACHE[x][some_proc]
                        move(some_proc, to_proc, some_x)
                    end
                else
                    time_start = time_ns()
                    _x = move(to_proc, x)
                    time_finish = time_ns()
                    if x.handle.size !== nothing
                        Threads.atomic_add!(transfer_time, time_finish - time_start)
                        Threads.atomic_add!(transfer_size, x.handle.size)
                    end
                    CHUNK_CACHE[x] = Dict{Processor,Any}()
                    CHUNK_CACHE[x][to_proc] = _x
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

    # Check if we'll go over capacity from running this thunk
    real_util = lock(TASK_SYNC) do
        AT = get!(()->Dict{Processor,Ref{UInt64}}(), PROC_UTILIZATION, uid)
        get!(()->Ref{UInt64}(UInt64(0)), AT, to_proc)
    end
    #cap = UInt64(capacity(OSProc(), typeof(to_proc))) * UInt64(1e9)
    cap = typemax(UInt64)

    # Wait for a free processor if necessary
    while true
        lock(TASK_SYNC)
        if ((extra_util isa MaxUtilization) && (real_util[] > 0)) ||
           ((extra_util isa Real) && (extra_util + real_util[] > cap))
            # Fully subscribed, wait and re-check
            @debug "($(myid())) $f ($thunk_id) Waiting for free $(typeof(to_proc)): $extra_util | $(real_util[])/$cap"
            wait(TASK_SYNC)
            unlock(TASK_SYNC)
        else
            # Under-subscribed, calculate extra utilization and execute thunk
            @debug "($(myid())) $f ($thunk_id) Using available $to_proc: $extra_util | $(real_util[])/$cap"
            extra_util = if extra_util isa MaxUtilization
                count(c->typeof(c)===typeof(to_proc), children(from_proc))
            else
                extra_util
            end
            real_util[] += extra_util
            unlock(TASK_SYNC)
            break
        end
    end

    timespan_start(ctx, :compute, thunk_id, (;f, to_proc))
    res = nothing
    threadtime_start = cputhreadtime()
    result_meta = try
        # Set TLS variables
        Dagger.set_tls!((
            sch_uid=uid,
            sch_handle=sch_handle,
            processor=to_proc,
            utilization=extra_util,
        ))

        res = Dagger.with_options(propagated) do
            # Execute
            execute!(to_proc, f, fetched...)
        end

        # Construct result
        send_result || meta ? res : tochunk(res, to_proc; persist=persist, cache=persist ? true : cache)
    catch ex
        bt = catch_backtrace()
        RemoteException(myid(), CapturedException(ex, bt))
    end
    threadtime = cputhreadtime() - threadtime_start
    timespan_finish(ctx, :compute, thunk_id, (;f, to_proc); tasks=Dagger.prof_tasks_take!(thunk_id))
    lock(TASK_SYNC) do
        real_util[] -= extra_util
        pop!(TASKS_RUNNING, thunk_id)
        notify(TASK_SYNC)
    end
    @debug "($(myid())) $f ($thunk_id) Releasing $(typeof(to_proc)): $extra_util | $(real_util[])/$cap"
    metadata = (
        pressure=real_util[],
        loadavg=((Sys.loadavg()...,) ./ Sys.CPU_THREADS),
        threadtime=threadtime,
        transfer_rate=(transfer_size[] > 0 && transfer_time[] > 0) ? round(UInt64, transfer_size[] / (transfer_time[] / 10^9)) : nothing,
    )
    (result_meta, metadata)
end

end # module Sch
