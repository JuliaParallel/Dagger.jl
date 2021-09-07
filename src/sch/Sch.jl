module Sch

using Distributed
import MemPool: DRef, poolset

import ..Dagger
import ..Dagger: Context, Processor, Thunk, WeakThunk, ThunkFuture, ThunkFailedException, Chunk, OSProc, AnyScope
import ..Dagger: order, free!, dependents, noffspring, istask, inputs, unwrap_weak_checked, affinity, tochunk, @dbg, @logmsg, timespan_start, timespan_end, unrelease, procs, move, capacity, chunktype, default_enabled, get_processors, execute!, rmprocs!, addprocs!, thunk_processor, constrain, cputhreadtime

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

"""
    ComputeState

The internal state-holding struct of the scheduler.

Fields:
- `uid::UInt64` - Unique identifier for this scheduler instance
- `waiting::OneToMany` - Map from downstream `Thunk` to upstream `Thunk`s that still need to execute
- `waiting_data::Dict{Union{Thunk,Chunk},Set{Thunk}}` - Map from input `Chunk`/upstream `Thunk` to all unfinished downstream `Thunk`s, to retain caches
- `ready::Vector{Thunk}` - The list of `Thunk`s that are ready to execute
- `cache::Dict{Thunk, Any}` - Maps from a finished `Thunk` to it's cached result, often a DRef
- `running::Set{Thunk}` - The set of currently-running `Thunk`s
- `running_on::Dict{Thunk,OSProc}` - Map from `Thunk` to the OS process executing it
- `thunk_dict::Dict{Int, WeakThunk}` - Maps from thunk IDs to a `Thunk`
- `node_order::Any` - Function that returns the order of a thunk
- `worker_pressure::Dict{Int,Dict{Type,UInt64}}` - Cache of worker pressure
- `worker_capacity::Dict{Int,Dict{Type,UInt64}}` - Maps from worker ID to capacity
- `worker_loadavg::Dict{Int,NTuple{3,Float64}}` - Worker load average
- `worker_chans::Dict{Int, Tuple{RemoteChannel,RemoteChannel}}` - Communication channels between the scheduler and each worker
- `procs_cache_list::Base.RefValue{Union{ProcessorCacheEntry,Nothing}}` - Cached linked list of processors ready to be used
- `function_cost_cache::Dict{Type{<:Tuple},UInt64}` - Cache of estimated CPU time required to compute the given signature
- `halt::Base.Event` - Event indicating that the scheduler is halting
- `lock::ReentrantLock` - Lock around operations which modify the state
- `futures::Dict{Thunk, Vector{ThunkFuture}}` - Futures registered for waiting on the result of a thunk.
- `errored::WeakKeyDict{Thunk,Bool}` - Indicates if a thunk's result is due to an error.
- `chan::RemoteChannel{Channel{Any}}` - Channel for receiving completed thunks.
"""
struct ComputeState
    uid::UInt64
    waiting::OneToMany
    waiting_data::Dict{Union{Thunk,Chunk},Set{Thunk}}
    ready::Vector{Thunk}
    cache::WeakKeyDict{Thunk, Any}
    running::Set{Thunk}
    running_on::Dict{Thunk,OSProc}
    thunk_dict::Dict{Int, WeakThunk}
    node_order::Any
    worker_pressure::Dict{Int,Dict{Type,UInt64}}
    worker_capacity::Dict{Int,Dict{Type,UInt64}}
    worker_loadavg::Dict{Int,NTuple{3,Float64}}
    worker_chans::Dict{Int, Tuple{RemoteChannel,RemoteChannel}}
    procs_cache_list::Base.RefValue{Union{ProcessorCacheEntry,Nothing}}
    function_cost_cache::Dict{Type{<:Tuple},UInt64}
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
                         Dict{Thunk, Any}(),
                         Set{Thunk}(),
                         Dict{Thunk,OSProc}(),
                         Dict{Int, WeakThunk}(),
                         node_order,
                         Dict{Int,Dict{Type,UInt64}}(),
                         Dict{Int,Dict{Type,UInt64}}(),
                         Dict{Int,NTuple{3,Float64}}(),
                         Dict{Int, Tuple{RemoteChannel,RemoteChannel}}(),
                         Ref{Union{ProcessorCacheEntry,Nothing}}(nothing),
                         Dict{Type{<:Tuple},UInt64}(),
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
    merge(sopts, topts)

Combine `SchedulerOptions` and `ThunkOptions` into a new `ThunkOptions`.
"""
function merge(sopts::SchedulerOptions, topts::ThunkOptions)
    single = topts.single != 0 ? topts.single : sopts.single
    allow_errors = sopts.allow_errors || topts.allow_errors
    proclist = topts.proclist !== nothing ? topts.proclist : sopts.proclist
    ThunkOptions(single, proclist, topts.procutil, allow_errors, topts.checkpoint, topts.restore)
end
merge(sopts::SchedulerOptions, ::Nothing) =
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
function init_proc(state, p)
    # Initialize pressure and capacity
    proc = OSProc(p.pid)
    lock(state.lock) do
        state.worker_pressure[p.pid] = Dict{Type,UInt64}()
        state.worker_capacity[p.pid] = Dict{Type,UInt64}()
        state.worker_loadavg[p.pid] = (0.0, 0.0, 0.0)
        for T in unique(typeof.(get_processors(proc)))
            state.worker_pressure[p.pid][T] = 0
            state.worker_capacity[p.pid][T] = capacity(proc, T) * UInt64(1e9)
        end
        state.worker_pressure[p.pid][OSProc] = 0
        state.worker_capacity[p.pid][OSProc] = 0
    end
    cap = remotecall(capacity, p.pid)
    @async begin
        cap = fetch(cap) * UInt64(1e9)
        lock(state.lock) do
            state.worker_capacity[p.pid] = cap
        end
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
end
function _cleanup_proc(uid)
    empty!(CHUNK_CACHE) # FIXME: Should be keyed on uid!
end
function cleanup_proc(state, p)
    lock(WORKER_MONITOR_LOCK) do
        wid = p.pid
        if haskey(WORKER_MONITOR_CHANS, wid)
            delete!(WORKER_MONITOR_CHANS[wid], state.uid)
            remote_do(_cleanup_proc, wid, state.uid)
        end
    end
end

"Process-local condition variable (and lock) indicating task completion."
const TASK_SYNC = Threads.Condition()

"Process-local set of running task IDs."
const TASKS_RUNNING = Set{Int}()

"Process-local dictionary tracking per-processor total utilization."
const PROC_UTILIZATION = Dict{UInt64,Dict{Type,Ref{UInt64}}}()

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
    master = OSProc(myid())
    @dbg timespan_start(ctx, :scheduler_init, 0, master)

    chan = RemoteChannel(()->Channel(1024))
    deps = dependents(d)
    ord = order(d, noffspring(deps))

    node_order = x -> -get(ord, x, 0)
    state = start_state(deps, node_order, chan)

    # setup thunk_dict mappings
    for node in filter(istask, keys(deps))
        state.thunk_dict[node.id] = WeakThunk(node)
        for dep in deps[node]
            state.thunk_dict[dep.id] = WeakThunk(dep)
        end
    end

    # Initialize procs, pressure, and capacity
    @sync for p in procs_to_use(ctx)
        @async begin
            try
                init_proc(state, p)
            catch err
                @error "Error initializing worker $p" exception=(err,catch_backtrace())
                remove_dead_proc!(ctx, state, p)
            end
        end
    end

    # setup dynamic listeners
    dynamic_listener!(ctx, state)

    @dbg timespan_end(ctx, :scheduler_init, 0, master)

    # start off some tasks
    # Note: procs_state may be different things for different contexts. Don't touch it out here!
    procs_state = assign_new_procs!(ctx, state, procs_to_use(ctx))

    safepoint(state)

    # Loop while we still have thunks to execute
    while !isempty(state.ready) || !isempty(state.running)
        procs_state = assign_new_procs!(ctx, state, procs_state)
        if !isempty(state.ready)
            # Nothing running, so schedule up to N thunks, 1 per N workers
            schedule!(ctx, state, procs_state)
        end

        # This is a bit redundant as the @async task below does basically the
        # same job Without it though, testing of process modification becomes
        # non-deterministic (due to sleep in CI environment) which is why it is
        # still here.
        procs_state = assign_new_procs!(ctx, state, procs_state)

        check_integrity(ctx)

        # Check periodically for new workers in a parallel task so that we
        # don't accidentally end up having to wait for `take!(chan)` on some
        # large task before new workers are put to work. Locking is used to
        # stop this task as soon as something pops out from the channel to
        # minimize risk that the task schedules thunks simultaneously as the
        # main task (after future refactoring).
        newtasks_lock = ReentrantLock()
        @async while !isempty(state.ready) || !isempty(state.running)
            sleep(1)
            islocked(newtasks_lock) && return
            procs_state = lock(newtasks_lock) do
                assign_new_procs!(ctx, state, procs_state)
            end
        end

        isempty(state.running) && continue
        chan_value = take!(chan) # get result of completed thunk
        if chan_value isa RescheduleSignal
            continue
        end
        pid, proc, thunk_id, (res, metadata) = chan_value
        gproc = OSProc(pid)
        lock(newtasks_lock) # This waits for any assign_new_procs! above to complete and then shuts down the task
        safepoint(state)
        thunk_failed = false
        if res isa Exception
            if unwrap_nested_exception(res) isa ProcessExitedException
                @warn "Worker $(pid) died, rescheduling work"

                # Remove dead worker from procs list
                remove_dead_proc!(ctx, state, gproc)

                lock(state.lock) do
                    handle_fault(ctx, state, gproc)
                end
                continue
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
            state.worker_pressure[pid][typeof(proc)] = metadata.pressure
            state.worker_loadavg[pid] = metadata.loadavg
            sig = signature(node, state)
            state.function_cost_cache[sig] = (metadata.threadtime + get(state.function_cost_cache, sig, 0)) ÷ 2
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

        # FIXME: Move log start and lock to before error check
        @dbg timespan_start(ctx, :finish, thunk_id, master)
        lock(state.lock) do
            finish_task!(ctx, state, node, thunk_failed)
        end
        @dbg timespan_end(ctx, :finish, thunk_id, master)

        safepoint(state)
    end
    @assert !isready(state.chan)
    close(state.chan)
    notify(state.halt)
    @sync for p in procs_to_use(ctx)
        @async cleanup_proc(state, p)
    end
    value = state.cache[d] # TODO: move(OSProc(), state.cache[d])
    if get(state.errored, d, false)
        throw(value)
    end
    if options.checkpoint !== nothing
        try
            options.checkpoint(value)
        catch err
            report_catch_error(err, "Scheduler checkpoint failed")
        end
    end
    value
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

        # Populate the cache if empty
        if state.procs_cache_list[] === nothing
            current = nothing
            for p in map(x->x.pid, procs)
                for proc in get_processors(OSProc(p))
                    next = ProcessorCacheEntry(OSProc(p), proc)
                    if current === nothing
                        current = next
                        current.next = current
                        state.procs_cache_list[] = current
                    else
                        current.next = next
                        current = next
                        current.next = state.procs_cache_list[]
                    end
                end
            end
            # FIXME: Sort by lowest absolute utilization
        end

        function can_use_proc(task, gproc, proc, opts, scope)
            # Check against proclist
            if opts.proclist === nothing
                if !default_enabled(proc)
                    return false
                end
            elseif opts.proclist isa Function
                if !Base.invokelatest(opts.proclist, proc)
                    return false
                end
            elseif opts.proclist isa Vector
                if !(typeof(proc) in opts.proclist)
                    return false
                end
            else
                throw(SchedulingException("proclist must be a Function, Vector, or nothing"))
            end

            # Check against single
            if opts.single != 0
                if gproc.pid != opts.single
                    return false
                end
            end

            # Check scope
            if constrain(scope, Dagger.ExactScope(proc)) isa Dagger.InvalidScope
                return false
            end

            return true
        end
        function has_capacity(p, gp, procutil, sig)
            T = typeof(p)
            # FIXME: MaxUtilization
            extra_util = round(UInt64, get(procutil, T, 1) * 1e9)
            real_util = state.worker_pressure[gp][T]
            if (T === Dagger.ThreadProc) && haskey(state.function_cost_cache, sig)
                # Assume that the extra pressure is between estimated and measured
                # TODO: Generalize this to arbitrary processor types
                extra_util = min(extra_util, state.function_cost_cache[sig])
            end
            # TODO: update real_util based on loadavg
            cap = state.worker_capacity[gp][T]
            if ((extra_util isa MaxUtilization) && (real_util > 0)) ||
               ((extra_util isa Real) && (extra_util + real_util > cap))
                return false, cap, extra_util
            end
            return true, cap, extra_util
        end

        # Schedule tasks
        to_fire = Dict{Tuple{OSProc,<:Processor},Vector{Tuple{Thunk,<:Any}}}()
        failed_scheduling = Thunk[]

        # Select a new task and get its options
        @label pop_task
        if isempty(state.ready)
            @goto fire_tasks
        end
        task = pop!(state.ready)
        @assert !haskey(state.cache, task)
        opts = merge(ctx.options, task.options)
        sig = signature(task, state)

        # Calculate scope
        scope = AnyScope()
        for input in unwrap_weak_checked.(task.inputs)
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

        # Try to select a processor
        selected_entry = nothing
        entry = state.procs_cache_list[]
        cap, extra_util = nothing, nothing
        procs_found = false
        # N.B. if we only have one processor, we need to select it now
        if can_use_proc(task, entry.gproc, entry.proc, opts, scope)
            has_cap, cap, extra_util = has_capacity(entry.proc, entry.gproc.pid, opts.procutil, sig)
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
                has_cap, cap, extra_util = has_capacity(entry.proc, entry.gproc.pid, opts.procutil, sig)
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

        # Progress through list
        if ctx.options.round_robin
            # Proceed to next entry to spread work
            state.procs_cache_list[] = state.procs_cache_list[].next
            @goto pop_task
        end
        util = state.worker_pressure[gproc.pid][typeof(proc)]
        if util >= cap
            # Proceed to next entry due to over-pressure
            state.procs_cache_list[] = state.procs_cache_list[].next
        end
        @goto pop_task

        # Fire all newly-scheduled tasks
        @label fire_tasks
        @sync for gpp in keys(to_fire)
            @async fire_tasks!(ctx, to_fire[gpp], gpp, state)
        end
        append!(state.ready, failed_scheduling)
    end
end

# Main responsibility of this function is to check if new procs have been pushed to the context
function assign_new_procs!(ctx, state, assignedprocs)
    ps = procs_to_use(ctx)
    # Must track individual procs to handle the case when procs are removed
    diffps = setdiff(ps, assignedprocs)
    if !isempty(diffps)
        for p in diffps
            init_proc(state, p)
        end
        state.procs_cache_list[] = nothing
        schedule!(ctx, state, diffps)
    end
    return ps
end

shall_remove_proc(ctx, proc) = proc ∉ procs_to_use(ctx)

function remove_dead_proc!(ctx, state, proc, options=ctx.options)
    @assert options.single !== proc.pid "Single worker failed, cannot continue."
    rmprocs!(ctx, [proc])
    delete!(state.worker_pressure, proc.pid)
    delete!(state.worker_capacity, proc.pid)
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
    if haskey(state.waiting_data, node) && isempty(state.waiting_data[node])
        delete!(state.waiting_data, node)
    end
    evict_all_chunks!(ctx, to_evict)
end

function evict_all_chunks!(ctx, to_evict)
    if !isempty(to_evict)
        @sync for w in map(p->p.pid, procs_to_use(ctx))
            @async remote_do(evict_chunks!, w, to_evict)
        end
    end
end
function evict_chunks!(chunks::Set{Chunk})
    for chunk in chunks
        haskey(CHUNK_CACHE, chunk) && delete!(CHUNK_CACHE, chunk)
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
            data = unrelease(thunk.cache_ref) # ask worker to keep the data around
                                              # till this compute cycle frees it
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

        ids = map(enumerate(thunk.inputs)) do (idx,x)
            istask(x) ? unwrap_weak_checked(x).id : -idx
        end

        data = map(thunk.inputs) do x
            istask(x) ? state.cache[unwrap_weak_checked(x)] : x
        end
        toptions = thunk.options !== nothing ? thunk.options : ThunkOptions()
        options = merge(ctx.options, toptions)
        @assert (options.single == 0) || (gproc.pid == options.single)
        # TODO: Set `sch_handle.tid.ref` to the right `DRef`
        sch_handle = SchedulerHandle(ThunkID(thunk.id, nothing), state.worker_chans[gproc.pid]...)
        state.worker_pressure[gproc.pid][typeof(proc)] += util

        # TODO: De-dup common fields (log_sink, uid, etc.)
        push!(to_send, (util, thunk.id, thunk.f, data, thunk.get_result,
                        thunk.persist, thunk.cache, thunk.meta, options, ids,
                        (log_sink=ctx.log_sink, profile=ctx.profile),
                        sch_handle, state.uid))
    end
    try
        remotecall_wait(do_tasks, gproc.pid, proc, state.chan, to_send)
    catch
        # We might get a deserialization error due to something not being
        # defined on the worker; in this case, we re-fire one task at a time to
        # determine which task failed
        for ts in to_send
            try
                remotecall_wait(do_tasks, gproc.pid, proc, state.chan, [ts])
            catch err
                bt = catch_backtrace()
                put!(state.chan, (gproc.pid, proc, ts[2], (CapturedException(err, bt), nothing)))
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
                result = do_task(to_proc, task...)
                put!(chan, (myid(), to_proc, task[2], result))
            catch ex
                bt = catch_backtrace()
                put!(chan, (myid(), to_proc, task[2], (CapturedException(ex, bt), nothing)))
            end
        end
    end
end
"Executes a single task on `to_proc`."
function do_task(to_proc, extra_util, thunk_id, f, data, send_result, persist, cache, meta, options, ids, ctx_vars, sch_handle, uid)
    ctx = Context(Processor[]; log_sink=ctx_vars.log_sink, profile=ctx_vars.profile)

    from_proc = OSProc()
    Tdata = map(x->x isa Chunk ? chunktype(x) : x, data)

    # Fetch inputs
    fetched = if meta
        data
    else
        fetch_report.(map(Iterators.zip(data,ids)) do (x, id)
            @async begin
                @dbg timespan_start(ctx, :move, (thunk_id, id), (f, id))
                x = if x isa Chunk
                    if haskey(CHUNK_CACHE, x)
                        get!(CHUNK_CACHE[x], to_proc) do
                            # TODO: Choose "closest" processor of same type first
                            some_proc = first(keys(CHUNK_CACHE[x]))
                            some_x = CHUNK_CACHE[x][some_proc]
                            move(some_proc, to_proc, some_x)
                        end
                    else
                        _x = move(to_proc, x)
                        CHUNK_CACHE[x] = Dict{Processor,Any}()
                        CHUNK_CACHE[x][to_proc] = _x
                        _x
                    end
                else
                    move(to_proc, x)
                end
                @dbg timespan_end(ctx, :move, (thunk_id, id), (f, id))
                return x
            end
        end)
    end

    # Check if we'll go over capacity from running this thunk
    real_util = lock(TASK_SYNC) do
        AT = get!(()->Dict{Type,Ref{UInt64}}(), PROC_UTILIZATION, uid)
        get!(()->Ref{UInt64}(UInt64(0)), AT, typeof(to_proc))
    end
    cap = UInt64(capacity(OSProc(), typeof(to_proc))) * UInt64(1e9)
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

    @dbg timespan_start(ctx, :compute, thunk_id, (f, to_proc))
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

        # Execute
        res = execute!(to_proc, f, fetched...)

        # Construct result
        send_result || meta ? res : tochunk(res, to_proc; persist=persist, cache=persist ? true : cache)
    catch ex
        bt = catch_backtrace()
        RemoteException(myid(), CapturedException(ex, bt))
    end
    threadtime = cputhreadtime() - threadtime_start
    @dbg timespan_end(ctx, :compute, thunk_id, (f, to_proc))
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
    )
    (result_meta, metadata)
end

end # module Sch
