module Sch

using Distributed
import MemPool: DRef

import ..Dagger: Context, Processor, Thunk, Chunk, OSProc, order, free!, dependents, noffspring, istask, inputs, affinity, tochunk, @dbg, @logmsg, timespan_start, timespan_end, unrelease, procs, move, capacity, default_enabled, get_processors, choose_processor, execute!, rmprocs!, addprocs!

include("fault-handler.jl")

const OneToMany = Dict{Thunk, Set{Thunk}}

"""
    ComputeState

The internal state-holding struct of the scheduler.

Fields
- dependents::OneToMany - The result of calling `dependents` on the DAG
- finished::Set{Thunk} - The set of completed `Thunk`s
- waiting::OneToMany - Map from parent `Thunk` to child `Thunk`s that still need to execute
- waiting_data::OneToMany - Map from child `Thunk` to all parent `Thunk`s, accumulating over time
- ready::Vector{Thunk} - The list of `Thunk`s that are ready to execute
- cache::Dict{Thunk, Any} - Maps from a finished `Thunk` to it's cached result, often a DRef
- running::Set{Thunk} - The set of currently-running `Thunk`s
- thunk_dict::Dict{Int, Any} - Maps from thunk IDs to a `Thunk`
- node_order::Any - Function that returns the order of a thunk
- worker_procs::Dict{Int,OSProc} - Maps from worker ID to root processor
- worker_pressure::Dict{Int,Int} - Cache of worker pressure
- worker_capacity::Dict{Int,Int} - Maps from worker ID to capacity
"""
struct ComputeState
    dependents::OneToMany
    finished::Set{Thunk}
    waiting::OneToMany
    waiting_data::OneToMany
    ready::Vector{Thunk}
    cache::Dict{Thunk, Any}
    running::Set{Thunk}
    thunk_dict::Dict{Int, Any}
    node_order::Any
    worker_procs::Dict{Int,OSProc}
    worker_pressure::Dict{Int,Dict{Type,Float64}}
    worker_capacity::Dict{Int,Dict{Type,Float64}}
end

"""
    SchedulerOptions

Stores DAG-global options to be passed to the Dagger.Sch scheduler.

# Arguments
- `single::Int=0`: Force all work onto worker with specified id. `0` disables this option.
- `proclist::Vector{Type{<:Processor}}=Type[]`: Force scheduler to use one or
more processors that are instances/subtypes of a contained type. Leave this
vector empty to disable.
"""
Base.@kwdef struct SchedulerOptions
    single::Int = 0
    proclist = nothing
end

"""
    ThunkOptions

Stores Thunk-local options to be passed to the Dagger.Sch scheduler.

# Arguments
- `single::Int=0`: Force thunk onto worker with specified id. `0` disables this option.
- `proclist=nothing`: Force thunk to use one or more processors that are
instances/subtypes of a contained type. Alternatively, a function can be
supplied, and the function will be called with a processor as the sole
argument and should return a `Bool` result to indicate whether or not to use
the given processor. `nothing` enables all default processors.
- `procutil::Dict{Type,Any}=Dict{Type,Any}()`: Indicates the maximum expected
processor utilization for this thunk. Each keypair maps a processor type to
the utilization, where the value can be a real (approx. the number of processors
of this type utilized), or `MaxUtilization()` (utilizes all processors of this
type) By default, the scheduler assumes that this thunk only uses one processor.
"""
Base.@kwdef struct ThunkOptions
    single::Int = 0
    proclist = nothing
    procutil::Dict{Type,Any} = Dict{Type,Any}()
end

"Combine `SchedulerOptions` and `ThunkOptions` into a new `ThunkOptions`."
function merge(sopts::SchedulerOptions, topts::ThunkOptions)
    single = topts.single != 0 ? topts.single : sopts.single
    ThunkOptions(single, topts.proclist, topts.procutil)
end
merge(sopts::SchedulerOptions, ::Nothing) =
    ThunkOptions(sopts.single, sopts.proclist, Dict{Type,Any}())

function cleanup(ctx)
end

"Process-local count of actively-executing Dagger tasks per processor type."
const ACTIVE_TASKS = Dict{Type,Ref{Float64}}()
const ACTIVE_TASKS_LOCK = ReentrantLock()

"Process-local condition variable indicating task completion."
const TASK_SYNC = Condition()

"Indicates that a thunk uses all processors of a given type."
struct MaxUtilization end

function compute_dag(ctx, d::Thunk; options=SchedulerOptions())
    if options === nothing
        options = SchedulerOptions()
    end
    ctx.options = options
    master = OSProc(myid())
    @dbg timespan_start(ctx, :scheduler_init, 0, master)

    chan = Channel{Any}(32)
    deps = dependents(d)
    ord = order(d, noffspring(deps))

    node_order = x -> -get(ord, x, 0)
    state = start_state(deps, node_order)

    # Initialize procs, pressure, and capacity
    @sync for p in procs_to_use(ctx)
        @async begin
            proc = OSProc(p.pid)
            state.worker_procs[p.pid] = proc
            state.worker_pressure[p.pid] = Dict{Type,Float64}()
            state.worker_capacity[p.pid] = Dict{Type,Float64}()
            for T in unique(typeof.(get_processors(proc)))
                state.worker_pressure[p.pid][T] = 0
                state.worker_capacity[p.pid][T] = capacity(proc, T)
            end
            state.worker_pressure[p.pid][OSProc] = 0
            state.worker_capacity[p.pid][OSProc] = 0
        end
    end

    # start off some tasks
    # Note: procs_state may be different things for different contexts. Don't touch it out here!
    procs_state = assign_new_procs!(ctx, state, chan, procs_to_use(ctx))
    @dbg timespan_end(ctx, :scheduler_init, 0, master)

    # Loop while we still have thunks to execute
    while !isempty(state.ready) || !isempty(state.running)
        if isempty(state.running) && !isempty(state.ready)
            # Nothing running, so schedule up to N thunks, 1 per N workers
            schedule!(ctx, state, chan, procs_state)
        end

        # This is a bit redundant as the @async task below does basically the
        # same job Without it though, testing of process modification becomes
        # non-deterministic (due to sleep in CI environment) which is why it is
        # still here.
        procs_state = assign_new_procs!(ctx, state, chan, procs_state)

        if isempty(state.running)
            # the block above fired only meta tasks
            continue
        end
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
                assign_new_procs!(ctx, state, chan, procs_state)
            end
        end

        proc, thunk_id, res, metadata = take!(chan) # get result of completed thunk
        lock(newtasks_lock) # This waits for any assign_new_procs! above to complete and then shuts down the task
        if isa(res, CapturedException) || isa(res, RemoteException)
            if check_exited_exception(res)
                @warn "Worker $(proc.pid) died on thunk $thunk_id, rescheduling work"

                # Remove dead worker from procs list
                remove_dead_proc!(ctx, proc)

                handle_fault(ctx, state, state.thunk_dict[thunk_id], proc, chan)
                continue
            else
                throw(res)
            end
        end
        state.worker_pressure[proc.pid][metadata.pressure_type] = metadata.pressure
        node = state.thunk_dict[thunk_id]
        @logmsg("WORKER $(proc.pid) - $node ($(node.f)) input:$(node.inputs)")
        state.cache[node] = res

        @dbg timespan_start(ctx, :scheduler, thunk_id, master)
        immediate_next = finish_task!(state, node)
        if !isempty(state.ready) && !shall_remove_proc(ctx, proc)
            pop_and_fire!(ctx, state, chan, proc; immediate_next=immediate_next)
        end
        @dbg timespan_end(ctx, :scheduler, thunk_id, master)
    end
    state.cache[d]
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

function schedule!(ctx, state, chan, procs=procs_to_use(ctx))
    @assert length(procs) > 0
    proc_keys = map(x->x.pid, procs)
    proc_set = Set{Any}()
    for p in proc_keys
        for proc in get_processors(OSProc(p))
            push!(proc_set, p=>proc)
        end
    end
    progress = false
    was_empty = isempty(state.ready)
    failed_scheduling = Thunk[]
    while !isempty(state.ready)
        task = pop!(state.ready)
        opts = merge(ctx.options, task.options)
        proclist = opts.proclist
        proc_set_useable = if proclist === nothing
            filter(x->default_enabled(x[2]), proc_set)
        elseif proclist isa Function
            filter(x->proclist(x[2], x[1]), proc_set)
        else
            filter(x->x[2] in proclist, proc_set)
        end
        @assert !isempty(proc_set_useable) "No processors available, try making proclist more liberal"
        procutil = opts.procutil
        proc = nothing
        extra_util = nothing
        cap = nothing
        for (gp,p) in proc_set_useable
            T = typeof(p)
            extra_util = get(procutil, T, 1)
            real_util = state.worker_pressure[gp][T]
            cap = capacity(OSProc(gp), T)
            if ((extra_util isa MaxUtilization) && (real_util > 0)) ||
               ((extra_util isa Real) && (extra_util + real_util > cap))
                continue
            else
                proc = OSProc(gp), p
                break
            end
        end
        if proc !== nothing
            extra_util = extra_util isa MaxUtilization ? cap : extra_util
            fire_task!(ctx, task, proc, state, chan; util=extra_util)
            progress = true
            continue
        else
            push!(failed_scheduling, task)
        end
    end
    append!(state.ready, failed_scheduling)
    @assert was_empty || progress || !isempty(state.running) "Failed to make forward progress"
    return progress
end
function pop_and_fire!(ctx, state, chan, proc; immediate_next=false)
    task = pop_with_affinity!(ctx, state.ready, proc, immediate_next)
    if task !== nothing
        fire_task!(ctx, task, proc, state, chan)
        return true
    end
    return false
end

# Main responsibility of this function is to check if new procs have been pushed to the context
function assign_new_procs!(ctx, state, chan, assignedprocs)
    ps = procs_to_use(ctx)
    # Must track individual procs to handle the case when procs are removed
    diffps = setdiff(ps, assignedprocs)
    if !isempty(diffps)
        schedule!(ctx, state, chan, diffps)
    end
    return ps
end

# Might be a good policy to not remove the proc if immediate_next
shall_remove_proc(ctx, proc) = proc ∉ procs_to_use(ctx)

function remove_dead_proc!(ctx, proc, options=ctx.options)
    @assert options.single !== proc.pid "Single worker failed, cannot continue."
    rmprocs!(ctx, [proc])
end

function pop_with_affinity!(ctx, tasks, proc, immediate_next)
    # allow JIT specialization on Pairs
    mapfirst(c) = first.(c)

    if immediate_next
        # fast path
        if proc in mapfirst(affinity(tasks[end]))
            return pop!(tasks)
        end
    end

    # TODO: use the size
    parent_affinity_procs = Vector(undef, length(tasks))
    # parent_affinity_sizes = Vector(undef, length(tasks))
    for i=length(tasks):-1:1
        t = tasks[i]
        aff = affinity(t)
        aff_procs = mapfirst(aff)
        if proc in aff_procs
            deleteat!(tasks, i)
            return t
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
            deleteat!(tasks, i)
            return t
        end
        if all(!(p in aff_procs) for p in procs(ctx))
            # no proc is ever going to ask for it
            t = tasks[i]
            deleteat!(tasks, i)
            return t
        end
    end
    return nothing
end

fire_task!(ctx, thunk, proc::OSProc, state, chan; util=1.0) =
    fire_task!(ctx, thunk, (proc, proc), state, chan; util=util)
function fire_task!(ctx, thunk, (gproc, proc), state, chan; util=1.0)
    push!(state.running, thunk)
    if thunk.cache && thunk.cache_ref !== nothing
        # the result might be already cached
        data = unrelease(thunk.cache_ref) # ask worker to keep the data around
                                          # till this compute cycle frees it
        if data !== nothing
            @logmsg("cache hit: $(thunk.cache_ref)")
            state.cache[thunk] = data
            immediate_next = finish_task!(state, thunk; free=false)
            if !isempty(state.ready)
                pop_and_fire!(ctx, state, chan, gproc; immediate_next=immediate_next)
            end
            return
        else
            thunk.cache_ref = nothing
            @logmsg("cache miss: $(thunk.cache_ref) recomputing $(thunk)")
        end
    end

    ids = map(enumerate(thunk.inputs)) do (idx,x)
        istask(x) ? x.id : -idx
    end
    if thunk.meta
        # Run it on the parent node, do not move data.
        p = OSProc(myid())
        fetched = map(Iterators.zip(thunk.inputs,ids)) do (x, id)
            @dbg timespan_start(ctx, :comm, (thunk.id, id), (thunk.f, id))
            x = istask(x) ? state.cache[x] : x
            @dbg timespan_end(ctx, :comm, (thunk.id, id), (thunk.f, id))
            return x
        end

        @dbg timespan_start(ctx, :compute, thunk.id, (thunk.f, p))
        Threads.atomic_add!(ACTIVE_TASKS, 1)
        res = thunk.f(fetched...)
        Threads.atomic_sub!(ACTIVE_TASKS, 1)
        @dbg timespan_end(ctx, :compute, thunk.id, (thunk.f, p))

        #push!(state.running, thunk)
        state.cache[thunk] = res
        immediate_next = finish_task!(state, thunk; free=false)
        if !isempty(state.ready)
            pop_and_fire!(ctx, state, chan, gproc; immediate_next=immediate_next)
        end
        return
    end

    data = map(thunk.inputs) do x
        istask(x) ? state.cache[x] : x
    end
    state.thunk_dict[thunk.id] = thunk
    toptions = thunk.options !== nothing ? thunk.options : ThunkOptions()
    options = merge(ctx.options, toptions)
    state.worker_pressure[gproc.pid][typeof(proc)] += util
    async_apply((gproc, proc), thunk.id, thunk.f, data, chan, thunk.get_result, thunk.persist, thunk.cache, options, ids, ctx.log_sink)
end

function finish_task!(state, node; free=true)
    if istask(node) && node.cache
        node.cache_ref = state.cache[node]
    end
    immediate_next = false
    for dep in sort!(collect(state.dependents[node]), by=state.node_order)
        set = state.waiting[dep]
        pop!(set, node)
        if isempty(set)
            pop!(state.waiting, dep)
            push!(state.ready, dep)
            immediate_next = true
        end
        # todo: free data
    end
    for inp in inputs(node)
        if inp in keys(state.waiting_data)
            s = state.waiting_data[inp]
            if node in s
                pop!(s, node)
            end
            if free && isempty(s)
                if haskey(state.cache, inp)
                    _node = state.cache[inp]
                    free!(_node, force=false, cache=(istask(inp) && inp.cache))
                    pop!(state.cache, inp)
                end
            end
        end
    end
    push!(state.finished, node)
    pop!(state.running, node)
    immediate_next
end

function start_state(deps::Dict, node_order)
    state = ComputeState(
                  deps,
                  Set{Thunk}(),
                  OneToMany(),
                  OneToMany(),
                  Vector{Thunk}(undef, 0),
                  Dict{Thunk, Any}(),
                  Set{Thunk}(),
                  Dict{Int, Thunk}(),
                  node_order,
                  Dict{Int,OSProc}(),
                  Dict{Int,Dict{Type,Float64}}(),
                  Dict{Int,Dict{Type,Float64}}(),
                 )

    nodes = sort(collect(keys(deps)), by=node_order)
    merge!(state.waiting_data, deps)
    for k in nodes
        if istask(k)
            waiting = Set{Thunk}(Iterators.filter(istask,
                                                  inputs(k)))
            if isempty(waiting)
                push!(state.ready, k)
            else
                state.waiting[k] = waiting
            end
        end
    end
    state
end

@noinline function do_task(to_proc, thunk_id, f, data, send_result, persist, cache, options, ids, log_sink)
    ctx = Context(Processor[]; log_sink=log_sink)
    from_proc = OSProc()
    fetched = map(Iterators.zip(data,ids)) do (x, id)
        @dbg timespan_start(ctx, :comm, (thunk_id, id), (f, id))
        x = x isa Union{Chunk,Thunk} ? collect(ctx, x) : x
        @dbg timespan_end(ctx, :comm, (thunk_id, id), (f, id))
        return x
    end

    # TODO: Time choose_processor?
    if to_proc isa Integer
        to_proc = choose_processor(from_proc, options, f, fetched)
    end
    fetched = map(Iterators.zip(fetched,ids)) do (x, id)
        @dbg timespan_start(ctx, :move, (thunk_id, id), (f, id))
        x = move(from_proc, to_proc, x)
        @dbg timespan_end(ctx, :move, (thunk_id, id), (f, id))
        return x
    end

    # Check if we'll go over capacity from running this thunk
    extra_util = get(options.procutil, typeof(to_proc), 1)
    real_util = lock(ACTIVE_TASKS_LOCK) do
        get!(()->Ref{Float64}(0.0), ACTIVE_TASKS, typeof(to_proc))
    end
    cap = capacity(OSProc(), typeof(to_proc))
    while true
        if ((extra_util isa MaxUtilization) && (real_util[] > 0)) ||
           ((extra_util isa Integer) && (extra_util + real_util[] > cap))
            # Fully subscribed, wait and re-check
            @info "($(myid())) Waiting for free $(typeof(to_proc)): $extra_util | $(real_util[])/$cap"
            wait(TASK_SYNC)
        else
            # Under-subscribed, calculate extra utilization and execute thunk
            @info "($(myid())) Using available $to_proc: $extra_util | $(real_util[])/$cap"
            extra_util = if extra_util isa MaxUtilization
                count(c->typeof(c)===typeof(to_proc), from_proc.children)
            else
                extra_util
            end
            lock(ACTIVE_TASKS_LOCK) do
                real_util[] += Float64(extra_util)
            end
            break
        end
    end

    @dbg timespan_start(ctx, :compute, thunk_id, (f, to_proc))
    res = nothing
    result_meta = try
        # Set TLS variables
        task_local_storage(:processor, to_proc)

        # Execute
        res = execute!(to_proc, f, fetched...)

        # Construct result
        send_result ? res : tochunk(res, to_proc; persist=persist, cache=persist ? true : cache)
    catch ex
        bt = catch_backtrace()
        RemoteException(myid(), CapturedException(ex, bt))
    end
    @dbg timespan_end(ctx, :compute, thunk_id, (f, to_proc))
    lock(ACTIVE_TASKS_LOCK) do
        real_util[] -= Float64(extra_util)
    end
    notify(TASK_SYNC)
    metadata = (pressure=real_util[], pressure_type=typeof(to_proc))
    (from_proc, thunk_id, result_meta, metadata)
end

@noinline function async_apply((gp,p), thunk_id, f, data, chan, send_res, persist, cache, options, ids, log_sink)
    @async begin
        p = p isa OSProc ? p.pid : p
        try
            put!(chan, remotecall_fetch(do_task, gp.pid, p, thunk_id, f, data, send_res, persist, cache, options, ids, log_sink))
        catch ex
            bt = catch_backtrace()
            put!(chan, (gp, thunk_id, CapturedException(ex, bt), nothing))
        end
        nothing
    end
end

end # module Sch
