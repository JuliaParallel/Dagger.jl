module Sch

using Distributed
import MemPool: DRef

import ..Dagger
import ..Dagger: Context, Processor, Thunk, ThunkFuture, ThunkFailedException, Chunk, OSProc, order, free!, dependents, noffspring, istask, inputs, affinity, tochunk, @dbg, @logmsg, timespan_start, timespan_end, unrelease, procs, move, capacity, chunktype, choose_processor, execute!, rmprocs!, addprocs!, thunk_processor

const OneToMany = Dict{Thunk, Set{Thunk}}

include("util.jl")
include("fault-handler.jl")
include("dynamic.jl")

"""
    ComputeState

The internal state-holding struct of the scheduler.

Fields
- dependents::OneToMany - The result of calling `dependents` on the DAG
- finished::Set{Thunk} - The set of completed `Thunk`s
- waiting::OneToMany - Map from downstream `Thunk` to upstream `Thunk`s that still need to execute
- waiting_data::OneToMany - Map from upstream `Thunk` to all downstream `Thunk`s, accumulating over time
- ready::Vector{Thunk} - The list of `Thunk`s that are ready to execute
- cache::Dict{Thunk, Any} - Maps from a finished `Thunk` to it's cached result, often a DRef
- running::Set{Thunk} - The set of currently-running `Thunk`s
- thunk_dict::Dict{Int, Any} - Maps from thunk IDs to a `Thunk`
- node_order::Any - Function that returns the order of a thunk
- worker_pressure::Dict{Int,Int} - Cache of worker pressure
- worker_capacity::Dict{Int,Int} - Maps from worker ID to capacity
- worker_chans::Dict{Int, Tuple{RemoteChannel,RemoteChannel}} - Communication channels between the scheduler and each worker
- halt::Ref{Bool} - Flag indicating, when set, that the scheduler should halt immediately
- lock::ReentrantLock() - Lock around operations which modify the state
- futures::Dict{Thunk, Vector{ThunkFuture}} - Futures registered for waiting on the result of a thunk.
- errored::Set{Thunk} - Thunks that threw an error
- chan::Channel - Channel for receiving completed thunks
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
    worker_pressure::Dict{Int,Int}
    worker_capacity::Dict{Int,Int}
    worker_chans::Dict{Int, Tuple{RemoteChannel,RemoteChannel}}
    halt::Ref{Bool}
    lock::ReentrantLock
    futures::Dict{Thunk, Vector{ThunkFuture}}
    errored::Set{Thunk}
    chan::Channel{Any}
end

"""
    SchedulerOptions

Stores DAG-global options to be passed to the Dagger.Sch scheduler.

# Arguments
- `single::Int=0`: Force all work onto worker with specified id. `0` disables this option.
- `proctypes::Vector{Type{<:Processor}}=Type[]`: Force scheduler to use one or
more processors that are instances/subtypes of a contained type. Leave this
vector empty to disable.
- `allow_errors::Bool=true`: Allow thunks to error without affecting
non-dependent thunks.
"""
Base.@kwdef struct SchedulerOptions
    single::Int = 0
    proctypes::Vector{Type} = Type[]
    allow_errors::Bool = false
end

"""
    ThunkOptions

Stores Thunk-local options to be passed to the Dagger.Sch scheduler.

# Arguments
- `single::Int=0`: Force thunk onto worker with specified id. `0` disables this option.
- `proctypes::Vector{Type{<:Processor}}=Type[]`: Force thunk to use one or
more processors that are instances/subtypes of a contained type. Leave this
vector empty to disable.
- `allow_errors::Bool=true`: Allow this thunk to error without affecting
non-dependent thunks.
"""
Base.@kwdef struct ThunkOptions
    single::Int = 0
    proctypes::Vector{Type} = Type[]
    allow_errors::Bool = false
end

# Eager scheduling
include("eager.jl")

"Combine `SchedulerOptions` and `ThunkOptions` into a new `ThunkOptions`."
function merge(sopts::SchedulerOptions, topts::ThunkOptions)
    single = topts.single != 0 ? topts.single : sopts.single
    proctypes = vcat(sopts.proctypes, topts.proctypes)
    allow_errors = sopts.allow_errors || topts.allow_errors
    ThunkOptions(single, proctypes, allow_errors)
end

function isrestricted(task::Thunk, proc::OSProc)
    if (task.options !== nothing) && (task.options.single != 0) &&
       (task.options.single != proc.pid)
        return true
    end
    return false
end

function cleanup(ctx)
end

function init_proc(state, p)
    # Initialize pressure and capacity
    lock(state.lock) do
        state.worker_pressure[p.pid] = 0
        state.worker_capacity[p.pid] = 1
    end
    cap = remotecall_fetch(capacity, p.pid)
    @async lock(state.lock) do
        state.worker_capacity[p.pid] = cap
    end

    # Setup worker-to-scheduler channels
    inp_chan = RemoteChannel(p.pid)
    out_chan = RemoteChannel(p.pid)
    lock(state.lock) do
        state.worker_chans[p.pid] = (inp_chan, out_chan)
    end
end

"Process-local count of actively-executing Dagger tasks."
const ACTIVE_TASKS = Threads.Atomic{Int}(0)

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
    state = start_state(deps, node_order, chan)

    # setup thunk_dict mappings
    for node in keys(deps)
        state.thunk_dict[node.id] = node
        for dep in deps[node]
            state.thunk_dict[dep.id] = dep
        end
    end

    @sync for p in procs_to_use(ctx)
        @async init_proc(state, p)
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
        pid, thunk_id, (res, metadata) = take!(chan) # get result of completed thunk
        proc = OSProc(pid)
        lock(newtasks_lock) # This waits for any assign_new_procs! above to complete and then shuts down the task
        safepoint(state)
        thunk_failed = false
        if res isa Exception
            if unwrap_nested_exception(res) isa Union{ProcessExitedException, Base.IOError}
                @warn "Worker $(proc.pid) died on thunk $thunk_id, rescheduling work"

                # Remove dead worker from procs list
                remove_dead_proc!(ctx, state, proc)

                lock(state.lock) do
                    handle_fault(ctx, state, state.thunk_dict[thunk_id], proc)
                end
                continue
            else
                if ctx.options.allow_errors || state.thunk_dict[thunk_id].options.allow_errors
                    thunk_failed = true
                else
                    throw(res)
                end
            end
        end
        state.worker_pressure[proc.pid] = metadata.pressure
        node = state.thunk_dict[thunk_id]
        state.cache[node] = res

        # FIXME: Move log start and lock to before error check
        @dbg timespan_start(ctx, :finish, thunk_id, master)
        lock(state.lock) do
            finish_task!(state, node, thunk_failed)
        end
        @dbg timespan_end(ctx, :finish, thunk_id, master)

        safepoint(state)
    end
    value = state.cache[d] # TODO: move(OSProc(), state.cache[d])
    if d in state.errored
        throw(value)
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

function schedule!(ctx, state, procs=procs_to_use(ctx))
    lock(state.lock) do
        safepoint(state)
        @assert length(procs) > 0
        proc_keys = map(x->x.pid, procs)
        proc_ratios = Dict(p=>(state.worker_pressure[p]/state.worker_capacity[p]) for p in proc_keys)
        proc_ratios_sorted = sort(proc_keys, lt=(a,b)->proc_ratios[a]<proc_ratios[b])
        id = popfirst!(proc_ratios_sorted)
        was_empty = isempty(state.ready)
        todo = Thunk[]
        while !isempty(state.ready)
            if (state.worker_pressure[id] >= state.worker_capacity[id])
                # TODO: provide forward progress guarantees at user's request
                if isempty(proc_ratios_sorted)
                    break
                end
                id = popfirst!(proc_ratios_sorted)
            end
            proc = OSProc(id)
            if !pop_and_fire!(ctx, state, proc)
                # Internal scheduler gave up, so we force scheduling
                task = pop!(state.ready)
                if isrestricted(task, proc)
                   push!(todo, task)
                   continue
                end
                fire_task!(ctx, task, proc, state)
            end
        end
        for task in todo
            id = task.options.single
            if state.worker_pressure[id] < state.worker_capacity[id]
                fire_task!(ctx, task, OSProc(id), state)
            end
        end
        if !isempty(state.ready)
            # we still have work we can schedule, so oversubscribe with round-robin
            # TODO: if we're going to oversubcribe, do it intelligently
            for p in procs
                isempty(state.ready) && break
                pop_and_fire!(ctx, state, p)
            end
        end
    end
end
function pop_and_fire!(ctx, state, proc)
    task = pop_with_affinity!(ctx, state.ready, proc)
    if task !== nothing
        fire_task!(ctx, task, proc, state)
        return true
    end
    return false
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

function fire_task!(ctx, thunk, proc, state)
    push!(state.running, thunk)
    if thunk.cache && thunk.cache_ref !== nothing
        # the result might be already cached
        data = unrelease(thunk.cache_ref) # ask worker to keep the data around
                                          # till this compute cycle frees it
        if data !== nothing
            # cache hit
            state.cache[thunk] = data
            thunk_failed = thunk in state.errored
            finish_task!(state, thunk, thunk_failed; free=false)
            if !isempty(state.ready)
                pop_and_fire!(ctx, state, proc)
            end
            return
        else
            # cache miss
            thunk.cache_ref = nothing
        end
    end

    ids = map(enumerate(thunk.inputs)) do (idx,x)
        istask(x) ? x.id : -idx
    end

    data = map(thunk.inputs) do x
        istask(x) ? state.cache[x] : x
    end
    toptions = thunk.options !== nothing ? thunk.options : ThunkOptions()
    options = merge(ctx.options, toptions)
    @assert (options.single == 0) || (proc.pid == options.single)
    sch_handle = SchedulerHandle(ThunkID(thunk.id), state.worker_chans[proc.pid]...)
    state.worker_pressure[proc.pid] += 1
    async_apply(proc, thunk.id, thunk.f, data, state.chan, thunk.get_result,
                thunk.persist, thunk.cache, options, ids, ctx.log_sink,
                sch_handle, thunk.meta)
end

function finish_task!(state, node, thunk_failed; free=true)
    pop!(state.running, node)
    if !thunk_failed
        push!(state.finished, node)
    else
        set_failed!(state, node)
    end
    if istask(node) && node.cache
        node.cache_ref = state.cache[node]
    end
    if !thunk_failed
        for dep in sort!(collect(state.dependents[node]), by=state.node_order)
            set = state.waiting[dep]
            node in set && pop!(set, node)
            if isempty(set)
                pop!(state.waiting, dep)
                push!(state.ready, dep)
            end
            # todo: free data
        end
        if haskey(state.futures, node)
            # Notify any listening thunks
            for future in state.futures[node]
                if istask(node) && haskey(state.cache, node)
                    put!(future, state.cache[node])
                else
                    put!(future, nothing)
                end
            end
            delete!(state.futures, node)
        end
    end
    # Internal clean-up
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
end

function start_state(deps::Dict, node_order, chan)
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
                  Dict{Int,Int}(),
                  Dict{Int,Int}(),
                  Dict{Int, Tuple{RemoteChannel,RemoteChannel}}(),
                  Ref{Bool}(false),
                  ReentrantLock(),
                  Dict{Thunk, Vector{ThunkFuture}}(),
                  Set{Thunk}(),
                  chan,
                 )

    nodes = sort(collect(keys(deps)), by=node_order)
    # N.B. Using merge! here instead would modify deps
    for (key,val) in deps
        state.waiting_data[key] = copy(val)
    end
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

@noinline function do_task(thunk_id, f, data, send_result, persist, cache, options, ids, log_sink, sch_handle, meta)
    ctx = Context(Processor[]; log_sink=log_sink)
    # TODO: Time choose_processor
    Tdata = map(x->x isa Chunk ? chunktype(x) : x, data)
    to_proc = choose_processor(options, f, Tdata)
    fetched = if meta
        data
    else
        fetch.(map(Iterators.zip(data,ids)) do (x, id)
            @async begin
                @dbg timespan_start(ctx, :move, (thunk_id, id), (f, id))
                x = move(to_proc, x)
                @dbg timespan_end(ctx, :move, (thunk_id, id), (f, id))
                return x
            end
        end)
    end
    @dbg timespan_start(ctx, :compute, thunk_id, f)
    res = nothing
    result_meta = try
        # Set TLS variables
        task_local_storage(:processor, to_proc)
        task_local_storage(:sch_handle, sch_handle)

        # Execute
        Threads.atomic_add!(ACTIVE_TASKS, 1)
        res = execute!(to_proc, f, fetched...)
        Threads.atomic_sub!(ACTIVE_TASKS, 1)

        # Construct result
        send_result || meta ? res : tochunk(res, to_proc; persist=persist, cache=persist ? true : cache)
    catch ex
        bt = catch_backtrace()
        RemoteException(myid(), CapturedException(ex, bt))
    end
    @dbg timespan_end(ctx, :compute, thunk_id, (f, to_proc, typeof(res), sizeof(res)))
    metadata = (pressure=ACTIVE_TASKS[],)
    (result_meta, metadata)
end

@noinline function async_apply(p::OSProc, thunk_id, f, data, chan, send_res, persist, cache, options, ids, log_sink, sch_handle, meta)
    @async begin
        try
            put!(chan, (p.pid, thunk_id, remotecall_fetch(do_task, p.pid, thunk_id, f, data,
                                                          send_res, persist, cache, options, ids,
                                                          log_sink, sch_handle, meta)))
        catch ex
            bt = catch_backtrace()
            put!(chan, (p.pid, thunk_id, (CapturedException(ex, bt), nothing)))
        end
        nothing
    end
end

end # module Sch
