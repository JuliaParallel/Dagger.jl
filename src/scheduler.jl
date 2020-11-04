module Sch

using Distributed
import MemPool: DRef

import ..Dagger: Context, Processor, Thunk, Chunk, OSProc, order, free!, dependents, noffspring, istask, inputs, affinity, tochunk, @dbg, @logmsg, timespan_start, timespan_end, unrelease, procs, move, capacity, choose_processor, execute!, rmprocs!, addprocs!

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
    worker_pressure::Dict{Int,Int}
    worker_capacity::Dict{Int,Int}
end

"""
    SchedulerOptions

Stores DAG-global options to be passed to the Dagger.Sch scheduler.

# Arguments
- `single::Int=0`: Force all work onto worker with specified id. `0` disables this option.
- `proctypes::Vector{Type{<:Processor}}=Type[]`: Force scheduler to use one or
more processors that are instances/subtypes of a contained type. Leave this
vector empty to disable.
"""
Base.@kwdef struct SchedulerOptions
    single::Int = 0
    proctypes::Vector{Type} = Type[]
end

"""
    ThunkOptions

Stores Thunk-local options to be passed to the Dagger.Sch scheduler.

# Arguments
- `single::Int=0`: Force thunk onto worker with specified id. `0` disables this option.
- `proctypes::Vector{Type{<:Processor}}=Type[]`: Force thunk to use one or
more processors that are instances/subtypes of a contained type. Leave this
vector empty to disable.
"""
Base.@kwdef struct ThunkOptions
    single::Int = 0
    proctypes::Vector{Type} = Type[]
end

"Combine `SchedulerOptions` and `ThunkOptions` into a new `ThunkOptions`."
function merge(sopts::SchedulerOptions, topts::ThunkOptions)
    single = topts.single != 0 ? topts.single : sopts.single
    proctypes = vcat(sopts.proctypes, topts.proctypes)
    ThunkOptions(single, proctypes)
end

function cleanup(ctx)
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
    state = start_state(deps, node_order)

    # Initialize pressure and capacity
    @sync for p in procs_to_use(ctx)
        state.worker_pressure[p.pid] = 0
        @async state.worker_capacity[p.pid] = remotecall_fetch(capacity, p.pid)
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
        state.worker_pressure[proc.pid] = metadata.pressure
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
    proc_ratios = Dict(p=>(state.worker_pressure[p]/state.worker_capacity[p]) for p in proc_keys)
    proc_ratios_sorted = sort(proc_keys, lt=(a,b)->proc_ratios[a]<proc_ratios[b])
    progress = false
    id = popfirst!(proc_ratios_sorted)
    was_empty = isempty(state.ready)
    while !isempty(state.ready)
        if (state.worker_pressure[id] >= state.worker_capacity[id])
            # TODO: provide forward progress guarantees at user's request
            if isempty(proc_ratios_sorted)
                break
            end
            id = popfirst!(proc_ratios_sorted)
        end
        if !pop_and_fire!(ctx, state, chan, OSProc(id))
            # Internal scheduler gave up, so we force scheduling
            task = pop!(state.ready)
            fire_task!(ctx, task, OSProc(id), state, chan)
        end
        progress = true
    end
    if !isempty(state.ready)
        # we still have work we can schedule, so oversubscribe with round-robin
        # TODO: if we're going to oversubcribe, do it intelligently
        for p in procs
            isempty(state.ready) && break
            progress |= pop_and_fire!(ctx, state, chan, p)
        end
    end
    @assert was_empty || progress
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

function fire_task!(ctx, thunk, proc, state, chan)
    @logmsg("W$(proc.pid) + $thunk ($(showloc(thunk.f, length(thunk.inputs)))) input:$(thunk.inputs) cache:$(thunk.cache) $(thunk.cache_ref)")
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
                pop_and_fire!(ctx, state, chan, proc; immediate_next=immediate_next)
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

        @dbg timespan_start(ctx, :compute, thunk.id, thunk.f)
        Threads.atomic_add!(ACTIVE_TASKS, 1)
        res = thunk.f(fetched...)
        Threads.atomic_sub!(ACTIVE_TASKS, 1)
        @dbg timespan_end(ctx, :compute, thunk.id, (thunk.f, p, typeof(res), sizeof(res)))

        #push!(state.running, thunk)
        state.cache[thunk] = res
        immediate_next = finish_task!(state, thunk; free=false)
        if !isempty(state.ready)
            pop_and_fire!(ctx, state, chan, proc; immediate_next=immediate_next)
        end
        return
    end

    data = map(thunk.inputs) do x
        istask(x) ? state.cache[x] : x
    end
    state.thunk_dict[thunk.id] = thunk
    toptions = thunk.options !== nothing ? thunk.options : ThunkOptions()
    options = merge(ctx.options, toptions)
    state.worker_pressure[proc.pid] += 1
    async_apply(proc, thunk.id, thunk.f, data, chan, thunk.get_result, thunk.persist, thunk.cache, options, ids, ctx.log_sink)
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
                  Dict{Int,Int}(),
                  Dict{Int,Int}(),
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

@noinline function do_task(thunk_id, f, data, send_result, persist, cache, options, ids, log_sink)
    ctx = Context(Processor[]; log_sink=log_sink)
    from_proc = OSProc()
    fetched = map(Iterators.zip(data,ids)) do (x, id)
        @dbg timespan_start(ctx, :comm, (thunk_id, id), (f, id))
        x = x isa Union{Chunk,Thunk} ? collect(ctx, x) : x
        @dbg timespan_end(ctx, :comm, (thunk_id, id), (f, id))
        return x
    end

    # TODO: Time choose_processor?
    to_proc = choose_processor(from_proc, options, f, fetched)
    fetched = map(Iterators.zip(fetched,ids)) do (x, id)
        @dbg timespan_start(ctx, :move, (thunk_id, id), (f, id))
        x = move(from_proc, to_proc, x)
        @dbg timespan_end(ctx, :move, (thunk_id, id), (f, id))
        return x
    end
    @dbg timespan_start(ctx, :compute, thunk_id, f)
    res = nothing
    result_meta = try
        # Set TLS variables
        task_local_storage(:processor, to_proc)

        # Execute
        Threads.atomic_add!(ACTIVE_TASKS, 1)
        res = execute!(to_proc, f, fetched...)
        Threads.atomic_sub!(ACTIVE_TASKS, 1)

        # Construct result
        send_result ? res : tochunk(res, to_proc; persist=persist, cache=persist ? true : cache)
    catch ex
        bt = catch_backtrace()
        RemoteException(myid(), CapturedException(ex, bt))
    end
    @dbg timespan_end(ctx, :compute, thunk_id, (f, to_proc, typeof(res), sizeof(res)))
    metadata = (pressure=ACTIVE_TASKS[],)
    (from_proc, thunk_id, result_meta, metadata)
end

@noinline function async_apply(p::OSProc, thunk_id, f, data, chan, send_res, persist, cache, options, ids, log_sink)
    @async begin
        try
            put!(chan, remotecall_fetch(do_task, p.pid, thunk_id, f, data, send_res, persist, cache, options, ids, log_sink))
        catch ex
            bt = catch_backtrace()
            put!(chan, (p, thunk_id, CapturedException(ex, bt), nothing))
        end
        nothing
    end
end

end # module Sch
