module Sch

using Distributed

import ..Dagger: Context, Thunk, Chunk, OSProc, order, free!, dependents, noffspring, istask, inputs, affinity, tochunk, @dbg, @logmsg, timespan_start, timespan_end, unrelease, procs

const OneToMany = Dict{Thunk, Set{Thunk}}
struct ComputeState
    dependents::OneToMany
    finished::Set{Thunk}
    waiting::OneToMany
    waiting_data::OneToMany
    ready::Vector{Thunk}
    cache::Dict{Thunk, Any}
    running::Set{Thunk}
    thunk_dict::Dict{Int, Any}
end

function cleanup(ctx)
end

function compute_dag(ctx, d::Thunk)
    master = OSProc(myid())
    @dbg timespan_start(ctx, :scheduler_init, 0, master)

    ps = procs(ctx)
    chan = Channel{Any}(32)
    deps = dependents(d)
    ord = order(d, noffspring(deps))

    node_order = x -> -get(ord, x, 0)
    state = start_state(deps, node_order)
    # start off some tasks
    for p in ps
        isempty(state.ready) && break
        task = pop_with_affinity!(ctx, state.ready, p, false)
        if task !== nothing
            fire_task!(ctx, task, p, state, chan, node_order)
        end
    end
    @dbg timespan_end(ctx, :scheduler_init, 0, master)

    while !isempty(state.ready) || !isempty(state.running)

        if isempty(state.running) && !isempty(state.ready)
            for p in ps
                isempty(state.ready) && break
                task = pop_with_affinity!(ctx, state.ready, p, false)
                if task !== nothing
                    fire_task!(ctx, task, p, state, chan, node_order)
                end
            end
        end

        if isempty(state.running)
            # the block above fired only meta tasks
            continue
        end

        proc, thunk_id, res = take!(chan)
        if isa(res, CapturedException) || isa(res, RemoteException)
            throw(res)
        end
        node = state.thunk_dict[thunk_id]
        @logmsg("WORKER $(proc.pid) - $node ($(node.f)) input:$(node.inputs)")
        state.cache[node] = res

        @dbg timespan_start(ctx, :scheduler, thunk_id, master)
        immediate_next = finish_task!(state, node, node_order)
        if !isempty(state.ready)
            thunk = pop_with_affinity!(Context(ps), state.ready, proc, immediate_next)
            if thunk !== nothing
                fire_task!(ctx, thunk, proc, state, chan, node_order)
            end
        end
        @dbg timespan_end(ctx, :scheduler, thunk_id, master)
    end
    state.cache[d]
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

function fire_task!(ctx, thunk, proc, state, chan, node_order)
    @logmsg("W$(proc.pid) + $thunk ($(showloc(thunk.f, length(thunk.inputs)))) input:$(thunk.inputs) cache:$(thunk.cache) $(thunk.cache_ref)")
    push!(state.running, thunk)
    if thunk.cache && thunk.cache_ref !== nothing
        # the result might be already cached
        data = unrelease(thunk.cache_ref) # ask worker to keep the data around
                                          # till this compute cycle frees it
        if data !== nothing
            @logmsg("cache hit: $(thunk.cache_ref)")
            state.cache[thunk] = data
            immediate_next = finish_task!(state, thunk, node_order; free=false)
            if !isempty(state.ready)
                thunk = pop_with_affinity!(ctx, state.ready, proc, immediate_next)
                if thunk !== nothing
                    fire_task!(ctx, thunk, proc, state, chan, node_order)
                end
            end
            return
        else
            thunk.cache_ref = nothing
            @logmsg("cache miss: $(thunk.cache_ref) recomputing $(thunk)")
        end
    end

    if thunk.meta
        # Run it on the parent node
        # do not _move data.
        p = OSProc(myid())
        @dbg timespan_start(ctx, :comm, thunk.id, p)
        fetched = map(thunk.inputs) do x
            istask(x) ? state.cache[x] : x
        end
        @dbg timespan_end(ctx, :comm, thunk.id, p)

        @dbg timespan_start(ctx, :compute, thunk.id, p)
        res = thunk.f(fetched...)
        @dbg timespan_end(ctx, :compute, thunk.id, p)

        #push!(state.running, thunk)
        state.cache[thunk] = res
        immediate_next = finish_task!(state, thunk, node_order; free=false)
        if !isempty(state.ready)
            if immediate_next
                thunk = pop!(state.ready)
            else
                thunk = pop_with_affinity!(ctx, state.ready, proc, immediate_next)
            end
            if thunk !== nothing
                fire_task!(ctx, thunk, proc, state, chan, node_order)
            end
        end
        return
    end

    data = map(thunk.inputs) do x
        istask(x) ? state.cache[x] : x
    end
    state.thunk_dict[thunk.id] = thunk
    async_apply(ctx, proc, thunk.id, thunk.f, data, chan, thunk.get_result, thunk.persist, thunk.cache)
end

function finish_task!(state, node, node_order; free=true)
    if istask(node) && node.cache
        node.cache_ref = state.cache[node]
    end
    immediate_next = false
    for dep in sort!(collect(state.dependents[node]), by=node_order)
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
                  Dict{Int, Thunk}()
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

_move(ctx, to_proc, x) = x
_move(ctx, to_proc::OSProc, x::Union{Chunk, Thunk}) = collect(ctx, x)

@noinline function do_task(ctx, proc, thunk_id, f, data, send_result, persist, cache)
    @dbg timespan_start(ctx, :comm, thunk_id, proc)
    time_cost = @elapsed fetched = map(x->_move(ctx, proc, x), data)
    @dbg timespan_end(ctx, :comm, thunk_id, proc)

    @dbg timespan_start(ctx, :compute, thunk_id, proc)
    result_meta = try
        res = f(fetched...)
        (proc, thunk_id, send_result ? res : tochunk(res, persist=persist, cache=persist ? true : cache)) #todo: add more metadata
    catch ex
        bt = catch_backtrace()
        (proc, thunk_id, RemoteException(myid(), CapturedException(ex, bt)))
    end
    @dbg timespan_end(ctx, :compute, thunk_id, proc)
    result_meta
end

@noinline function async_apply(ctx, p::OSProc, thunk_id, f, data, chan, send_res, persist, cache)
    @async begin
        try
            put!(chan, remotecall_fetch(do_task, p.pid, ctx, p, thunk_id, f, data, send_res, persist, cache))
        catch ex
            bt = catch_backtrace()
            put!(chan, (p, thunk_id, CapturedException(ex, bt)))
        end
        nothing
    end
end

end # module Sch
