export stage, cached_stage, compute, debug_compute, cached, free!
using Compat

compute(x) = compute(Context(), x)
compute(ctx, c::Chunk) = c

collect(ctx::Context, c) = collect(ctx, compute(ctx, c))
collect(d::Union{Chunk,Thunk}) = collect(Context(), d)

@compat abstract type Computation end

compute(ctx, c::Computation) = compute(ctx, stage(ctx, c))
collect(c::Computation) = collect(Context(), c)


function finish_task!(state, node, node_order; free=true)
    if istask(node) && node.cache
        node.cache_ref = Nullable{Any}(state.cache[node])
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

###### Scheduler #######
"""
Compute a Thunk - creates the DAG, assigns ranks to
nodes for tie breaking and runs the scheduler.
"""
function compute(ctx, d::Thunk)
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

    while !isempty(state.ready) ||
        !isempty(state.running)

        if isempty(state.running) && !isempty(state.ready)
            for p in ps
                isempty(state.ready) && break
                task = pop_with_affinity!(ctx, state.ready, p, false)
                if task !== nothing
                    fire_task!(ctx, task, p, state, chan, node_order)
                end
            end
        end
        proc, thunk_id, res = take!(chan)
        if isa(res, CapturedException) || isa(res, RemoteException)
            rethrow(res)
        end
        node = _thunk_dict[thunk_id]
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
    if immediate_next
        # fast path
        if proc in first.(affinity(tasks[end]))
            return pop!(tasks)
        end
    end
    parent_affinities = affinity.(tasks)
    for i=length(tasks):-1:1
        # TODO: use the size
        if proc in first.(parent_affinities[i])
            t = tasks[i]
            deleteat!(tasks, i)
            return t
        end
    end
    for i=length(tasks):-1:1
        # use up tasks without affinitites
        # let the procs with the respective affinities pick up
        # other tasks
        if isempty(parent_affinities[i])
            t = tasks[i]
            deleteat!(tasks, i)
            return t
        end
        aff = first.(parent_affinities[i])
        if all(!(p in aff) for p in procs(ctx)) # no proc is ever going to ask for it
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
    if thunk.cache && !isnull(thunk.cache_ref)
        # the result might be already cached
        data = unrelease(get(thunk.cache_ref)) # ask worker to keep the data around
                                          # till this compute cycle frees it
        if !isnull(data)
            @logmsg("cache hit: $(get(thunk.cache_ref))")
            state.cache[thunk] = get(data)
            immediate_next = finish_task!(state, thunk, node_order; free=false)
            if !isempty(state.ready)
                thunk = pop_with_affinity!(ctx, state.ready, proc, immediate_next)
                if thunk !== nothing
                    fire_task!(ctx, thunk, proc, state, chan, node_order)
                end
            end
            return
        else
            thunk.cache_ref = Nullable{Any}()
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
    async_apply(ctx, proc, thunk.id, thunk.f, data, chan, thunk.get_result, thunk.persist)
end


##### Scheduling logic #####

"find the set of direct dependents for each task"
function dependents(node::Thunk, deps=Dict{Thunk, Set{Thunk}}())
    if !haskey(deps, node)
        deps[node] = Set{Thunk}()
    end
    for inp = inputs(node)
        if isa(inp, Thunk)
            s::Set{Thunk} = Base.@get!(deps, inp, Set{Thunk}())
            push!(s, node)
            dependents(inp, deps)
        end
    end
    deps
end

"""
recursively find the number of taks dependent on each task in the DAG.
Input: dependents dict
"""
function noffspring(dpents::Dict)
    Dict(node => noffspring(node, dpents) for node in keys(dpents))
end

function noffspring(n, dpents)
    if haskey(dpents, n)
        ds = dpents[n]
        reduce(+, length(ds), noffspring(d, dpents) for d in ds)
    else
        0
    end
end

"""
Given a root node of the DAG, calculates a total order for tie-braking

  * Root node gets score 1,
  * rest of the nodes are explored in DFS fashion but chunks
    of each node are explored in order of `noffspring`,
    i.e. total number of tasks depending on the result of the said node.

Args:
    - node: root node
    - ndeps: result of `noffspring`
"""
function order(node::Thunk, ndeps)
    function recur(nodes, s)
        for n in nodes
            output[n] = s += 1
            parents = collect(Iterators.filter(istask, inputs(n)))
            s = recur(sort!(parents, by=k->get(ndeps,k,0)), s)
        end
        return s
    end
    output = Dict{Thunk,Int}()
    recur([node], 0)
    return output
end

const OneToMany = Dict{Thunk, Set{Thunk}}
struct ComputeState
    dependents::OneToMany
    finished::Set{Thunk}
    waiting::OneToMany
    waiting_data::OneToMany
    ready::Vector{Thunk}
    cache::Dict{Thunk, Any}
    running::Set{Thunk}
end

function start_state(deps::Dict, node_order)
    state = ComputeState(
                  deps,
                  Set{Thunk}(),
                  OneToMany(),
                  OneToMany(),
                  Vector{Thunk}(0),
                  Dict{Thunk, Any}(),
                  Set{Thunk}()
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

function do_task(ctx, proc, thunk_id, f, data, send_result, persist)
    @dbg timespan_start(ctx, :comm, thunk_id, proc)
    time_cost = @elapsed fetched = map(x->_move(ctx, proc, x), data)
    @dbg timespan_end(ctx, :comm, thunk_id, proc)

    @dbg timespan_start(ctx, :compute, thunk_id, proc)
    result_meta = try
        res = f(fetched...)
        (proc, thunk_id, send_result ? res : tochunk(res, persist=persist)) #todo: add more metadata
    catch ex
        bt = catch_backtrace()
        (proc, thunk_id, RemoteException(myid(), CapturedException(ex, bt)))
    end
    @dbg timespan_end(ctx, :compute, thunk_id, proc)
    result_meta
end

function async_apply(ctx, p::OSProc, thunk_id, f, data, chan, send_res, persist)
    @schedule begin
        try
            put!(chan, Base.remotecall_fetch(do_task, p.pid, ctx, p, thunk_id, f, data, send_res, persist))
        catch ex
            bt = catch_backtrace()
            put!(chan, (p, thunk_id, CapturedException(ex, bt)))
        end
        nothing
    end
end

function debug_compute(ctx::Context, args...; profile=false)
    @time res = compute(ctx, args...)
    get_logs!(ctx.log_sink), res
end

function debug_compute(arg; profile=false)
    ctx = Context()
    dbgctx = Context(procs(ctx), LocalEventLog(), profile)
    debug_compute(dbgctx, arg)
end

Base.@deprecate gather(ctx, x) collect(ctx, x)
Base.@deprecate gather(x) collect(x)
