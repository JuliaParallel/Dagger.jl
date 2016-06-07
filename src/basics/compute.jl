export stage, cached_stage, compute, debug_compute, cached, free!
using Compat

"""
A `Computation` represents a computation to be
performed on some distributed data
"""
abstract Computation

"""
`stage` on a computation creates a set of Thunk objects
each denoting a smaller work item required to realize a
computation. The set of thunks are put in a `Cat` to annotate
metadata about the result such as its type, domain and
partition scheme.
"""
@unimplemented stage(ctx, c::Computation)
function stage(ctx, node::Cat)
    node
end

global _stage_cache = WeakKeyDict{Context, Dict}()
"""
A memoized version of stage. It is important that the
tasks generated for the same Computation have the same
identity, for example:

    A = rand(BlockPartition(100,100), Float64, 1000, 1000)
    compute(A+A')

must not result in computation of A twice.
"""
function cached_stage(ctx, x)
    cache = if !haskey(_stage_cache, ctx)
        _stage_cache[ctx] = Dict()
    else
        _stage_cache[ctx]
    end

    if haskey(cache, x)
        cache[x]
    else
        cache[x] = stage(ctx, x)
    end
end

"""
Calling `compute` on an `Computation` will make an
`AbstractPart` by computing it.

You can call `gather` on the result to get the result
into the calling process (e.g. a REPL)
"""
compute(ctx, x::Computation) = wrap_computed(compute(ctx, cached_stage(ctx, x)))
compute(x) = compute(Context(), x)
gather(ctx, x) = gather(ctx, compute(ctx, x))
gather(x) = gather(Context(), x)

function wrap_computed(x::AbstractPart)
    persist!(x)
    Computed(x)
end
wrap_computed(x) = x

immutable TupleCompute <: Computation
    comps::Tuple
end

function stage(ctx, tc::TupleCompute)
    t = map(c -> thunkize(ctx, cached_stage(ctx, c)), tc.comps)
    Thunk(tuple, t)
end
compute(ctx, x::Tuple) = compute(ctx, TupleCompute(x))

export Computed
"""
promote a computed value to a Computation
"""
type Computed <: Computation
    result::AbstractPart
    # TODO: Allow passive branching for Save?
    function Computed(x)
        c = new(x)
        finalizer(c, finalize_computed!)
        c
    end
end

"""
Tell the CF not to remove the result of a computation
once it's done.
"""
immutable Cached <: Computation
    inp::Computation
end

cached(x::Computation) = Cached(x)
function stage(ctx, x::Cached)
    persist!(cached_stage(ctx, x))
    x
end

free!(x::Computed, force=true) = free!(x.result,force)
function finalize_computed!(x::Computed)
    @schedule free!(x, true) # @schedule needed because gc can't yield
end

gather(ctx, x::Computed) = gather(ctx, x.result)
function stage(ctx, c::Computed)
    c.result
end

function free!(x::Cat)
    for p in parts(x)
        free!(p)
    end
    nothing
end

"""
`Part` and `Sub` objects are always in computed state,
this method just returns them.
"""
compute(ctx, x::Union{Part, Sub}) = x

"""
A Cat object may contain a thunk in it, in which case
we first turn it into a Thunk object and then compute it.
"""
function compute(ctx, x::Cat)
    thunk = thunkize(ctx, x)
    if isa(thunk, Thunk)
        compute(ctx, thunk)
    else
        x
    end
end

"""
If a Cat tree has a Thunk in it, make the whole thing a big thunk
"""
function thunkize(ctx, c::Cat)
    if any(istask, parts(c))
        thunks = map(x -> thunkize(ctx, x), parts(c))
        sz = size(parts(c))
        Thunk(thunks; meta=true) do results...
            t = parttype(results[1])
            Cat(t, domain(c), reshape(AbstractPart[results...], sz))
        end
    else
        c
    end
end
thunkize(ctx, x::AbstractPart) = x
thunkize(ctx, x::Thunk) = x
function finish_task!(state, node, node_order; free=true)
    deps = sort([i for i in state[:dependents][node]], by=node_order)
    for dep in deps
        set = state[:waiting][dep]
        pop!(set, node)
        if isempty(set)
            pop!(state[:waiting], dep)
            push!(state[:ready], dep)
        end
        # todo: free data
    end
    for inp in inputs(node)
        if inp in keys(state[:waiting_data])
            s = state[:waiting_data][inp]
            #@show s
            if node in s
                pop!(s, node)
            end
            if free && isempty(s)
                if haskey(state[:cache], inp)
                    _node = state[:cache][inp]
                    free!(_node, false)
                    pop!(state[:cache], inp)
                end
            end
        end
    end
    state[:finished] = node
    pop!(state[:running], node)
end

free!(x, force=true) = x # catch-all for non-parts

###### Scheduler #######
"""
Compute a Thunk - creates the DAG, assigns ranks to
nodes for tie breaking and runs the scheduler.
"""
function compute(ctx, d::Thunk)
    master = OSProc(myid())
    @dbg timespan_start(ctx, :scheduler_init, 0, master)
    ps = procs(ctx)
    chan = RemoteChannel()
    deps = dependents(d)
    ndeps = noffspring(deps)
    ord = order(d, ndeps)

    sort_ord = [(k,v) for (k,v) in ord]
    sortord = x -> istask(x[1]) ? x[1].id : 0
    sort_ord = sort(sort_ord, by=sortord)

    node_order = x -> -ord[x]
    state = start_state(deps, node_order)
    # start off some tasks
    for p in ps
        isempty(state[:ready]) && break
        fire_task!(ctx, p, state, chan, node_order)
    end
    @dbg timespan_end(ctx, :scheduler_init, 0, master)

    while !isempty(state[:waiting]) || !isempty(state[:ready]) || !isempty(state[:running])
        proc, thunk_id, res = take!(chan)

        if isa(res, CapturedException)
            rethrow(res)
        end
        node = _thunk_dict[thunk_id]
        @logmsg("W$(proc.pid) - $node ($(node.f)) input:$(node.inputs)")
        state[:cache][node] = res
        #@show state[:cache]
        #@show ord
        # if any of this guy's dependents are waiting,
        # update them
        @dbg timespan_start(ctx, :scheduler, thunk_id, master)

        finish_task!(state, node, node_order)

        while !isempty(state[:ready]) && length(state[:running]) < length(ps)
            fire_task!(ctx, proc, state, chan, node_order)
        end
        @dbg timespan_end(ctx, :scheduler, thunk_id, master)
    end

    state[:cache][d]
end

function fire_task!(ctx, proc, state, chan, node_order)
    thunk = pop!(state[:ready])
    @logmsg("W$(proc.pid) + $thunk ($(thunk.f)) input:$(thunk.inputs)")
    push!(state[:running], thunk)
    if thunk.administrative
        # Run it on the parent node
        # do not _move data.
        p = OSProc(myid())
        @dbg timespan_start(ctx, :comm, thunk.id, p)
        fetched = Any[state[:cache][i] for i in thunk.inputs]
        @dbg timespan_end(ctx, :comm, thunk.id, p)

        @dbg timespan_start(ctx, :compute, thunk.id, p)
        res = thunk.f(fetched...)
        @dbg timespan_end(ctx, :compute, thunk.id, p)

        #push!(state[:running], thunk)
        state[:cache][thunk] = res
        finish_task!(state, thunk, node_order; free=false)
        !isempty(state[:ready]) && fire_task!(ctx, proc, state, chan, node_order)
        return
    end

    data = Any[state[:cache][n] for n in thunk.inputs]

    async_apply(ctx, proc, thunk.id, thunk.f, data, chan, thunk.get_result)
end


##### Scheduling logic #####

"find the set of direct dependents for each task"
function dependents(node::Thunk, deps=Dict())
    if !haskey(deps, node)
        deps[node] = Set()
    end
    for inp = inputs(node)
        s::Set{Any} = Base.@get!(deps, inp, Set())
        push!(s, node)
        if isa(inp, Thunk)
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
    Pair[node => noffspring(node, dpents) for node in keys(dpents)] |> Dict
end

function noffspring(n, dpents)
    ds = get(dpents, n, Set()) # dependents of n
    length(dpents[n]) + reduce(+, 0, [noffspring(d, dpents)
        for d in ds])
end


"""
Given a root node of the DAG, calculates a total order for tie-braking

  * Root node gets score 1,
  * rest of the nodes are explored in DFS fashion but parts
    of each node are explored in order of `noffspring`,
    i.e. total number of tasks depending on the result of the said node.

Args:
    - ndeps: result of `noffspring`
    - node: root node
"""
function order(node::Thunk, ndeps)
    order([node], ndeps, 0)[2]
end

function order(nodes::AbstractArray, ndeps, c, output=Dict())

    for node in nodes
        c+=1
        output[node] = c
        nxt = sort(Any[n for n in inputs(node)], by=k->ndeps[k])
        c, output = order(nxt, ndeps, c, output)
    end
    c, output
end

function start_state(deps::Dict, node_order)
    state = Dict()
    state[:dependents] = deps
    state[:finished] = Set()
    state[:waiting] = Dict() # who is x waiting for?
    state[:waiting_data] = Dict() # dependents still waiting
    state[:ready] = Any[]
    state[:cache] = Dict()
    state[:running] = Set()

    nodes = sort(collect(keys(deps)), by=node_order)
    state[:waiting_data] = copy(deps)
    for k in nodes
        if istask(k)
            waiting = Set{Any}(filter(istask, inputs(k)))
            if isempty(waiting)
                push!(state[:ready], k)
            else
                state[:waiting][k] = waiting
            end
        else
            state[:cache][k] = k
        end
    end
    state
end

_move(ctx, to_proc, x) = x
_move(ctx, to_proc::OSProc, x::AbstractPart) = gather(ctx, x)

function do_task(ctx, proc, thunk_id, f, data, chan, send_result)
        @dbg timespan_start(ctx, :comm, thunk_id, proc)
        fetched = map(x->_move(ctx, proc, x), data)
        @dbg timespan_end(ctx, :comm, thunk_id, proc)

        @dbg timespan_start(ctx, :compute, thunk_id, proc)
        try
            res = f(fetched...)
            put!(chan, (proc, thunk_id, send_result ? res : part(res))) #todo: add more metadata
        catch ex
            bt = catch_backtrace()
            put!(chan, (proc, thunk_id, CapturedException(ex, bt)))
        end
        @dbg timespan_end(ctx, :compute, thunk_id, proc)
    nothing
end

function async_apply(ctx, p::OSProc, thunk_id, f, data, chan, send_res)
    Base.remote_do(do_task, p.pid, ctx, p, thunk_id, f, data, chan, send_res)
end

function debug_compute(ctx::Context, args...; profile=false)
    @time res = compute(ctx, args)
    get_logs!(ctx.log_sink), res
end

function debug_compute(arg; profile=false)
    ctx = Context()
    dbgctx = Context(procs(ctx), LocalEventLog(), profile)
    debug_compute(dbgctx, arg)
end
