export stage, cached_stage, compute

"""
An `Computation` represents a computation to be
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

"""
A memoized version of stage. It is important that the
tasks generated for the same Computation have the same
identity, for example:

    A = rand(BlockPartition(100,100), Float64, 1000, 1000)
    compute(A+A')

will should not return in computation of A twice because
we are staging A multiple times.
"""

global _stage_cache = Dict()
function cached_stage(ctx, x)
    if haskey(_stage_cache, x)
        _stage_cache[(ctx, x)]
    else
        _stage_cache[(ctx, x)] = stage(ctx, x)
    end
end

"""
Calling `compute` on an `Computation` will make an
`AbstractPart` by computing it.

You can call `gather` on the result to get the result
into the calling process (e.g. a REPL)
"""
compute(ctx, x::Computation) = compute(ctx, stage(ctx, x))
compute(x) = compute(Context(), x)
gather(ctx, x) = gather(ctx, compute(ctx, x))
gather(x) = gather(Context(), x)


immutable TupleCompute <: Computation
    comps::Tuple
end

function stage(ctx, tc::TupleCompute)
    t = map(c -> cached_stage(ctx, c), tc.comps)
    Thunk(tuple, t)
end
gather(ctx, x::Tuple) = gather(ctx, compute(ctx, TupleCompute(x)))

"""
promote a computed value to a Computation
"""
immutable Computed <: Computation
    result::AbstractPart
    # TODO: Allow passive branching for Save?
end

function stage(ctx, c::Computed)
    c.result
end

"""
`PartSpec` and `Sub` objects are always in computed state,
this method just returns them.
"""
compute(ctx, x::Union{PartSpec, Sub}) = x

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
    if any(istask, c.parts)
        thunks = map(x -> thunkize(ctx, x), c.parts)
        Thunk(thunks; meta=true) do results...
            t = parttype(results[1])
            Cat(partition(c), t, domain(c), AbstractPart[results...])
        end
    else
        c
    end
end
thunkize(ctx, x::AbstractPart) = x
thunkize(ctx, x::Thunk) = x


###### Scheduler #######
"""
Compute a Thunk - creates the DAG, assigns ranks to
nodes for tie breaking and runs the scheduler.
"""
function compute(ctx, d::Thunk)
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
        fire_task!(ctx, p, state, chan)
    end

    while !isempty(state[:waiting]) || !isempty(state[:ready]) || !isempty(state[:running])
        proc, thunk_id, res = take!(chan)

        if isa(res, Exception)
            rethrow(res)
        end
        node = _thunk_dict[thunk_id]
        @logmsg("W$(proc.pid) - $node ($(node.f)) input:$(node.inputs)")
        state[:cache][node] = res
        #@show state[:cache]
        #@show ord
        # if any of this guy's dependents are waiting,
        # update them
        deps = sort([i for i in state[:dependents][node]], by=node_order)
        for dep in deps
            set = state[:waiting][dep]
            pop!(set, node)
            if isempty(set)
                pop!(state[:waiting], dep)
                push!(state[:ready], dep)
            end
            # todo: release data
        end
        for inp in inputs(node)
            if inp in keys(state[:waiting_data])
                s = state[:waiting_data][inp]
                #@show s
                if node in s
                    pop!(s, node)
                end
                if isempty(s)
                    @logmsg("releasing $inp")
                    release!(state[:cache], inp)
                end
            end
        end
        state[:finished] = node
        pop!(state[:running], node)

        while !isempty(state[:ready]) && length(state[:running]) < length(ps)
            fire_task!(ctx, proc, state, chan)
        end
    end

    state[:cache][d]
end

function release!(cache, node)
    if haskey(cache, node)
        if isa(cache[node], PartSpec{DistMem})
            @logmsg("Finalizing remoteref")
            finalize(cache[node].handle.ref)
        end
        pop!(cache, node)
    end
end

function fire_task!(ctx, proc, state, chan)
    thunk = pop!(state[:ready])
    @logmsg("W$(proc.pid) + $thunk ($(thunk.f)) input:$(thunk.inputs)")
    if thunk.administrative
        # Run it on the parent node
        # do not _move data.
        state[:cache][thunk] = thunk.f(map(n -> state[:cache][n], thunk.inputs)...)
        return
    end
    push!(state[:running], thunk)

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
        s = if !haskey(deps, inp)
            deps[inp] = Set()
        else deps[inp] end :: Set{Any}

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
    ndeps = Dict()
    num_needed = Dict(Pair[k => length(v) for (k, v) in dpents])
    current = Set()
    for (k, v) in num_needed
        # start with nodes that have no dependents (root)
        v == 0 && push!(current, k)
    end

    while !isempty(current)
        key = pop!(current)
        ndeps[key] = 1 + sum(Int[ndeps[dep] for dep in dpents[key]])
        for c in inputs(key)
            num_needed[c] -= 1
            if num_needed[c] == 0
                push!(current, c)
            end
        end
    end
    ndeps
end

"""
Given a root node of the DAG, calculates a total order for tie-braking

  * Root node gets score 1,
  * rest of the nodes are explored in DFS fashion but children
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

_move(ctx, x) = x
_move(ctx, x::AbstractPart) = gather(ctx, x)

function do_task(ctx, proc, thunk_id, f, data, chan, send_result)
    try
        res = f(map(x->_move(ctx, x), data)...)
        put!(chan, (proc, thunk_id, send_result ? res : part(res))) #todo: add more metadata
    catch ex
        put!(chan, (proc, thunk_id, ex))
    end
    nothing
end
function async_apply(ctx, p::OSProc, thunk_id, f, data, chan, send_res)
    remotecall(do_task, p.pid, ctx, p, thunk_id, f, data, chan, send_res)
end
