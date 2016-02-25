import Base.get

##### Scheduling logic #####

"find the set of direct dependents for each task"
function dependents(node::Thunk, deps=Dict())
    if !haskey(deps, node)
        deps[node] = Set()
    end
    for inp = inputs(node)
        deps[inp] = push!(get(deps, inp, Set()), node)
        if isa(inp, Thunk)
            dependents(inp, deps)
        end
    end
    deps
end

function dependents(nodes::AbstractArray, deps=Dict())
    for n in nodes
        dependents(n, deps)
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
  * rest of the nodes are explored in DFS fashion but children
    of each node are explored in order of `noffspring`,
    i.e. total number of tasks depending on the result of the said node.

Args:
    - ndeps: result of `noffspring`
    - node: root node
"""
function order(nodes, ndeps)
    order(nodes, ndeps, 0)[2]
end

function order(nodes::AbstractArray, ndeps, c, output=Dict())

    for node in nodes
        c+=1
        output[node] = c
        nxt = sort([n for n in inputs(node)], by=k->ndeps[k])
        c, output = order(nxt, ndeps, c, output)
    end
    c, output
end

function start_state(d::AbstractArray, node_order)
    state = Dict()
    deps = dependents(d)
    state[:dependents] = deps
    state[:finished] = Set()
    state[:waiting] = Dict() # who is x waiting for?
    state[:waiting_data] = Dict() # dependents still waiting
    state[:ready] = Any[]
    state[:cache] = ObjectIdDict()
    state[:running] = Set()

    nodes = sort(collect(keys(deps)), by=node_order)
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

function _move(ctx, x::AbstractPart)
    gather(ctx, x)
end
_move(ctx, x) = x

function do_task(ctx, proc, thunk_id, f, data, chan)
    try
        res = f(map(x->_move(ctx, x), data)...)
        part(res)
        put!(chan, (proc, thunk_id, part(res))) #todo: add more metadata
    catch e
        put!(chan, (proc, thunk_id, e))
    end
end

function async_apply(ctx, p::OSProc, thunk_id, f, data, chan)
    remotecall(do_task, p.pid, ctx, p, thunk_id, f, data, chan)
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
    async_apply(ctx, proc, thunk.id, thunk.f, data, chan)
end

compute(ctx, x::AbstractPart) = x
function compute(ctx, x::Cat)
    thunk = thunkize(ctx, x)
    if isa(thunk, Thunk)
        compute(ctx, thunk)
    else
        x
    end
end
function compute(ctx, d::Thunk)
    ps = procs(ctx)
    chan = RemoteChannel()
    deps = dependents([d])
    ndeps = noffspring(deps)
    ord = order([d], ndeps)

    sort_ord = [(k,v) for (k,v) in ord]
    sortord = x -> istask(x[1]) ? x[1].id : 0
    sort_ord = sort(sort_ord, by=sortord)

    node_order = x -> -ord[x]
    state = start_state([d], node_order)
    # start off some tasks
    for p in ps
        isempty(state[:ready]) && break
        fire_task!(ctx, p, state, chan)
    end

    while !isempty(state[:waiting]) || !isempty(state[:ready]) || !isempty(state[:running])
        proc, thunk_id, res = take!(chan)

        node = _thunk_dict[thunk_id]
        @logmsg("W$(proc.pid) - $node ($(node.f)) input:$(node.inputs)")
        state[:cache][node] = res
        #@show state[:cache]
        # if any of this guy's dependents are waiting,
        # update them
        #@show ord
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
        state[:finished] = node
        pop!(state[:running], node)

        while !isempty(state[:ready]) && length(state[:running]) < length(ps)
            fire_task!(ctx, proc, state, chan)
        end
    end

    state[:cache][d]
end

"""
If a Cat tree has a Thunk in it, make the whole thing a big thunk
"""
function thunkize(ctx, c::Cat)
    if any(istask, c.children)
        thunks = map(x -> thunkize(ctx, x), c.children)
        Thunk(thunks; meta=true) do results...
            t = promote_type(map(parttype, results)...)
            Cat(partition(c), t, domain(c), AbstractPart[results...])
        end
    else
        c
    end
end
thunkize(ctx, x::AbstractPart) = x
thunkize(ctx, x::Thunk) = x
