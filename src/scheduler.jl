import Base.get
using Base.Test

let counter=0
    global next_id
    next_id() = "_$(counter+=1)" |> symbol
end

##### Macro sugar #####
function async_call(expr::Expr, id=string(expr))
    if expr.head == :call
        f = expr.args[1]
        args = map(async_call, expr.args[2:end])
        :(Thunk($id, $f, ($(args...))))
    elseif expr.head == :(=)
        x = expr.args[1]
        cll = async_call(expr.args[2], string(x))
        :($x = $cll)
    end
end

function async_call(expr, id=string(expr))
    expr
end

macro par(expr)
    if expr.head in [:(=), :call]
        async_call(expr) |> esc
    elseif expr.head == :block
        Expr(:block, map(async_call, expr.args)...) |> esc
    else
        error("@par only works for function call and assignments")
    end
end

let @par t = 1+2
    @test t == Thunk(:t, +, 1,2)
    @par begin
        a = 3
        b = a+3
    end
    @test b == Thunk(:b, +, 3, 3)
end

function Base.show(io::IO, p::Thunk)
    write(io, "*")
    write(io, p.id)
    write(io, "*")
end

inputs(x::Thunk) = x.inputs
inputs(x) = ()

istask(x::Thunk) = true
istask(x) = false


##### Scheduling logic #####

" find the set of direct dependents for each task "
function dependents(node, deps=Dict())
    if !haskey(deps, node)
        deps[node] = Set()
    end
    for inp = inputs(node)
        deps[inp] = push!(get(deps, inp, Set()), node)
        dependents(inp, deps)
    end
    deps
end

"""
recursively find the number of taks depending on each task in the DAG
takes the root (final result) node of the dag.
"""
function ndependents(dpents::Dict)
    Pair[node => ndependents(node, dpents) for node in keys(dpents)] |> Dict
end

function ndependents(t::Thunk)
    ndependents(dependents(t))
end

function ndependents(n, dpents)
    dependents = get(dpents, n, Set())
    length(dependents) + reduce(+, 0, [ndependents(c, dpents)
        for c in dependents])
end

"""
Given a root node of the DAG, calculates a total order for tie-braking

  * Root node gets score 1,
  * rest of the nodes are explored in DFS fashion but children
    of each node are explored in order of `ndependents`,
    i.e. total number of tasks depending on the result of the said node.

"""
function order(node::Thunk)
    order(node, ndependents(node))[2]
end
function order(node, ndeps, c=0, output=Dict())

    c+=1
    output[node] = c
    next = sort([i for i in inputs(node)], by=k->-ndeps[k])
    for n in next
        c, output = order(n, ndeps, c, output)
    end
    c, output
end


#### test set 2 begin

function inc(x)
    x+1
end

let _= nothing
    @par begin
        a = 1
        b = inc(a)
        c = inc(b)
    end

    deps = dependents(c)
    @test deps == Dict(a => Set([b]), b => Set([c]), c=>Set())
    @test ndependents(deps) == Dict(a=>2, b=>1, c=>0)
    @test order(c) == Dict(a => 3, b => 2, c => 1)
end

let _=nothing

    @par begin
        a = 1
        b = 2
        c = inc(a)
        d = b+c
    end
    @test ndependents(d) == Dict(a => 2, b => 1, c => 1, d => 0)
    @test order(d) == Dict(d=>1, c=>3, b=>2, a=>4)
end

##### test set 2 ends

function start_state(d::Thunk, node_order)
    state = Dict()
    deps = dependents(d)
    state[:dependents] = deps
    state[:finished] = Set()
    state[:waiting] = Dict() # who is x waiting for?
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

function do_task(proc, node, f, data, chan)
    res = f(data...)
    ref = RemoteChannel()
    put!(ref, res)
    put!(chan, (proc, node, res)) #todo: add more metadata
end

function async_apply(p::OSProc, node, f, data, chan)
    remotecall(p.pid, do_task, p, node, f, data, chan)
end

function fire_task!(proc, state, chan)
    node = pop!(state[:ready])
    push!(state[:running], node)

    data = Any[state[:cache][n] for n in node.inputs]
    async_apply(proc, node, node.f, data, chan)
end

get(ctx::Context, x) = x
function get(ctx::Context, d::Thunk)
    ps = procs(ctx)
    chan = RemoteChannel()
    ord = order(d)

    node_order = x -> -ord[x]
    state = start_state(d, node_order)
    # start off some tasks
    for p in ps
        isempty(state[:ready]) && break
        fire_task!(p, state, chan)
    end

    while !isempty(state[:waiting]) || !isempty(state[:ready]) || !isempty(state[:running])
        proc, node, res = take!(chan)
        state[:cache][node] = res
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
        state[:finished] = node
        pop!(state[:running], node)

        while !isempty(state[:ready]) && length(state[:running]) < length(ps)
            fire_task!(proc, state, chan)
        end
    end

    state[:cache][d]
end

let _=nothing
    @par begin
        a = 1
        b = 2
        c = inc(a)
        d = b+c
    end
    @test ndependents(d) == Dict(a => 2, b => 1, c => 1, d => 0)
    @test order(d) == Dict(d=>1, c=>3, b=>2, a=>4)

    @test get(Context(), d) == 4
end
