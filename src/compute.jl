export stage, cached_stage, compute, debug_compute, free!, cleanup

###### Scheduler #######

compute(x; kwargs...) = compute(Context(), x; kwargs...)
compute(ctx, c::Chunk; kwags...) = c

collect(ctx::Context, c; kwargs...) = collect(ctx, compute(ctx, c); kwargs...)
collect(d::Union{Chunk,Thunk}; kwargs...) = collect(Context(), d; kwargs...)

abstract type Computation end

compute(ctx, c::Computation; kwargs...) = compute(ctx, stage(ctx, c); kwargs...)
collect(c::Computation; kwargs...) = collect(Context(), c; kwargs...)

"""
Compute a Thunk - creates the DAG, assigns ranks to nodes for tie breaking and
runs the scheduler with the specified options.
"""
function compute(ctx, d::Thunk; kwargs...)
    if !(:scheduler in keys(PLUGINS))
        PLUGINS[:scheduler] = get_type(PLUGIN_CONFIGS[:scheduler])
    end
    scheduler = PLUGINS[:scheduler]
    (scheduler).compute_dag(ctx, d; kwargs...)
end

function debug_compute(ctx::Context, args...; profile=false, kwargs...)
    @time res = compute(ctx, args...; kwargs...)
    get_logs!(ctx.log_sink), res
end

function debug_compute(arg; profile=false, kwargs...)
    ctx = Context()
    dbgctx = Context(procs(ctx), LocalEventLog(), profile)
    debug_compute(dbgctx, arg; kwargs...)
end

Base.@deprecate gather(ctx, x) collect(ctx, x)
Base.@deprecate gather(x) collect(x)

cleanup() = cleanup(Context())
function cleanup(ctx::Context)
    if :scheduler in keys(PLUGINS)
        scheduler = PLUGINS[:scheduler]
        (scheduler).cleanup(ctx)
        delete!(PLUGINS, :scheduler)
    end
    nothing
end

function get_type(s::String)
    local T
    for t in split(s, ".")
        t = Symbol(t)
        if !@isdefined(T)
            T = Base.require(@__MODULE__, t)
        else
            T = Core.eval(T, t)
        end
    end
    T
end

##### Dag utilities #####

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
        reduce(+, (noffspring(d, dpents) for d in ds), init = length(ds))
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
