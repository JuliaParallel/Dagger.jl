export stage, cached_stage, compute, debug_compute, free!, cleanup

###### Scheduler #######

compute(x; options=nothing) = compute(Context(), x; options=options)
compute(ctx, c::Chunk; options=nothing) = c

collect(ctx::Context, t::Thunk; options=nothing) =
    collect(ctx, compute(ctx, t; options=options); options=options)
collect(d::Union{Chunk,Thunk}; options=nothing) =
    collect(Context(), d; options=options)

abstract type Computation end

compute(ctx, c::Computation; options=nothing) =
    compute(ctx, stage(ctx, c); options=options)
collect(c::Computation; options=nothing) =
    collect(Context(), c; options=options)

"""
    compute(ctx::Context, d::Thunk; options=nothing) -> Chunk

Compute a Thunk - creates the DAG, assigns ranks to nodes for tie breaking and
runs the scheduler with the specified options. Returns a Chunk which references
the result.
"""
function compute(ctx::Context, d::Thunk; options=nothing)
    scheduler = get!(PLUGINS, :scheduler) do
        get_type(PLUGIN_CONFIGS[:scheduler])
    end
    res = scheduler.compute_dag(ctx, d; options=options)
    if ctx.log_file !== nothing
        if ctx.log_sink !== LocalEventLog
            logs = get_logs!(ctx.log_sink)
            open(ctx.log_file, "w") do io
                Dagger.show_plan(io, logs, d)
            end
        else
            @warn "Context log_sink not set to LocalEventLog, skipping"
        end
    end
    res
end

function debug_compute(ctx::Context, args...; profile=false, options=nothing)
    @time res = compute(ctx, args...; options=options)
    get_logs!(ctx.log_sink), res
end

function debug_compute(arg; profile=false, options=nothing)
    ctx = Context()
    dbgctx = Context(procs(ctx), LocalEventLog(), profile)
    debug_compute(dbgctx, arg; options=options)
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

"""
    dependents(node::Thunk, deps=Dict{Thunk, Set{Thunk}}()) -> Dict{Thunk, Set{Thunk}}

Find the set of direct dependents for each task.
"""
function dependents(node::Thunk, deps=Dict{Thunk, Set{Thunk}}())
    if !haskey(deps, node)
        deps[node] = Set{Thunk}()
    end
    for inp = inputs(node)
        if isa(inp, Thunk)
            s::Set{Thunk} = get!(()->Set{Thunk}(), deps, inp)
            push!(s, node)
            dependents(inp, deps)
        end
    end
    deps
end

"""
    noffspring(dpents::Dict{Thunk, Set{Thunk}}) -> Dict{Thunk, Int}

Recursively find the number of tasks dependent on each task in the DAG.
Takes a Dict as returned by [`dependents`](@ref).
"""
function noffspring(dpents::Dict{Thunk, Set{Thunk}})
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
    order(node::Thunk, ndeps) -> Dict{Thunk,Int}

Given a root node of the DAG, calculates a total order for tie-breaking.

  * Root node gets score 1,
  * rest of the nodes are explored in DFS fashion but chunks
    of each node are explored in order of `noffspring`,
    i.e. total number of tasks depending on the result of the said node.

Args:
- node: root node
- ndeps: result of [`noffspring`](@ref)
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
