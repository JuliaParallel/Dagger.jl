export stage, cached_stage, compute, debug_compute, free!, cleanup

###### Scheduler #######

compute(x; options=nothing) = compute(Context(global_context()), x; options=options)
compute(ctx, c::Chunk; options=nothing) = c

collect(ctx::Context, t::Thunk; options=nothing) =
    collect(ctx, compute(ctx, t; options=options); options=options)
collect(d::Union{Chunk,Thunk}; options=nothing) =
    collect(Context(global_context()), d; options=options)

abstract type Computation end

compute(ctx, c::Computation; options=nothing) =
    compute(ctx, stage(ctx, c); options=options)
collect(c::Computation; options=nothing) =
    collect(Context(global_context()), c; options=options)

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
    ctx = Context(global_context())
    dbgctx = Context(procs(ctx), LocalEventLog(), profile)
    debug_compute(dbgctx, arg; options=options)
end

Base.@deprecate gather(ctx, x) collect(ctx, x)
Base.@deprecate gather(x) collect(x)

cleanup() = cleanup(Context(global_context()))
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
    dependents(node::Thunk) -> Dict{Union{Thunk,Chunk}, Set{Thunk}}

Find the set of direct dependents for each task.
"""
function dependents(node::Thunk)
    deps = Dict{Union{Thunk,Chunk}, Set{Thunk}}()
    visited = Set{Thunk}()
    to_visit = Set{Thunk}()
    push!(to_visit, node)
    while !isempty(to_visit)
        next = pop!(to_visit)
        (next in visited) && continue
        if !haskey(deps, next)
            deps[next] = Set{Thunk}()
        end
        for inp in inputs(next)
            if istask(inp) || (inp isa Chunk)
                s = get!(()->Set{Thunk}(), deps, inp)
                push!(s, next)
                if istask(inp) && !(inp in visited)
                    push!(to_visit, inp)
                end
            end
        end
        push!(visited, next)
    end
    deps
end

"""
    noffspring(dpents::Dict{Union{Thunk,Chunk}, Set{Thunk}}) -> Dict{Thunk, Int}

Recursively find the number of tasks dependent on each task in the DAG.
Takes a Dict as returned by [`dependents`](@ref).
"""
function noffspring(dpents::Dict{Union{Thunk,Chunk}, Set{Thunk}})
    noff = Dict{Thunk,Int}()
    to_visit = collect(filter(istask, keys(dpents)))
    while !isempty(to_visit)
        next = popfirst!(to_visit)
        haskey(noff, next) && continue
        off = 0
        has_all = true
        for dep in dpents[next]
            if haskey(noff, dep)
                off += noff[dep] + 1
            else
                pushfirst!(to_visit, next)
                pushfirst!(to_visit, dep)
                has_all = false
                break
            end
        end
        has_all || continue
        noff[next] = off
    end
    noff
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
    output = Dict{Thunk,Int}()
    to_visit = Thunk[]
    push!(to_visit, node)
    s = 0
    while !isempty(to_visit)
        next = popfirst!(to_visit)
        haskey(output, next) && continue
        s += 1
        output[next] = s
        parents = filter(istask, inputs(next))
        if !isempty(parents)
            # If parents is empty, sort! should be a no-op, but raises an ambiguity error
            # when InlineStrings.jl is loaded (at least, version 1.1.0), because InlineStrings
            # defines a method defalg(::AbstractArray{<:Union{Missing, String1, String15, String3, String7}})
            # while Base defines a method defalg(v::AbstractArray{<:Union{Missing, Number}}). When parents is
            # empty it is a Vector{Union{}}. Since Union{} is a subtype of all Union{...}s, any package
            # that defines a function defalg(::AbstractArray{<:Union{...anything can go here...}}) will cause
            # ambiguity. By not calling sort! when parents is empty, we avoid calling sort! with a Vector{Union{}}
            # and always call with a more specific type that is not a subtype of other packages' types.
            sort!(parents, by=k->get(ndeps,k,0))
        end
        append!(to_visit, parents)
    end
    output
end
