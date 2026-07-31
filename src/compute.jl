export compute, debug_compute

###### Scheduler #######

compute(x; options::Union{SchedulerOptions,Nothing}=nothing) =
    compute(global_context(), x; options)
compute(ctx, c::Chunk; options::Union{SchedulerOptions,Nothing}=nothing) = c

collect(ctx::Context, t::Thunk; options::Union{SchedulerOptions,Nothing}=nothing) =
    collect(ctx, compute(ctx, t; options); options)
collect(d::Union{Chunk,Thunk}; options::Union{SchedulerOptions,Nothing}=nothing) =
    collect(global_context(), d; options)

abstract type Computation end

"""
    compute(ctx::Context, d::Thunk; options::Union{SchedulerOptions,Nothing}=nothing) -> Chunk

Compute a Thunk - creates the DAG, assigns ranks to nodes for tie breaking and
runs the scheduler with the specified options. Returns a Chunk which references
the result.
"""
function compute(ctx::Context, d::Thunk; options::Union{SchedulerOptions,Nothing}=nothing)
    if options === nothing
        options = SchedulerOptions()
    end
    return Sch.compute_dag(ctx, d, options)
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
    deps = Dict{Thunk, Set{Thunk}}()
    visited = Set{Thunk}()
    to_visit = Set{Thunk}()
    push!(to_visit, node)
    while !isempty(to_visit)
        next = pop!(to_visit)
        (next in visited) && continue
        if !haskey(deps, next)
            deps[next] = Set{Thunk}()
        end
        for inp in syncdeps_iterator(next)
            s = get!(()->Set{Thunk}(), deps, inp)
            push!(s, next)
            if !(inp in visited)
                push!(to_visit, inp)
            end
        end
        push!(visited, next)
    end
    return deps
end
syncdeps_iterator(thunk::Thunk) =
    Iterators.map(syncdep->unwrap_weak_checked(something(syncdep.thunk))::Thunk,
                  thunk.options.syncdeps)

"""
    noffspring(dpents::Dict{Thunk, Set{Thunk}}) -> Dict{Thunk, Int}

Recursively find the number of tasks dependent on each task in the DAG.
Takes a Dict as returned by [`dependents`](@ref).
"""
function noffspring(dpents::Dict{Thunk, Set{Thunk}})
    noff = Dict{Thunk,Int}()
    to_visit = collect(keys(dpents))
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
    return noff
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
        parents = collect(syncdeps_iterator(next))
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
