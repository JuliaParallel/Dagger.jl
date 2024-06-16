module GraphVizSimpleExt

if isdefined(Base, :get_extension)
    using Colors
else
    using ..Colors
end

import Dagger
import Dagger: Chunk, Thunk, DTask, Processor
import Dagger: show_logs
import Dagger: istask, dependents
import Dagger: unwrap_weak
import Dagger.TimespanLogging: Timespan

### DAG-based graphing

global _part_labels = Dict()

function write_node(io, t::Chunk, c, ctx=nothing)
    _part_labels[t]="part_$c"
    c+1
end

function node_id(t::Thunk)
    t.id
end

function node_id(t)
    dec(hash(t))
end

function node_id(t::Chunk)
    _part_labels[t]
end

function node_name(t::Thunk)
    "n_$(t.id)"
end

function node_name(t::Chunk)
    _part_labels[t]
end

function node_name(name::String)
    "n_$name"
end

function node_name(id)
    "n_$id"
end

# Modified version of the function from Dagger compute.jl 
function custom_dependents(node::Thunk)
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
        for inp in next.syncdeps
            unwrapped = unwrap_weak(inp)
            if (unwrapped === nothing)
                continue
            end
            inp = unwrapped
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
    return deps
end

function write_dag(io, e::DTask)
    t = convert_to_thunk(e)
    write_dag(io, t)
end

function write_dag(io, t::Thunk)
    !istask(t) && return

    # Chunk/Thunk nodes
    deps = custom_dependents(t)
    c=1
    for k in keys(deps)
        c = write_node(io, k, c)
    end
    for (k, v) in deps
        for dep in v
            if isa(k, Union{Chunk, Thunk})
                println(io, "$(node_name(k)) -> $(node_name(dep))")
            end
        end
    end

    # Argument nodes (not Chunks/Thunks)
    argmap = Dict{Int,Vector}()
    getargs!(argmap, t)
    argids = IdDict{Any,String}()
    for id in keys(argmap)
        for (argidx,arg) in argmap[id]
            name = "arg_$(argidx)_to_$(id)"
            if !isimmutable(arg)
                if arg in keys(argids)
                    name = argids[arg]
                else
                    argids[arg] = name
                    c = write_node(io, arg, c, name)
                end
            else
                c = write_node(io, arg, c, name)
            end
            # Arg-to-compute edges
            write_edge(io, name, id)
        end
    end
end

### Timespan-based graphing

pretty_time(ts::Timespan) = pretty_time(ts.finish-ts.start)
function pretty_time(t)
    r(t) = round(t; digits=3)
    if t > 1000^3
        "$(r(t/(1000^3))) s"
    elseif t > 1000^2
        "$(r(t/(1000^2))) ms"
    elseif t > 1000
        "$(r(t/1000)) us"
    else
        "$(r(t)) ns"
    end
end
function pretty_size(sz)
    if sz > 1024^4
        "$(sz/(1024^4)) TB (terabytes)"
    elseif sz > 1024^3
        "$(sz/(1024^3)) GB (gigabytes)"
    elseif sz > 1024^2
        "$(sz/(1024^2)) MB (megabytes)"
    elseif sz > 1024
        "$(sz/1024) KB (kilobytes)"
    else
        "$sz B (bytes)"
    end
end

node_label(x) = repr(x)
node_label(x::T) where T<:AbstractArray =
    "$T\nShape: $(size(x))\nSize: $(pretty_size(sizeof(x)))"
node_label(x::Chunk) = "Chunk on $(x.processor)"

node_proc(x) = nothing
node_proc(x::Chunk) = x.processor

_proc_color(ctx, proc::Processor) = get!(ctx.proc_to_color, proc) do
    _color = ctx.proc_colors[ctx.proc_color_idx[]]
    ctx.proc_color_idx[] = clamp(ctx.proc_color_idx[]+1, 0, 128)
    "#$(Colors.hex(_color))"
end
_proc_color(ctx, id::Int) = _proc_color(ctx, ctx.id_to_proc[id])
_proc_color(ctx, ::Nothing) = "black"
_proc_shape(ctx, proc::Processor) = get!(ctx.proc_to_shape, typeof(proc)) do
    _shape = ctx.proc_shapes[ctx.proc_shape_idx[]]
    ctx.proc_shape_idx[] = clamp(ctx.proc_shape_idx[]+1, 0, length(ctx.proc_shapes))
    _shape
end
_proc_shape(ctx, ::Nothing) = "ellipse"

function write_node(io, t::Thunk, c, ctx=nothing)
    f = isa(t.f, Function) ? "$(t.f)" : "fn"
    println(io, "$(node_name(t)) [label=\"$f - $(t.id)\"];")
    c
end

dec(x) = Base.dec(x, 0, false)
function write_node(io, t, c, ctx, id=dec(hash(t)))
    l = replace(node_label(t), "\""=>"")
    proc = node_proc(t)
    color = _proc_color(ctx, proc)
    shape = _proc_shape(ctx, proc)
    println(io, "$(node_name(id)) [label=\"$l\",color=\"$color\",shape=\"$shape\",penwidth=5];")
    c
end

function write_node(io, t, c, name::String)
    l = replace(node_label(t), "\""=>"")
    println(io, "$(node_name(name)) [label=\"$l\"];")
    c
end

function write_node(io, ts::Timespan, c, ctx)
    (;thunk_id, processor) = ts.id
    (;f) = ts.timeline
    f = isa(f, Function) ? "$f" : "fn"
    t_comp = pretty_time(ts)
    color = _proc_color(ctx, processor)
    shape = _proc_shape(ctx, processor)
    # TODO: t_log = log(ts.finish - ts.start) / 5
    ctx.id_to_proc[thunk_id] = processor
    println(io, "$(node_name(thunk_id)) [label=\"$f\n$t_comp\",color=\"$color\",shape=\"$shape\",penwidth=5];")
    # TODO: "\n Thunk $(ts.id)\nResult Type: $res_type\nResult Size: $sz_comp\",
    c
end

function write_edge(io, ts_move::Timespan, logs, ctx, inputname=nothing, inputarg=nothing)
    (;thunk_id, id) = ts_move.id
    (;f,) = ts_move.timeline
    t_move = pretty_time(ts_move)
    if id > 0
        print(io, "$(node_name(id)) -> $(node_name(thunk_id)) [label=\"Move: $t_move")
        color_src = _proc_color(ctx, id)
    else
        @assert inputname !== nothing
        @assert inputarg !== nothing
        print(io, "$(node_name(inputname)) -> $(node_name(thunk_id)) [label=\"Move: $t_move")
        proc = node_proc(inputarg)
        color_src = _proc_color(ctx, proc)
    end
    color_dst = _proc_color(ctx, thunk_id)
    # TODO: log_t = log(ts_move.finish-ts_move.start) / 5
    println(io, "\",color=\"$color_src;0.5:$color_dst\",penwidth=2];")
end

write_edge(io, from::String, to::String, ctx=nothing) = println(io, "$(node_name(from)) -> $(node_name(to));")
write_edge(io, from::String, to::Int, ctx=nothing) = println(io, "$(node_name(from)) -> $(node_name(to));")

convert_to_thunk(t::Thunk) = t
convert_to_thunk(t::DTask) = Dagger.Sch._find_thunk(t)

getargs!(d, t) = nothing

function getargs!(d, t::Thunk)
    raw_inputs = map(last, t.inputs)
    d[t.id] = [filter(x->!istask(x[2]), collect(enumerate(raw_inputs)))...,]
    foreach(i->getargs!(d, i), raw_inputs)
end

function getargs!(d, e::DTask)
    getargs!(d, convert_to_thunk(e))
end

function write_dag(io, logs::Vector, t::Union{Thunk, DTask, Nothing}=nothing)
    ctx = (proc_to_color = Dict{Processor,String}(),
           proc_colors = Colors.distinguishable_colors(128),
           proc_color_idx = Ref{Int}(1),
           proc_to_shape = Dict{Type,String}(),
           proc_shapes = ("ellipse","box","triangle"),
           proc_shape_idx = Ref{Int}(1),
           id_to_proc = Dict{Int,Processor}())
    
    c = 1
    # Compute nodes
    for ts in filter(x->x.category==:compute, logs)
        c = write_node(io, ts, c, ctx)
    end

    # Argument nodes & edges
    argmap = Dict{Int,Vector}()
    argids = IdDict{Any,String}()
    if (t !== nothing) # Then can get info from the Thunk/DTask
        t = convert_to_thunk(t)
        deps = custom_dependents(t)
        for t in keys(deps)
            getargs!(argmap, t)
        end
        argnodemap = Dict{Int,Vector{String}}()
        for id in keys(argmap)
            nodes = String[]
            arg_c = 1
            for (argidx,arg) in argmap[id]
                name = "arg_$(argidx)_to_$(id)"
                if !isimmutable(arg)
                    if arg in keys(argids)
                        name = argids[arg]
                    else
                        argids[arg] = name
                        c = write_node(io, arg, c, ctx, name)
                    end
                    push!(nodes, name)
                else
                    c = write_node(io, arg, c, ctx, name)
                    push!(nodes, name)
                end
                # Arg-to-compute edges
                for ts in filter(x->x.category==:move &&
                                    x.id.thunk_id==id &&
                                    x.id.id==-argidx, logs)
                    write_edge(io, ts, logs, ctx, name, arg)
                end
                arg_c += 1
            end
            argnodemap[id] = nodes
        end
    else # Rely on the logs only
        for ts in filter(x->x.category==:move && x.id.id < 0, logs)
            (;thunk_id, id) = ts.id
            arg = ts.timeline[2]
            name = "arg_$(-id)_to_$(thunk_id)"
            if !isimmutable(arg)
                if arg in keys(argids)
                    name = argids[arg]
                else
                    argids[arg] = name
                    c = write_node(io, arg, c, ctx, name)
                end
            else
                c = write_node(io, arg, c, ctx, name)
            end
        
            # Arg-to-compute edges
            write_edge(io, ts, logs, ctx, name, arg)
        end
    end
           
    # Move edges
    for ts in filter(x->x.category==:move && x.id.id>0, logs)
        write_edge(io, ts, logs, ctx)
    end
    #= FIXME: Legend (currently it's laid out horizontally)
    println(io, """
    subgraph  {
        graph[style=dotted,newrank=true,rankdir=TB];
        edge [style=invis];
        Legend [shape=box];""")
    cur = "Legend"
    for id in keys(ctx.id_to_proc)
        color = _proc_color(ctx, id)
        shape = _proc_shape(ctx, ctx.id_to_proc[id])
        name = "proc_$id"
        println(io, "$name [color=\"$color\",shape=\"$shape\"];")
        println(io, "$cur -> $name;")
        cur = name
    end
    println(io, "}")
    =#
end

function _show_plan(io::IO, t)
    println(io, """strict digraph {
    graph [layout=dot,rankdir=LR];""")
    write_dag(io, t)
    println(io, "}")
end
function _show_plan(io::IO, t::Union{Thunk,DTask}, logs::Vector{Timespan})
    println(io, """strict digraph {
    graph [layout=dot,rankdir=LR];""")
    write_dag(io, logs, t)
    println(io, "}")
end

show_logs(io::IO, t::Union{Thunk,DTask}, ::Val{:graphviz_simple}) = _show_plan(io, t)
show_logs(io::IO, logs::Vector{Timespan}, ::Val{:graphviz_simple}) = _show_plan(io, logs)
show_logs(io::IO, t::Union{Thunk,DTask}, logs::Vector{Timespan}, ::Val{:graphviz_simple}) = _show_plan(io, t, logs)

end
