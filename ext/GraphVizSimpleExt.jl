module GraphVizSimpleExt

if isdefined(Base, :get_extension)
    using Colors
else
    using ..Colors
end

import Dagger
import Dagger: Chunk, Thunk, Processor
import Dagger: show_logs
import Dagger.TimespanLogging: Timespan

### DAG-based graphing

global _part_labels = Dict()

function write_node(ctx, io, t::Chunk, c)
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

function write_dag(io, t::Thunk)
    !istask(t) && return
    deps = dependents(t)
    c=1
    for k in keys(deps)
        c = write_node(nothing, io, k, c)
    end
    for (k, v) in deps
        for dep in v
            if isa(k, Union{Chunk, Thunk})
                println(io, "$(node_id(k)) -> $(node_id(dep))")
            end
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

function write_node(ctx, io, t::Thunk, c)
    f = isa(t.f, Function) ? "$(t.f)" : "fn"
    println(io, "n_$(t.id) [label=\"$f - $(t.id)\"];")
    c
end

dec(x) = Base.dec(x, 0, false)
function write_node(ctx, io, t, c, id=dec(hash(t)))
    l = replace(node_label(t), "\""=>"")
    proc = node_proc(t)
    color = _proc_color(ctx, proc)
    shape = _proc_shape(ctx, proc)
    println(io, "n_$id [label=\"$l\",color=\"$color\",shape=\"$shape\",penwidth=5];")
    c
end

function write_node(ctx, io, ts::Timespan, c)
    (;thunk_id, processor) = ts.id
    (;f) = ts.timeline
    f = isa(f, Function) ? "$f" : "fn"
    t_comp = pretty_time(ts)
    color = _proc_color(ctx, processor)
    shape = _proc_shape(ctx, processor)
    # TODO: t_log = log(ts.finish - ts.start) / 5
    ctx.id_to_proc[thunk_id] = processor
    println(io, "n_$thunk_id [label=\"$f\n$t_comp\",color=\"$color\",shape=\"$shape\",penwidth=5];")
    # TODO: "\n Thunk $(ts.id)\nResult Type: $res_type\nResult Size: $sz_comp\",
    c
end

function write_edge(ctx, io, ts_move::Timespan, logs, inputname=nothing, inputarg=nothing)
    (;thunk_id, id) = ts_move.id
    (;f,) = ts_move.timeline
    t_move = pretty_time(ts_move)
    if id > 0
        print(io, "n_$id -> n_$thunk_id [label=\"Move: $t_move")
        color_src = _proc_color(ctx, id)
    else
        @assert inputname !== nothing
        @assert inputarg !== nothing
        print(io, "n_$inputname -> n_$thunk_id [label=\"Move: $t_move")
        proc = node_proc(inputarg)
        color_src = _proc_color(ctx, proc)
    end
    color_dst = _proc_color(ctx, thunk_id)
    # TODO: log_t = log(ts_move.finish-ts_move.start) / 5
    println(io, "\",color=\"$color_src;0.5:$color_dst\",penwidth=2];")
end

write_edge(ctx, io, from::String, to::String) = println(io, "n_$from -> n_$to;")

getargs!(d, t) = nothing
function getargs!(d, t::Thunk)
    raw_inputs = map(last, t.inputs)
    d[t.id] = [filter(x->!istask(x[2]), collect(enumerate(raw_inputs)))...,]
    foreach(i->getargs!(d, i), raw_inputs)
end
function write_dag(io, t, logs::Vector)
    ctx = (proc_to_color = Dict{Processor,String}(),
           proc_colors = Colors.distinguishable_colors(128),
           proc_color_idx = Ref{Int}(1),
           proc_to_shape = Dict{Type,String}(),
           proc_shapes = ("ellipse","box","triangle"),
           proc_shape_idx = Ref{Int}(1),
           id_to_proc = Dict{Int,Processor}())
    argmap = Dict{Int,Vector}()
    getargs!(argmap, t)
    c = 1
    # Compute nodes
    for ts in filter(x->x.category==:compute, logs)
        c = write_node(ctx, io, ts, c)
    end
    # Argument nodes
    argnodemap = Dict{Int,Vector{String}}()
    argids = IdDict{Any,String}()
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
                    c = write_node(ctx, io, arg, c, name)
                end
                push!(nodes, name)
            else
                c = write_node(ctx, io, arg, c, name)
                push!(nodes, name)
            end
            # Arg-to-compute edges
            for ts in filter(x->x.category==:move &&
                                x.id.thunk_id==id &&
                                x.id.id==-argidx, logs)
                write_edge(ctx, io, ts, logs, name, arg)
            end
            arg_c += 1
        end
        argnodemap[id] = nodes
    end
    # Move edges
    for ts in filter(x->x.category==:move && x.id.id>0, logs)
        write_edge(ctx, io, ts, logs)
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
function _show_plan(io::IO, t::Thunk, logs::Vector{Timespan})
    println(io, """strict digraph {
    graph [layout=dot,rankdir=LR];""")
    write_dag(io, t, logs)
    println(io, "}")
end

show_logs(io::IO, t::Thunk, ::Val{:graphviz_simple}) = _show_plan(io, t)
show_logs(io::IO, logs::Vector{Timespan}, ::Val{:graphviz_simple}) = _show_plan(io, logs)
show_logs(io::IO, t::Thunk, logs::Vector{Timespan}, ::Val{:graphviz_simple}) = _show_plan(io, t, logs)

end
