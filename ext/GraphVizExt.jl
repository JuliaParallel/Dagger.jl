module GraphVizExt

if isdefined(Base, :get_extension)
    using GraphViz
else
    using ..GraphViz
end

import Dagger
import Dagger: EagerThunk, Chunk
import Dagger.TimespanLogging: Timespan
using Dagger.Graphs

function to_dot(g, labels, procs, tid_to_vertex, arg_names, task_args; disconnected=false, color_by=:fn, layout_engine="dot")
    if !disconnected
        discon_vs = filter(v->isempty(inneighbors(g, v)) && isempty(outneighbors(g, v)), vertices(g))
        con_vs = filter(v->!in(v, discon_vs), vertices(g))
    else
        con_vs = vertices(g)
    end
    all_fns = unique(map(label->first(split(label, " ")), labels[con_vs]))
    all_procs = unique(procs)
    all_colors = ("red", "orange", "green", "blue", "purple", "pink", "silver")
    if color_by == :fn
        _colors = [all_colors[mod1(i, length(all_colors))] for i in 1:length(all_fns)]
        colors = Dict(v=>_colors[findfirst(fn->occursin(fn, labels[v]), all_fns)] for v in con_vs)
    elseif color_by == :proc
        _colors = [all_colors[mod1(i, length(all_colors))] for i in 1:length(all_procs)]
        colors = Dict(v=>_colors[findfirst(proc->proc==procs[v], all_procs)] for v in con_vs)
    else
        throw(ArgumentError("Unknown `color_by` value: $color_by\nAllowed: :fn, :proc"))
    end
    str = is_directed(g) ? "digraph mygraph {\n" : "graph mygraph {\n"

    # Add raw arguments
    for (id, name) in arg_names
        str *= "a$id [label=\"$name\", shape=box]\n"
    end

    # Add tasks
    for v in con_vs
        if !disconnected && (v in discon_vs)
            continue
        end
        label = labels[v]
        color = colors[v]
        proc = procs[v]
        proc_str = "($(proc.owner), $(proc.tid))"
        str *= "v$v [label=\"$label\\n$proc_str\", color=\"$color\", penwidth=2.0]\n"
    end

    # Add task dependencies
    edge_sep = is_directed(g) ? "->" : "--"
    for edge in edges(g)
        # FIXME: Label syncdeps with associated arguments and datadeps directions
        str *= "v$(src(edge)) $edge_sep v$(dst(edge))\n"
    end

    # Add task arguments
    for (tid, args) in task_args
        id = tid_to_vertex[tid]
        id in con_vs || continue
        for (pos, arg) in args
            # FIXME: Show argument position
            if !disconnected && !(arg in keys(arg_names))
                continue
            end
            str *= "a$arg $edge_sep v$id\n"
        end
    end

    str *= "}\n"
    gv = GraphViz.Graph(str)
    GraphViz.layout!(gv; engine=layout_engine)
    return gv
end

function logs_task_args(logs)
    arg_names = Dict{UInt,String}()
    task_args = Dict{Int,Vector{Pair{Union{Int,Symbol},UInt}}}()
    for w in keys(logs)
        for idx in 1:length(logs[w][:core])
            category = logs[w][:core][idx].category
            kind = logs[w][:core][idx].kind
            id = logs[w][:id][idx]
            if category == :data_annotation && kind == :start
                id::NamedTuple
                objid = id.objectid
                name = id.name
                arg_names[objid] = name
            elseif category == :add_thunk && kind == :start
                if haskey(logs[w], :taskargs)
                    id, args = logs[w][:taskargs][idx]::Pair{Int,<:Vector}
                    append!(get!(Vector{UInt}, task_args, id), args)
                end
            end
        end
    end
    return arg_names, task_args
end

function Dagger.render_logs(logs::Dict, ::Val{:graphviz}; options...)
    g, tid_to_vertex, task_names, task_procs = Dagger.logs_task_dependencies(logs)
    arg_names, task_args = logs_task_args(logs)
    return to_dot(g, task_names, task_procs, tid_to_vertex, arg_names, task_args; options...)
end

end
