module GraphVizExt

if isdefined(Base, :get_extension)
    using GraphViz
    using Graphs
else
    using ..GraphViz
    using ..Graphs
end

import Dagger
import Dagger: EagerThunk, Chunk
import Dagger.TimespanLogging: Timespan

function to_dot(g, labels, procs; disconnected=false, color_by=:fn, layout_engine="dot")
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
    end
    str = is_directed(g) ? "digraph mygraph {\n" : "graph mygraph {\n"
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
    edge_sep = is_directed(g) ? "->" : "--"
    for edge in edges(g)
        str *= "v$(src(edge)) $edge_sep v$(dst(edge))\n"
    end
    str *= "}\n"
    gv = GraphViz.Graph(str)
    GraphViz.layout!(gv; engine=layout_engine)
    return gv
end

function Dagger.render_logs(logs::Dict, ::Val{:graphviz}; options...)
    g, tid_to_vertex, task_names, task_procs = Dagger.logs_task_dependencies(logs)
    return to_dot(g, task_names, task_procs; options...)
end

end
