module GraphVizExt

if isdefined(Base, :get_extension)
    using GraphViz
else
    using ..GraphViz
end

import Dagger
import Dagger: DTask, Chunk, Processor
import Dagger.TimespanLogging: Timespan
import Graphs: SimpleDiGraph, add_edge!, add_vertex!, inneighbors, outneighbors, vertices, is_directed, edges, nv, src, dst

function pretty_time(t; digits=3)
    r(t) = round(t; digits)
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

"""
    Dagger.render_logs(logs::Dict, ::Val{:graphviz}; disconnected=false,
                       color_by=:fn, layout_engine="dot",
                       times::Bool=true, times_digits::Integer=3)

Render a graph of the task dependencies and data dependencies in `logs` using GraphViz.

Requires the following events enabled in `enable_logging!`: `taskdeps`, `tasknames`, `taskargs`, `taskargmoves`

Options:
- `disconnected`: If `true`, render disconnected vertices (tasks or arguments without upstream/downstream dependencies)
- `color_by`: How to color tasks; if `:fn`, then color by unique function name, if `:proc`, then color by unique processor
- `layout_engine`: The layout engine to use for GraphViz
- `times`: If `true`, annotate each task with its start and finish times
- `times_digits`: Number of digits to display in the time annotations
"""
function Dagger.render_logs(logs::Dict, ::Val{:graphviz}; disconnected=false,
                            color_by=:fn, layout_engine="dot",
                            times::Bool=true, times_digits::Integer=3)
    # Lookup all relevant task/argument dependencies and values in logs
    g = SimpleDiGraph()
    tid_to_vertex = Dict{Int,Int}()
    task_names = String[]
    tid_to_proc = Dict{Int,Processor}()
    arg_names = Dict{UInt,String}()
    task_args = Dict{Int,Vector{Pair{Union{Int,Symbol},UInt}}}()
    arg_moves = Dict{Int,Vector{Pair{Union{Int,Symbol},Tuple{UInt,UInt}}}}()
    for w in keys(logs)
        for idx in 1:length(logs[w][:core])
            category = logs[w][:core][idx].category
            kind = logs[w][:core][idx].kind
            id = logs[w][:id][idx]
            if category == :add_thunk && kind == :start
                id::NamedTuple
                taskdeps = logs[w][:taskdeps][idx]::Pair{Int,Vector{Int}}
                taskname = logs[w][:tasknames][idx]::String
                tid, deps = taskdeps
                add_vertex!(g)
                tid_to_vertex[tid] = nv(g)
                push!(task_names, taskname)
                for dep in deps
                    add_edge!(g, tid_to_vertex[dep], nv(g))
                end
                if haskey(logs[w], :taskargs)
                    id, args = logs[w][:taskargs][idx]::Pair{Int,<:Vector}
                    append!(get!(Vector{Pair{Union{Int,Symbol},UInt}}, task_args, id), args)
                end
            elseif category == :compute && kind == :start
                id::NamedTuple
                tid = id.thunk_id
                proc = id.processor
                tid_to_proc[tid] = proc
            elseif category == :move && kind == :finish
                if haskey(logs[w], :taskargmoves)
                    move_info = logs[w][:taskargmoves][idx]
                    move_info === nothing && continue
                    tid, pos, pre_objid, post_objid = move_info
                    v = get!(Vector{Pair{Union{Int,Symbol},Tuple{UInt,UInt}}}, arg_moves, tid)
                    push!(v, pos => (pre_objid, post_objid))
                end
            elseif category == :data_annotation && kind == :start
                id::NamedTuple
                objid = id.objectid
                name = id.name
                arg_names[objid] = name
            end
        end
    end
    tids_sorted = map(first, sort(collect(tid_to_vertex); by=last))
    task_procs = Processor[tid_to_proc[tid] for tid in tids_sorted]

    # Find all connected and disconnected vertices
    if !disconnected
        discon_vs = filter(v->isempty(inneighbors(g, v)) && isempty(outneighbors(g, v)), vertices(g))
        con_vs = filter(v->!in(v, discon_vs), vertices(g))
    else
        con_vs = vertices(g)
    end

    # Assign colors
    labels = task_names
    all_fns = unique(map(label->first(split(label, " ")), labels[con_vs]))
    all_procs = unique(task_procs)
    all_colors = ("red", "orange", "green", "blue", "purple", "pink", "silver")
    if color_by == :fn
        _colors = [all_colors[mod1(i, length(all_colors))] for i in 1:length(all_fns)]
        colors = Dict(v=>_colors[findfirst(fn->occursin(fn, labels[v]), all_fns)] for v in con_vs)
    elseif color_by == :proc
        _colors = [all_colors[mod1(i, length(all_colors))] for i in 1:length(all_procs)]
        colors = Dict(v=>_colors[findfirst(proc->proc==task_procs[v], all_procs)] for v in con_vs)
    else
        throw(ArgumentError("Unknown `color_by` value: $color_by\nAllowed: :fn, :proc"))
    end

    str = is_directed(g) ? "digraph mygraph {\n" : "graph mygraph {\n"

    # Add raw arguments
    for (id, name) in arg_names
        str *= "a$id [label=\"$name\", shape=box]\n"
    end

    if times
        vertex_to_tid = Dict{Int,Int}(v=>k for (k,v) in tid_to_vertex)

        # Determine per-worker start times
        worker_start_times = Dict{Int,UInt64}()
        for w in keys(logs)
            start = typemax(UInt64)
            for idx in 1:length(logs[w][:core])
                if logs[w][:core][idx].category == :compute && logs[w][:core][idx].kind == :start
                    tid = logs[w][:id][idx].thunk_id
                    haskey(tid_to_vertex, tid) || continue
                    id = tid_to_vertex[tid]
                    id in con_vs || continue
                    start = min(start, logs[w][:core][idx].timestamp)
                end
            end
            worker_start_times[w] = start
        end

        # Determine per-task start and finish times
        start_times = Dict{Int,UInt64}()
        finish_times = Dict{Int,UInt64}()
        for w in keys(logs)
            start = typemax(UInt64)
            for idx in 1:length(logs[w][:core])
                if logs[w][:core][idx].category == :compute
                    tid = logs[w][:id][idx].thunk_id
                    if logs[w][:core][idx].kind == :start
                        start_times[tid] = logs[w][:core][idx].timestamp - worker_start_times[w]
                    else
                        finish_times[tid] = logs[w][:core][idx].timestamp - worker_start_times[w]
                    end
                end
            end
        end
    end

    # Add argument moves
    for (tid, moves) in arg_moves
        for (pos, (pre_objid, post_objid)) in moves
            move_str = "a$pre_objid -> a$post_objid [label=\"move\"]\n"
            str *= move_str
        end
    end

    # Add tasks
    for v in con_vs
        if !disconnected && (v in discon_vs)
            continue
        end
        label = labels[v]
        color = colors[v]
        proc = task_procs[v]
        proc_str = '(' * Dagger.short_name(task_procs[v]) * ')'
        label_str = "$label\\n$proc_str"
        if times
            tid = vertex_to_tid[v]
            start_time = pretty_time(start_times[tid]; digits=times_digits)
            finish_time = pretty_time(finish_times[tid]; digits=times_digits)
            label_str *= "\\n[+$start_time -> +$finish_time]"
        end
        str *= "v$v [label=\"$label_str\", color=\"$color\", penwidth=2.0]\n"
    end

    # Add task dependencies
    edge_sep = is_directed(g) ? "->" : "--"
    for edge in edges(g)
        # FIXME: Label syncdeps with associated arguments and datadeps directions
        str *= "v$(src(edge)) $edge_sep v$(dst(edge)) [label=\"syncdep\"]\n"
    end

    # Add task arguments
    con_args = Vector{UInt}(collect(keys(arg_names)))
    for moves in values(arg_moves)
        for (_, (pre_objid, post_objid)) in moves
            push!(con_args, pre_objid)
            push!(con_args, post_objid)
        end
    end
    for (tid, args) in task_args
        id = tid_to_vertex[tid]
        id in con_vs || continue
        for (pos, arg) in args
            if !disconnected && !(arg in con_args)
                continue
            end
            arg_str = pos isa Int ? "arg $pos" : "kwarg $pos"
            str *= "a$arg $edge_sep v$id [label=\"$arg_str\"]\n"
        end
    end

    str *= "}\n"
    gv = GraphViz.Graph(str)
    GraphViz.layout!(gv; engine=layout_engine)
    return gv
end

end
