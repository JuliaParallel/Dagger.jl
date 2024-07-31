module GraphVizExt

if isdefined(Base, :get_extension)
    using GraphViz
else
    using ..GraphViz
end

import Dagger
import Dagger: DTask, Chunk, Processor, LoggedMutableObject
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

sanitize_label(label::String) = replace(label, "\"" => "\\\"")

_name_to_color(name::AbstractString, colors) =
    colors[mod1(hash(name), length(colors))]
_name_to_color(name::AbstractString, ::Nothing) = "black"
tab20_colors = [
    "#1f77b4", "#aec7e8", "#ff7f0e", "#ffbb78",
    "#2ca02c", "#98df8a", "#d62728", "#ff9896",
    "#9467bd", "#c5b0d5", "#8c564b", "#c49c94",
    "#e377c2", "#f7b6d2", "#7f7f7f", "#c7c7c7",
    "#bcbd22", "#dbdb8d", "#17becf", "#9edae5"
]
_default_colors = tab20_colors

"""
    Dagger.render_logs(logs::Dict, ::Val{:graphviz}; disconnected=false,
                       color_by=:fn, layout_engine="dot",
                       times::Bool=true, times_digits::Integer=3)

Render a graph of the task dependencies and data dependencies in `logs` using GraphViz.

Requires the `all_task_deps` event enabled in `enable_logging!`

Options:
- `disconnected`: If `true`, render disconnected vertices (tasks or arguments without upstream/downstream dependencies)
- `color_by`: How to color tasks; if `:fn`, then color by unique function name, if `:proc`, then color by unique processor
- `layout_engine`: The layout engine to use for GraphViz
- `times`: If `true`, annotate each task with its start and finish times
- `times_digits`: Number of digits to display in the time annotations
"""
function Dagger.render_logs(logs::Dict, ::Val{:graphviz}; disconnected=false,
                            color_by=:fn, layout_engine="dot",
                            times::Bool=true, times_digits::Integer=3,
                            colors=_default_colors, name_to_color=_name_to_color)
    # Lookup all relevant task/argument dependencies and values in logs
    g = SimpleDiGraph()

    tid_to_vertex = Dict{Int,Int}()
    tid_to_auto_name = Dict{Int,String}()
    tid_to_name = Dict{Int,String}()
    tid_to_proc = Dict{Int,Processor}()

    objid_to_vertex = Dict{UInt,Int}()
    objid_to_name = Dict{UInt,String}()

    task_args = Dict{Int,Vector{Pair{Union{Int,Symbol},UInt}}}()
    task_arg_moves = Dict{Int,Vector{Pair{Union{Int,Symbol},Tuple{UInt,UInt}}}}()
    task_result = Dict{Int,UInt}()

    uid_to_tid = Dict{UInt,Int}()
    dtasks_to_patch = Set{UInt}()

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
                v = get!(tid_to_vertex, tid) do
                    add_vertex!(g)
                    tid_to_vertex[tid] = nv(g)
                    nv(g)
                end
                tid_to_auto_name[tid] = taskname
                for dep in deps
                    dep_v = get!(tid_to_vertex, dep) do
                        add_vertex!(g)
                        tid_to_vertex[dep] = nv(g)
                        nv(g)
                    end
                    add_edge!(g, dep_v, v)
                end
                if haskey(logs[w], :taskuidtotid)
                    uid_tid = logs[w][:taskuidtotid][idx]
                    if uid_tid !== nothing
                        uid, tid = uid_tid::Pair{UInt,Int}
                        uid_to_tid[uid] = tid
                    end
                end
            elseif category == :compute && kind == :start
                id::NamedTuple
                tid = id.thunk_id
                proc = id.processor
                tid_to_proc[tid] = proc
            elseif category == :compute && kind == :finish
                if haskey(logs[w], :taskresult)
                    result_info = logs[w][:taskresult][idx]
                    result_info === nothing && continue
                    tid, obj = result_info::Pair{Int,LoggedMutableObject}
                    objid = obj.objid
                    task_result[tid] = objid
                    tid_v = get!(tid_to_vertex, tid) do
                        add_vertex!(g)
                        tid_to_vertex[tid] = nv(g)
                        nv(g)
                    end
                    v = get!(objid_to_vertex, objid) do
                        add_vertex!(g)
                        objid_to_vertex[objid] = nv(g)
                        nv(g)
                    end
                    add_edge!(g, tid_v, v)
                end
            elseif category == :move && kind == :finish
                if haskey(logs[w], :taskargs)
                    tid, args = logs[w][:taskargs][idx]::Pair{Int,<:Vector}
                    args = map(arg->arg[1]=>arg[2].objid, args)
                    append!(get!(Vector{Pair{Union{Int,Symbol},UInt}}, task_args, tid), args)
                    for arg in args
                        objid = arg[2]
                        arg_id = get!(objid_to_vertex, objid) do
                            add_vertex!(g)
                            objid_to_vertex[objid] = nv(g)
                            nv(g)
                        end
                        if tid != 0
                            tid_v = get!(tid_to_vertex, tid) do
                                add_vertex!(g)
                                tid_to_vertex[tid] = nv(g)
                                nv(g)
                            end
                            add_edge!(g, arg_id, tid_v)
                        end
                    end
                end
                if haskey(logs[w], :taskargmoves)
                    move_info = logs[w][:taskargmoves][idx]
                    move_info === nothing && continue
                    tid, pos, pre_obj, post_obj = move_info
                    v = get!(Vector{Pair{Union{Int,Symbol},Tuple{UInt,UInt}}}, task_arg_moves, tid)
                    pre_objid = pre_obj.objid
                    post_objid = post_obj.objid
                    push!(v, pos => (pre_objid, post_objid))
                    pre_arg_id = get!(objid_to_vertex, pre_objid) do
                        add_vertex!(g)
                        objid_to_vertex[pre_objid] = nv(g)
                        nv(g)
                    end
                    post_arg_id = get!(objid_to_vertex, post_objid) do
                        add_vertex!(g)
                        objid_to_vertex[post_objid] = nv(g)
                        nv(g)
                    end
                    add_edge!(g, pre_arg_id, post_arg_id)
                end
            elseif category == :data_annotation && kind == :start
                id::NamedTuple
                name = String(id.name)
                obj = id.objectid::LoggedMutableObject
                objid = obj.objid
                objid_to_name[objid] = name
                if obj.kind == :task
                    # N.B. We don't need the object vertex,
                    # since we'll just render it as a task
                    push!(dtasks_to_patch, objid)
                else
                    get!(objid_to_vertex, objid) do
                        add_vertex!(g)
                        objid_to_vertex[objid] = nv(g)
                        nv(g)
                    end
                end
            elseif category == :finish && kind == :finish
                if haskey(logs[w], :tasktochunk)
                    tid_chunk = logs[w][:tasktochunk][idx]
                    if tid_chunk !== nothing
                        tid, chunk_obj = tid_chunk::Pair{Int,LoggedMutableObject}
                        chunk_id = chunk_obj.objid
                        v = get!(objid_to_vertex, chunk_id) do
                            add_vertex!(g)
                            objid_to_vertex[chunk_id] = nv(g)
                            nv(g)
                        end
                        add_edge!(g, tid_to_vertex[tid], v)
                    end
                end
            end
        end
    end

    # Process DTasks-to-Thunk mappings
    for uid in dtasks_to_patch
        if haskey(uid_to_tid, uid)
            tid = uid_to_tid[uid]
            v = get!(tid_to_vertex, tid) do
                add_vertex!(g)
                tid_to_vertex[tid] = nv(g)
                nv(g)
            end

            # Fixup any missing tid data
            if haskey(objid_to_name, uid)
                tid_to_name[tid] = objid_to_name[uid]
            end
        end
    end

    # Auto-assign names
    for (tid, name) in tid_to_auto_name
        if !haskey(tid_to_name, tid)
            tid_to_name[tid] = name
        end
    end

    # Create reverse mappings
    vertex_to_tid = Dict{Int,Int}(v=>k for (k,v) in tid_to_vertex)
    vertex_to_objid = Dict{Int,UInt}(v=>k for (k,v) in objid_to_vertex)

    # Find all connected and disconnected vertices
    if !disconnected
        discon_vs = filter(v->isempty(inneighbors(g, v)) && isempty(outneighbors(g, v)), vertices(g))
        con_vs = filter(v->!in(v, discon_vs), vertices(g))
    else
        con_vs = vertices(g)
    end

    if times
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

    # Get the set of all unique task and object IDs
    all_tids = collect(keys(tid_to_vertex))
    all_objids = collect(keys(objid_to_vertex))

    # Assign colors
    if color_by == :fn
        all_fns = unique(values(tid_to_name))
        _colors = [name_to_color(all_fns[i], colors) for i in 1:length(all_fns)]
        colors = Dict(tid=>_colors[findfirst(fn->occursin(fn, tid_to_name[tid]), all_fns)] for tid in all_tids)
    elseif color_by == :proc
        all_procs = unique(values(tid_to_proc))
        _colors = [name_to_color(string(all_procs[i]), colors) for i in 1:length(all_procs)]
        colors = Dict(tid=>_colors[findfirst(proc->proc==tid_to_proc[tid], all_procs)] for tid in all_tids)
    else
        throw(ArgumentError("Unknown `color_by` value: $color_by\nAllowed: :fn, :proc"))
    end

    str = is_directed(g) ? "digraph mygraph {\n" : "graph mygraph {\n"

    # Add task vertices
    for tid in all_tids
        v = tid_to_vertex[tid]
        if !disconnected && (v in discon_vs)
            continue
        end
        label_str = tid_to_name[tid]
        if haskey(tid_to_auto_name, tid) && tid_to_name[tid] != tid_to_auto_name[tid]
            label_str *= "\\nTask: $(tid_to_auto_name[tid])"
        end
        color = colors[tid]
        proc = tid_to_proc[tid]
        label_str *= "\\n($(Dagger.short_name(tid_to_proc[tid])))"
        if times
            start_time = pretty_time(start_times[tid]; digits=times_digits)
            finish_time = pretty_time(finish_times[tid]; digits=times_digits)
            diff_time = pretty_time(finish_times[tid] - start_times[tid]; digits=times_digits)
            label_str *= "\\n[+$start_time -> +$finish_time (diff: $diff_time)]"
        end
        label_str = sanitize_label(label_str)
        str *= "v$v [label=\"$label_str\", shape=box, color=\"$color\", penwidth=2.0]\n"
    end

    # Add object vertices
    for objid in all_objids
        objid_v = objid_to_vertex[objid]
        if !disconnected && !(objid_v in con_vs)
            continue
        end
        if objid in dtasks_to_patch || haskey(uid_to_tid, objid)
            # DTask, skip it
            continue
        end
        # Object
        if haskey(objid_to_name, objid)
            label = sanitize_label(objid_to_name[objid])
            label *= "\\nData: $(repr(objid))"
        else
            label = "Data: $(repr(objid))"
        end
        str *= "a$objid_v [label=\"$label\", shape=oval]\n"
    end

    # Add task argument move edges
    seen_moves = Set{Tuple{UInt,UInt}}()
    for (tid, moves) in task_arg_moves
        for (pos, (pre_objid, post_objid)) in moves
            pre_objid == post_objid && continue
            (pre_objid, post_objid) in seen_moves && continue
            push!(seen_moves, (pre_objid, post_objid))
            pre_objid_v = objid_to_vertex[pre_objid]
            post_objid_v = objid_to_vertex[post_objid]
            move_str = "a$pre_objid_v -> a$post_objid_v [label=\"move\"]\n"
            str *= move_str
        end
    end

    # Add task-to-task (syncdep) dependency edges
    edge_sep = is_directed(g) ? "->" : "--"
    for edge in edges(g)
        if !haskey(vertex_to_tid, src(edge)) || !haskey(vertex_to_tid, dst(edge))
            continue
        end
        if !disconnected && !(src(edge) in con_vs) || !(dst(edge) in con_vs)
            continue
        end
        # FIXME: Label syncdeps with associated arguments and datadeps directions
        str *= "v$(src(edge)) $edge_sep v$(dst(edge)) [label=\"syncdep\"]\n"
    end

    # Add task argument edges
    for (tid, args) in task_args
        haskey(tid_to_vertex, tid) || continue
        tid_v = tid_to_vertex[tid]
        tid_v in con_vs || continue
        for (pos, arg) in args
            arg_v = objid_to_vertex[arg]
            if !disconnected && !(arg_v in con_vs)
                continue
            end
            arg_str = sanitize_label(pos isa Int ? "arg $pos" : "kwarg $pos")
            str *= "a$arg_v $edge_sep v$tid_v [label=\"$arg_str\"]\n"
        end
    end

    # Add task result edges
    for (tid, result) in task_result
        haskey(tid_to_vertex, tid) || continue
        tid_v = tid_to_vertex[tid]
        tid_v in con_vs || continue
        result_v = objid_to_vertex[result]
        if !disconnected && !(result_v in con_vs)
            continue
        end
        str *= "v$tid_v $edge_sep a$result_v [label=\"result\"]\n"
    end

    # Generate the final graph
    str *= "}\n"
    gv = GraphViz.Graph(str)
    GraphViz.layout!(gv; engine=layout_engine)

    return gv
end

end
