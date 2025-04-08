module PlotsExt

if isdefined(Base, :get_extension)
    using Plots
    using DataFrames
else
    using ..Plots
    using ..DataFrames
end

import Dagger
import Dagger: DTask, Chunk, Processor
import Dagger.TimespanLogging: Timespan

function logs_to_df(logs::Dict, ::Val{:execution};
                    colors, name_to_color, color_by)
    # Generate function names
    fn_names = Dict{Int, String}()
    dtask_names = Dict{UInt, String}()
    uid_to_tid = Dict{UInt, Int}()
    for w in keys(logs)
        for idx in 1:length(logs[w][:core])
            category = logs[w][:core][idx].category::Symbol
            kind = logs[w][:core][idx].kind::Symbol
            if category == :add_thunk && kind == :start
                tid = logs[w][:id][idx].thunk_id::Int
                if haskey(logs[w], :taskfuncnames)
                    fn_names[tid] = logs[w][:taskfuncnames][idx]::String
                else
                    @warn "Task names missing from logs"
                    fn_names[tid] = "unknown"
                end
                if haskey(logs[w], :taskuidtotid)
                    uid_tid = logs[w][:taskuidtotid][idx]
                    if uid_tid !== nothing
                        uid, tid = uid_tid::Pair{UInt,Int}
                        uid_to_tid[uid] = tid
                    end
                end
            elseif category == :data_annotation && kind == :start
                id = logs[w][:id][idx]::NamedTuple
                name = String(id.name)
                obj = id.objectid::Dagger.LoggedMutableObject
                objid = obj.objid
                if obj.kind == :task
                    dtask_names[objid] = name
                end
            end
        end
    end

    # Process DTasks-to-Thunk mappings
    for (uid, tid) in uid_to_tid
        # Patch in custom name
        if haskey(dtask_names, uid)
            fn_names[tid] = dtask_names[uid]
        end
    end

    # FIXME: Color eltype
    df = DataFrame(proc=Processor[], proc_name=String[], fn_name=String[], tid=Int[], t_start=UInt64[], t_end=UInt64[], color=Any[])
    Dagger.logs_event_pairs(logs) do w, start_idx, finish_idx
        category = logs[w][:core][start_idx].category
        if category == :compute
            proc = logs[w][:id][start_idx].processor::Processor
            proc_name = Dagger.short_name(proc)
            tid = logs[w][:id][start_idx].thunk_id::Int
            fn_name = fn_names[tid]
            t_start = logs[w][:core][start_idx].timestamp::UInt64
            t_end = logs[w][:core][finish_idx].timestamp::UInt64
            if color_by == :fn
                color = name_to_color(fn_name, colors)
            elseif color_by == :proc
                color = name_to_color(proc_name, colors)
            else
                throw(ArgumentError("Invalid color_by value: $(repr(color_by))"))
            end
            push!(df, (;proc, proc_name, fn_name, tid, t_start, t_end, color))
        end
    end
    return df
end
function logs_to_df(logs::Dict, ::Val{:processor};
                    colors, name_to_color, kwargs...)
    # Collect processor events
    # FIXME: Color eltype
    df = DataFrame(proc=Processor[], proc_name=String[], category=String[], t_start=UInt64[], t_end=UInt64[], color=Any[])
    Dagger.logs_event_pairs(logs) do w, start_idx, finish_idx
        category = logs[w][:core][start_idx].category
        if category in (:compute, :storage_wait, :storage_safe_scan, :proc_run_fetch, :proc_steal_local)
            proc = logs[w][:id][start_idx].processor::Processor
            proc_name = Dagger.short_name(proc)
            t_start = logs[w][:core][start_idx].timestamp::UInt64
            t_end = logs[w][:core][finish_idx].timestamp::UInt64
            category_str = string(category)
            color = name_to_color(category_str, colors)
            push!(df, (;proc, proc_name, category=category_str, t_start, t_end, color))
        end
    end
    return df
end
function logs_to_df(logs::Dict, ::Val{:scheduler};
                    colors, name_to_color, kwargs...)
    # Collect scheduler events
    # FIXME: Color eltype
    df = DataFrame(category=String[], t_start=UInt64[], t_end=UInt64[], color=Any[])
    Dagger.logs_event_pairs(logs) do w, start_idx, finish_idx
        category = logs[w][:core][start_idx].category
        if category in (:scheduler_init, :scheduler_exit, :init_proc, :remove_proc, :add_thunk, :handle_fault, :schedule, :fire, :enqueue, :take, :finish)
            t_start = logs[w][:core][start_idx].timestamp::UInt64
            t_end = logs[w][:core][finish_idx].timestamp::UInt64
            category_str = string(category)
            color = name_to_color(category_str, colors)
            push!(df, (;category=category_str, t_start, t_end, color))
        end
    end
    return df
end
logs_to_df(logs::Dict, ::Val{target}; kwargs...) where target =
        throw(ArgumentError("Invalid target: $(repr(target))"))

# Implementation adapted from:
# https://discourse.julialang.org/t/how-to-make-a-gantt-plot-with-plots-jl/95165/7
"""
    Dagger.render_logs(logs::Dict, ::Val{:plots_gantt};
                       target=:execution,
                       colors, name_to_color, color_by=:fn,
                       kwargs...)

Render a Gantt chart of task execution in `logs` using Plots.

Keyword arguments affect rendering behavior:
- `target`: Which aspect of the logs to render. May be one of `:execution`, `:processor`, or `:scheduler`.
- `colors`: A list of colors to use for rendering.
- `name_to_color`: A function mapping names to colors.
- `color_by`: Whether to color by function name (`:fn`) or processor name (`:proc`).
- `kwargs` are passed to `plot` directly.
"""
function Dagger.render_logs(logs::Dict, ::Val{:plots_gantt};
                            target=:execution,
                            colors=Dagger.Viz.default_colors,
                            name_to_color=Dagger.Viz.name_to_color,
                            color_by=:fn, kwargs...)
    df = logs_to_df(logs, Val{target}(); colors, name_to_color, color_by)
    y_elem = if target == :execution || target == :processor
        :proc_name
    elseif target == :scheduler
        :category
    end
    ylabel = if target == :execution || target == :processor
        "Processor"
    elseif target == :scheduler
        "Category"
    end
    sort!(df, y_elem)

    global_t_start, global_t_end = extrema(logs[1][:core][idx].timestamp for idx in 1:length(logs[1][:core]))

    rect(w, h, x, y) = Shape(x .+ [0,w,w,0], y .+ [0,0,h,h])

    t_start = (df.t_start .- global_t_start) ./ 1e9
    t_end = (df.t_end .- global_t_start) ./ 1e9
    duration = t_end .- t_start
    u = unique(getproperty(df, y_elem))
    dy = Dict(u .=> 1:length(u))
    r = [rect(t1, 1, t2, dy[t3]) for (t1,t2,t3) in zip(duration, t_start, getproperty(df, y_elem))]
    if target == :execution
        labels = permutedims(df.fn_name)
    elseif target == :processor
        labels = permutedims(df.category)
    elseif target == :scheduler
        labels = nothing
    end

    if labels !== nothing
        # Deduplicate labels
        for idx in 1:length(labels)
            if findfirst(other_idx->labels[other_idx]==labels[idx], 1:length(labels)) < idx
                labels[idx] = ""
            end
        end
    end

    return plot(r; color=permutedims(df.color), labels,
                yticks=(1.5:(nrow(df) + 0.5), u),
                xlabel="Time (seconds)", ylabel,
                xlim=(0.0, (global_t_end - global_t_start) / 1e9),
                legendalpha=0, linewidth=0.1,
                kwargs...)
end

end # module PlotsExt
