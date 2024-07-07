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

_name_to_color(name::AbstractString, colors) =
    colors[mod1(hash(name), length(colors))]
_name_to_color(name::AbstractString, ::Nothing) = "black"
_default_colors = ["red", "orange", "green", "blue", "purple", "pink", "silver"]

function logs_to_df(logs::Dict; colors=_default_colors, name_to_color=_name_to_color, color_by=:fn)
    # Generate function names
    fn_names = Dict{Int, String}()
    for w in keys(logs)
        for idx in 1:length(logs[w][:core])
            category = logs[w][:core][idx].category::Symbol
            kind = logs[w][:core][idx].kind::Symbol
            if category == :add_thunk && kind == :start
                tid = logs[w][:id][idx].thunk_id::Int
                if haskey(logs[w], :tasknames)
                    fn_names[tid] = first(split(logs[w][:tasknames][idx]::String, ' '))
                else
                    @warn "Task names missing from logs"
                    fn_names[tid] = "unknown"
                end
            end
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

# Implementation by Przemyslaw Szufel
function Dagger.render_logs(logs::Dict, ::Val{:plots_gantt_ps})
    df = logs_to_df(logs)

    proc_names = sort!(unique(df.proc_name))
    proc_idx = Dict{String,Int}()
    for name in proc_names
        proc_idx[name] = findfirst(==(name), proc_names)
    end
    proc_idxs = map(name->proc_idx[name], proc_names)
    tvals = zeros(UInt64, length(proc_names))
    plt = bar(orientation=:h, yticks=(1:length(proc_names), proc_names), linewidth=0,yflip=true,color=:green,legend=nothing)
    xlabel!(plt, "Time in seconds")
    dfc = deepcopy(df)
    while nrow(dfc) > 0
        rowslast = DataFrame([g[findmax(g.t_end)[2],:] for g in groupby(dfc, :proc_name)])
        tvals .= .0
        for i in 1:nrow(rowslast)
            tvals[proc_idx[rowslast.proc_name[i]]] = rowslast.t_end[i]
        end
        #setindex!.(Ref(tvals), rowslast.t_end, getindex.(proc_idx, rowslast.proc_name))
        bar!(plt, tvals[proc_idxs], orientation=:h, linewidth=0.5,yflip=true,color=:green)
        tvals .= .0
        for i in 1:nrow(rowslast)
            tvals[proc_idx[rowslast.proc_name[i]]] = rowslast.t_start[i]
        end
        #setindex!.(Ref(tvals), rowslast.t_start, proc_idx[rowslast.proc_name])
        bar!(plt, tvals[proc_idxs], orientation=:h, linewidth=0.5,linecolor=:white,yflip=true,color=:white)
        annotate!.(Ref(plt),(rowslast.t_start .+ rowslast.t_end) ./ 2,  findfirst.( .==(rowslast.proc_name), Ref(proc_names)),  text.(string.(rowslast.tid),9,rotation=90 ))
        dfc = dfc[ .! (dfc.tid .∈ Ref(rowslast.tid) ), : ]
    end
    # FIXME: theoretic_optimal = simulate_polling(df)[1] + minimum(df.t_start)
    theoretic_optimal = minimum(df.t_start)
    plot!(plt, [theoretic_optimal,theoretic_optimal], [0, length(proc_names)+1],width=3,color=:black,style=:dot)
    return plt
end

# Implementation adapted from:
# https://discourse.julialang.org/t/how-to-make-a-gantt-plot-with-plots-jl/95165/7
"""
    Dagger.render_logs(logs::Dict, ::Val{:plots_gantt}; kwargs...)

Render a Gantt chart of task execution in `logs` using Plots. `kwargs` are passed to `plot` directly.
"""
function Dagger.render_logs(logs::Dict, ::Val{:plots_gantt};
                            colors=_default_colors, name_to_color=_name_to_color,
                            color_by=:fn, kwargs...)
    df = logs_to_df(logs; colors, name_to_color, color_by)

    rect(w, h, x, y) = Shape(x .+ [0,w,w,0], y .+ [0,0,h,h])

    t_init = minimum(df.t_start)
    t_start = (df.t_start .- t_init) ./ 1e9
    t_end = (df.t_end .- t_init) ./ 1e9
    duration = t_end .- t_start
    u = unique(df.proc_name)
    dy = Dict(u .=> 1:length(u))
    r = [rect(t1, 1, t2, dy[t3]) for (t1,t2,t3) in zip(duration, t_start, df.proc_name)]
    labels = permutedims(df.fn_name)
    # Deduplicate labels
    for idx in 1:length(labels)
        if findfirst(other_idx->labels[other_idx]==labels[idx], 1:length(labels)) < idx
            labels[idx] = ""
        end
    end

    return plot(r; color=permutedims(df.color), labels,
                yticks=(1.5:(nrow(df) + 0.5), u),
                xlabel="Time (seconds)", ylabel="Processor",
                kwargs...)
end

end # module PlotsExt
