module JSON3Ext

if isdefined(Base, :get_extension)
    using JSON3
else
    using ..JSON3
end

using Dagger

function logs_to_chrome_trace(logs::Dict)
    execution_logs = Dict{Int,Any}()
    add_unknown_procs_metadata = false
    Dagger.logs_event_pairs(logs) do w, start_idx, finish_idx
        category = logs[w][:core][start_idx].category
        if category == :compute
            tid = logs[w][:id][start_idx].thunk_id # thunk id
            if !haskey(execution_logs, tid)
                execution_logs[tid] = Dict{Symbol,Any}()
            end
            t_start = logs[w][:core][start_idx].timestamp / 1e3 # us
            t_stop = logs[w][:core][finish_idx].timestamp / 1e3 # us
            proc = logs[w][:id][start_idx].processor
            execution_logs[tid][:ts] = t_start
            execution_logs[tid][:dur] = t_stop - t_start
            if proc isa Dagger.ThreadProc
                execution_logs[tid][:pid] = proc.owner
                execution_logs[tid][:tid] = proc.tid # thread id
            else
                @warn "Compute event for [$tid] executed on non-Dagger.ThreadProc processor. Assigning unknown pid and tid"
                execution_logs[tid][:pid] = -1
                execution_logs[tid][:tid] = -1
                add_unknown_procs_metadata = true
            end
        elseif category == :add_thunk
            tid = logs[w][:id][start_idx].thunk_id
            if !haskey(execution_logs, tid)
                execution_logs[tid] = Dict{Symbol,Any}()
            end
            taskname = logs[w][:tasknames][start_idx]
            fname = first(split(taskname, ' '))
            execution_logs[tid][:name] = fname
        end
    end
    events = Vector{Dict{Symbol,Any}}()
    for (_, v) in execution_logs
        v[:ph] = "X"
        v[:cat] = "compute"
        push!(events, v)
    end
    if add_unknown_procs_metadata
        push!(events, Dict(:name => "process_name", :ph => "M", :cat => "__metadata", :pid => -1, :args => Dict(:name => "Unknown")))
        push!(events, Dict(:name => "thread_name", :ph => "M", :cat => "__metadata", :pid => -1, :tid => -1, :args => Dict(:name => "Unknown")))
    end
    return Dict(:traceEvents => events)
end

function Dagger.render_logs(logs::Dict, ::Val{:chrome_trace})
    return JSON3.write(logs_to_chrome_trace(logs))
end

function Dagger.show_logs(io::IO, logs::Dict, ::Val{:chrome_trace})
    JSON3.write(io, logs_to_chrome_trace(logs))
end

end # module JSON3Ext