module JSON3Ext

if isdefined(Base, :get_extension)
    using JSON3
else
    using ..JSON3
end

using Dagger

function logs_to_chrome_trace(logs::Dict)
    execution_logs = Dict{Int,Any}()
    tid_to_uid = Dict{Int,UInt}()
    uid_to_name = Dict{UInt,String}()
    add_unknown_procs_metadata = false
    for w in keys(logs)
        for idx in 1:length(logs[w][:core])
            category = logs[w][:core][idx].category
            kind = logs[w][:core][idx].kind
            if category == :compute
                tid = logs[w][:id][idx].thunk_id # thunk id
                if !haskey(execution_logs, tid)
                    execution_logs[tid] = Dict{Symbol,Any}()
                end
                if kind == :start
                    #start time
                    execution_logs[tid][:ts] = logs[w][:core][idx].timestamp / 1e3 # us
                    # proc
                    proc = logs[w][:id][idx].processor
                    if proc isa Dagger.ThreadProc
                        execution_logs[tid][:pid] = proc.owner
                        execution_logs[tid][:tid] = proc.tid # thread id
                    else
                        @warn "Compute event for [$tid] executed on non-Dagger.ThreadProc processor. Assigning unknown pid and tid"
                        execution_logs[tid][:pid] = -1
                        execution_logs[tid][:tid] = -1
                        add_unknown_procs_metadata = true
                    end
                else
                    # stop time (temporary as duration is used in the trace format)
                    execution_logs[tid][:_stop] = logs[w][:core][idx].timestamp / 1e3 # us
                end
            elseif category == :add_thunk && kind == :start
                tid = logs[w][:id][idx].thunk_id
                if !haskey(execution_logs, tid)
                    execution_logs[tid] = Dict{Symbol,Any}()
                end
                # auto name
                execution_logs[tid][:name] = logs[w][:taskfuncnames][idx]
                # uid-tid mapping for user task name
                if haskey(logs[w], :taskuidtotid)
                    uid_tid = logs[w][:taskuidtotid][idx]
                    if uid_tid !== nothing
                        uid, tid = uid_tid::Pair{UInt,Int}
                        tid_to_uid[tid] = uid
                    end
                end
            elseif category == :data_annotation && kind == :start
                # user task name
                id = logs[w][:id][idx]::NamedTuple
                name = String(id.name)
                obj = id.objectid::Dagger.LoggedMutableObject
                objid = obj.objid
                uid_to_name[objid] = name
            end
        end
    end
    events = Vector{Dict{Symbol,Any}}()
    for (tid, v) in execution_logs
        if !haskey(v, :ts) || !haskey(v, :_stop)
            continue
        end
        v[:dur] = v[:_stop] - v[:ts]
        delete!(v, :_stop)

        v[:ph] = "X"
        v[:cat] = "compute"

        # replace auto name with user task name if present
        if haskey(tid_to_uid, tid)
            uid = tid_to_uid[tid]
            if haskey(uid_to_name, uid)
                v[:name] = uid_to_name[uid]
            end
        end
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
