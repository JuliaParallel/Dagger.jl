getname(p::Dagger.OSProc) = "OS Process on worker $(p.pid)"
getname(p::Dagger.ThreadProc) = "Thread $(p.tid) on worker $(p.owner)"
getname(p) = sprint(Base.show, p)

function proclt(p1::T, p2::R) where {T,R}
    if p1.owner != p2.owner
        return p1.owner < p2.owner
    else
        return repr(T) < repr(R)
    end
end
function proclt(p1::T, p2::T) where {T}
    if p1.owner != p2.owner
        return p1.owner < p2.owner
    else
        for field in fieldnames(T)
            f1 = getfield(p1, field)
            f2 = getfield(p2, field)
            if f1 != f2
                return f1 < f2
            end
        end
    end
    false
end
proclt(p1::Dagger.OSProc, p2::Dagger.OSProc) = p1.pid < p2.pid
proclt(p1::Dagger.OSProc, p2) = p1.pid < p2.owner
proclt(p1, p2::Dagger.OSProc) = p1.owner < p2.pid

function update_window_logs!(window_logs, logs; root_time, window_start)
    if !isempty(logs)
        for id in keys(logs)
            append!(window_logs, map(x->(x,), filter(x->x.category==:compute||x.category==:scheduler_init, logs[id])))
        end
    end
    for idx in length(window_logs):-1:1
        log = window_logs[idx]
        if length(log) == 2
            # Clear out finished events older than window start
            log_finish_s = (log[2].timestamp-root_time)/(1000^3)
            if log_finish_s < window_start
                @debug "Gantt: Deleted event"
                deleteat!(window_logs, idx)
            end
        elseif log[1] isa Dagger.Event{:finish}
            # Pair finish events with start events
            sidx = findfirst(x->length(x) == 1 &&
                                x[1] isa Dagger.Event{:start} &&
                                x[1].id==log[1].id, window_logs)
            if sidx === nothing
                @debug "Gantt: Removed unpaired finish"
                deleteat!(window_logs, idx)
                continue
            end
            window_logs[sidx] = (window_logs[sidx][1], log[1])
            @debug "Gantt: Paired event"
            deleteat!(window_logs, idx)
        end
    end
end
function logs_to_stackframes(logs)
    data = UInt64[]
    lidict = Dict{UInt64, Vector{Base.StackTraces.StackFrame}}()
    for log in filter(x->length(x)==2, logs)
        append!(data, log[2].profiler_samples.samples)
        merge!(lidict, log[2].profiler_samples.lineinfo)
    end
    return data, lidict
end

const continue_rendering = Ref{Bool}(true)
const render_results = Channel()
