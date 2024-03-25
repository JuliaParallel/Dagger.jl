using Profile

# Logging utilities

function enable_logging!(;metrics::Bool=true,
                          timeline::Bool=false,
                          tasknames::Bool=true,
                          taskdeps::Bool=true,
                          taskargs::Bool=false,
                          taskargmoves::Bool=false,
                          profile::Bool=false,
                          profile_continuous::Bool=isdefined(Profile, :is_continuous))
    ctx = Dagger.Sch.eager_context()
    ml = TimespanLogging.MultiEventLog()
    ml[:core] = TimespanLogging.Events.CoreMetrics()
    ml[:id] = TimespanLogging.Events.IDMetrics()
    if timeline
        ml[:timeline] = TimespanLogging.Events.TimelineMetrics()
    end
    if tasknames
        ml[:tasknames] = Dagger.Events.TaskNames()
    end
    if taskdeps
        ml[:taskdeps] = Dagger.Events.TaskDependencies()
    end
    if taskargs
        ml[:taskargs] = Dagger.Events.TaskArguments()
    end
    if taskargmoves
        ml[:taskargmoves] = Dagger.Events.TaskArgumentMoves()
    end
    if profile
        ml[:profile] = Dagger.Events.ProfileMetrics()
        if isdefined(Profile, :is_continuous)
            n, delay = Profile.init()
            Profile.init(;n, delay, continuous=profile_continuous)
        elseif profile_continuous
            @warn "Profiler doesn't support continuous profiling\nFalling back to non-continuous profiling" maxlog=1
        end
    end
    ctx.profile = profile
    if metrics
        ml[:wsat] = Dagger.Events.WorkerSaturation()
        ml[:loadavg] = TimespanLogging.Events.CPULoadAverages()
        ml[:bytes] = Dagger.Events.BytesAllocd()
        ml[:mem] = TimespanLogging.Events.MemoryFree()
        ml[:esat] = TimespanLogging.Events.EventSaturation()
        ml[:psat] = Dagger.Events.ProcessorSaturation()
    end
    ctx.log_sink = ml
end
function disable_logging!()
    Dagger.Sch.eager_context().log_sink = TimespanLogging.NoOpLog()
end
function fetch_logs!()
    return TimespanLogging.get_logs!(Dagger.Sch.eager_context())
end

function logs_event_pairs(f, logs::Dict)
    running_events = Dict{Tuple,Int}()
    for w in keys(logs)
        for idx in 1:length(logs[w][:core])
            kind = logs[w][:core][idx].kind
            category = logs[w][:core][idx].category
            id = logs[w][:id][idx]
            if id === nothing
                continue
            end
            id::NamedTuple
            if haskey(id, :thunk_id)
                event_key = (category, id)
                if kind == :start
                    running_events[event_key] = idx
                else
                    event_start_idx = running_events[event_key]
                    f(w, event_start_idx, idx)
                end
            end
        end
    end
end

"""
Associates an argument `arg` with `name` in the logs, which logs renderers may
utilize for display purposes.
"""
function logs_annotate!(ctx::Context, arg, name::Union{String,Symbol})
    ismutable(arg) || throw(ArgumentError("Argument must be mutable to be annotated"))
    Dagger.TimespanLogging.timespan_start(ctx, :data_annotation, (;objectid=objectid(arg), name), nothing)
    # TODO: Remove redundant log event
    Dagger.TimespanLogging.timespan_finish(ctx, :data_annotation, nothing, nothing)
end
logs_annotate!(arg, name::Union{String,Symbol}) =
    logs_annotate!(Dagger.Sch.eager_context(), arg, name)
