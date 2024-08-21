# Logging utilities

"""
    enable_logging!(;kwargs...)

Enables logging globally for all workers. Certain core events are always enabled by this call, but additional ones may be specified via `kwargs`.

Extra events:
- `metrics::Bool`: Enables various utilization and allocation metrics
- `timeline::Bool`: Enables raw "timeline" values, which are event-specific; not recommended except for debugging
- `all_task_deps::Bool`: Enables all task dependency-related logging
- `tasknames::Bool`: Enables generating unique task names for each task
- `taskfuncnames::Bool`: Enables reporting of task function names for each task
- `taskdeps::Bool`: Enables reporting of upstream task dependencies (as task IDs) for each task argument
- `taskargs::Bool`: Enables reporting of upstream non-task dependencies (as `objectid` hash) for each task argument
- `taskargmoves::Bool`: Enables reporting of copies of upstream dependencies (as original and copy `objectid` hashes) for each task argument
- `taskresult::Bool`: Enables reporting of task result values (as `objectid` hash)
- `taskuidtotid::Bool`: Enables reporting of task UID-to-TID mappings
- `tasktochunk::Bool`: Enables reporting of DTask-to-Chunk mappings
- `profile::Bool`: Enables profiling of task execution; not currently recommended, as it adds significant overhead
"""
function enable_logging!(;metrics::Bool=true,
                          timeline::Bool=false,
                          all_task_deps::Bool=false,
                          tasknames::Bool=true,
                          taskfuncnames::Bool=false,
                          taskdeps::Bool=true,
                          taskargs::Bool=false,
                          taskargmoves::Bool=false,
                          taskresult::Bool=false,
                          taskuidtotid::Bool=false,
                          tasktochunk::Bool=false,
                          profile::Bool=false)
    ml = TimespanLogging.MultiEventLog()
    ml[:core] = TimespanLogging.Events.CoreMetrics()
    ml[:id] = TimespanLogging.Events.IDMetrics()
    if timeline
        ml[:timeline] = TimespanLogging.Events.TimelineMetrics()
    end
    if all_task_deps
        taskfuncnames = true
        taskdeps = true
        taskargs = true
        taskargmoves = true
        taskresult = true
        taskuidtotid = true
        tasktochunk = true
    end
    if tasknames
        ml[:tasknames] = Dagger.Events.TaskNames()
    end
    if taskfuncnames
        ml[:taskfuncnames] = Dagger.Events.TaskFunctionNames()
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
    if taskresult
        ml[:taskresult] = Dagger.Events.TaskResult()
    end
    if taskuidtotid
        ml[:taskuidtotid] = Dagger.Events.TaskUIDtoTID()
    end
    if tasktochunk
        ml[:tasktochunk] = Dagger.Events.TaskToChunk()
    end
    if profile
        ml[:profile] = DaggerWebDash.ProfileMetrics()
    end
    if metrics
        ml[:wsat] = Dagger.Events.WorkerSaturation()
        ml[:loadavg] = TimespanLogging.Events.CPULoadAverages()
        ml[:bytes] = Dagger.Events.BytesAllocd()
        ml[:mem] = TimespanLogging.Events.MemoryFree()
        ml[:esat] = TimespanLogging.Events.EventSaturation()
        ml[:psat] = Dagger.Events.ProcessorSaturation()
    end
    Dagger.Sch.eager_context().log_sink = ml
    return
end

"""
    disable_logging!()

Disables logging previously enabled with `enable_logging!`.
"""
function disable_logging!()
    Dagger.Sch.eager_context().log_sink = TimespanLogging.NoOpLog()
    return
end

"""
    fetch_logs!() -> Dict{Int, Dict{Symbol, Vector}}

Fetches and returns the currently-accumulated logs for each worker. Each entry
of the outer `Dict` is keyed on worker ID, so `logs[1]` are the logs for worker
`1`.

Consider using `show_logs` or `render_logs` to generate a renderable display of
these logs.
"""
fetch_logs!() = TimespanLogging.get_logs!(Dagger.Sch.eager_context())

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
            event_key = (category, id)
            if kind == :start
                running_events[event_key] = idx
            elseif haskey(running_events, event_key)
                event_start_idx = running_events[event_key]
                f(w, event_start_idx, idx)
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
    Dagger.TimespanLogging.timespan_start(ctx, :data_annotation, (;objectid=objectid_or_chunkid(arg), name), nothing)
    # TODO: Remove redundant log event
    Dagger.TimespanLogging.timespan_finish(ctx, :data_annotation, nothing, nothing)
end
logs_annotate!(arg, name::Union{String,Symbol}) =
    logs_annotate!(Dagger.Sch.eager_context(), arg, name)

objectid_or_chunkid(@nospecialize(x)) =
    LoggedMutableObject(objectid(x), :object)
objectid_or_chunkid(@nospecialize(x::Chunk)) =
    LoggedMutableObject(hash(x), :chunk)
objectid_or_chunkid(@nospecialize(x::DTask)) =
    LoggedMutableObject(x.uid, :task)
