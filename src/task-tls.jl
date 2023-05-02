# In-Thunk Helpers

"""
    thunk_processor()

Get the current processor executing the current thunk.
"""
thunk_processor() = task_local_storage(:_dagger_processor)::Processor

"""
    in_thunk()

Returns `true` if currently in a [`Thunk`](@ref) process, else `false`.
"""
in_thunk() = haskey(task_local_storage(), :_dagger_sch_uid)

"""
    get_tls()

Gets all Dagger TLS variable as a `NamedTuple`.
"""
function get_tls()
    tls = task_local_storage()
    return (;
        sch_uid=tls[:_dagger_sch_uid],
        sch_handle=tls[:_dagger_sch_handle],
        processor=tls[:_dagger_processor],
        task_spec=tls[:_dagger_task_spec],
        metrics=get(Sch.LocalMetricsCache, tls, :_dagger_local_metrics_cache)
    )
end

"""
    set_tls!(tls)

Sets all Dagger TLS variables from the `NamedTuple` `tls`.
"""
function set_tls!(tls)
    task_local_storage(:_dagger_sch_uid, tls.sch_uid)
    task_local_storage(:_dagger_sch_handle, tls.sch_handle)
    task_local_storage(:_dagger_processor, tls.processor)
    task_local_storage(:_dagger_task_spec, tls.task_spec)
    metrics = haskey(tls, :metrics) ? tls.metrics : Sch.LocalMetricsCache()
    task_local_storage(:_dagger_local_metrics_cache, metrics)
end
