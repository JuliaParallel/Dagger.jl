# In-Thunk Helpers

"""
    thunk_processor() -> Processor

Get the current processor executing the current thunk.
"""
thunk_processor() = task_local_storage(:_dagger_processor)::Processor

"""
    in_thunk() -> Bool

Returns `true` if currently in a [`Thunk`](@ref) process, else `false`.
"""
in_thunk() = haskey(task_local_storage(), :_dagger_sch_uid)

"""
    thunk_logging_enabled() -> Bool

Returns `true` if logging is enabled for the current thunk, else `false`.
"""
thunk_logging_enabled() = task_local_storage(:_dagger_logging_enabled)

"""
    get_tls() -> NamedTuple

Gets all Dagger TLS variable as a `NamedTuple`.
"""
get_tls() = (
    sch_uid=task_local_storage(:_dagger_sch_uid),
    sch_handle=task_local_storage(:_dagger_sch_handle),
    processor=thunk_processor(),
    task_spec=task_local_storage(:_dagger_task_spec),
    logging_enabled=thunk_logging_enabled(),
)

"""
    set_tls!(tls::NamedTuple)

Sets all Dagger TLS variables from the `NamedTuple` `tls`.
"""
function set_tls!(tls)
    task_local_storage(:_dagger_sch_uid, tls.sch_uid)
    task_local_storage(:_dagger_sch_handle, tls.sch_handle)
    task_local_storage(:_dagger_processor, tls.processor)
    task_local_storage(:_dagger_task_spec, tls.task_spec)
    task_local_storage(:_dagger_logging_enabled, tls.logging_enabled)
end
