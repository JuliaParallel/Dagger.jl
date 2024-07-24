# In-Thunk Helpers

"""
    task_processor()

Get the current processor executing the current Dagger task.
"""
task_processor() = task_local_storage(:_dagger_processor)::Processor
@deprecate thunk_processor() task_processor()

"""
    in_task()

Returns `true` if currently executing in a [`DTask`](@ref), else `false`.
"""
in_task() = haskey(task_local_storage(), :_dagger_sch_uid)
@deprecate in_thunk() in_task()

"""
    get_tls()

Gets all Dagger TLS variable as a `NamedTuple`.
"""
get_tls() = (
    sch_uid=task_local_storage(:_dagger_sch_uid),
    sch_handle=task_local_storage(:_dagger_sch_handle),
    processor=task_processor(),
    task_spec=task_local_storage(:_dagger_task_spec),
)

"""
    set_tls!(tls)

Sets all Dagger TLS variables from the `NamedTuple` `tls`.
"""
function set_tls!(tls)
    task_local_storage(:_dagger_sch_uid, tls.sch_uid)
    task_local_storage(:_dagger_sch_handle, tls.sch_handle)
    task_local_storage(:_dagger_processor, tls.processor)
    task_local_storage(:_dagger_task_spec, tls.task_spec)
end
