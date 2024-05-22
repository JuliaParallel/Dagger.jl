# In-Thunk Helpers

struct DTaskTLS
    processor::Processor
    sch_uid::UInt
    sch_handle::Any # FIXME: SchedulerHandle
    task_spec::Vector{Any} # FIXME: TaskSpec
end

const DTASK_TLS = TaskLocalValue{Union{DTaskTLS,Nothing}}(()->nothing)

"""
    get_tls() -> DTaskTLS

Gets all Dagger TLS variable as a `DTaskTLS`.
"""
get_tls() = DTASK_TLS[]::DTaskTLS

"""
    set_tls!(tls)

Sets all Dagger TLS variables from `tls`, which may be a `DTaskTLS` or a `NamedTuple`.
"""
function set_tls!(tls)
    DTASK_TLS[] = DTaskTLS(tls.processor, tls.sch_uid, tls.sch_handle, tls.task_spec)
end

"""
    in_task() -> Bool

Returns `true` if currently executing in a [`DTask`](@ref), else `false`.
"""
in_task() = DTASK_TLS[] !== nothing
@deprecate in_thunk() in_task()

"""
    task_processor() -> Processor

Get the current processor executing the current [`DTask`](@ref).
"""
task_processor() = get_tls().processor
@deprecate thunk_processor() task_processor()
