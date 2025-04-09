# In-Thunk Helpers

mutable struct DTaskTLS
    processor::Processor
    sch_uid::UInt
    sch_handle::Any # FIXME: SchedulerHandle
    task_spec::Any # FIXME: TaskSpec
    cancel_token::CancelToken
    logging_enabled::Bool
end

const DTASK_TLS = TaskLocalValue{Union{DTaskTLS,Nothing}}(()->nothing)

Base.copy(tls::DTaskTLS) =
    DTaskTLS(tls.processor,
             tls.sch_uid,
             tls.sch_handle,
             tls.task_spec,
             tls.cancel_token,
             tls.logging_enabled)

"""
    get_tls() -> DTaskTLS

Gets all Dagger TLS variable as a `DTaskTLS`.
"""
get_tls() = DTASK_TLS[]::DTaskTLS

"""
    set_tls!(tls::NamedTuple)

Sets all Dagger TLS variables from `tls`, which may be a `DTaskTLS` or a `NamedTuple`.
"""
function set_tls!(tls)
    DTASK_TLS[] = DTaskTLS(tls.processor,
                           tls.sch_uid,
                           tls.sch_handle,
                           tls.task_spec,
                           tls.cancel_token,
                           tls.logging_enabled)
end

"""
    in_task() -> Bool

Returns `true` if currently executing in a [`DTask`](@ref), else `false`.
"""
in_task() = DTASK_TLS[] !== nothing
@deprecate(in_thunk(), in_task())

"""
    task_id() -> Int

Returns the ID of the current [`DTask`](@ref).
"""
task_id() = get_tls().sch_handle.thunk_id.id

"""
    task_processor() -> Processor

Get the current processor executing the current [`DTask`](@ref).
"""
task_processor() = get_tls().processor
@deprecate(thunk_processor(), task_processor())

"""
    task_cancelled(; must_force::Bool=false) -> Bool

Returns `true` if the current [`DTask`](@ref) has been cancelled, else `false`.
If `must_force=true`, then only return `true` if the cancellation was forced.
"""
task_cancelled(; must_force::Bool=false) =
    is_cancelled(get_tls().cancel_token; must_force)

"""
    task_may_cancel!(; must_force::Bool=false)

Throws an `InterruptException` if the current [`DTask`](@ref) has been cancelled.
If `must_force=true`, then only throw if the cancellation was forced.
"""
function task_may_cancel!(;must_force::Bool=false)
    if task_cancelled(;must_force)
        throw(InterruptException())
    end
end

"""
    task_cancel!(; graceful::Bool=true)

Cancels the current [`DTask`](@ref). If `graceful=true`, then the task will be
cancelled gracefully, otherwise it will be forced.
"""
task_cancel!(; graceful::Bool=true) = cancel!(get_tls().cancel_token; graceful)

"""
    task_logging_enabled() -> Bool

Returns `true` if logging is enabled for the current [`DTask`](@ref), else `false`.
"""
task_logging_enabled() = get_tls().logging_enabled
