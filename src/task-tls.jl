# In-Thunk Helpers

mutable struct DTaskTLS
    processor::Processor
    sch_uid::UInt
    sch_handle::Any # FIXME: SchedulerHandle
    task_spec::Vector{Any} # FIXME: TaskSpec
    cancel_token::CancelToken
end

const DTASK_TLS = TaskLocalValue{Union{DTaskTLS,Nothing}}(()->nothing)

Base.copy(tls::DTaskTLS) = DTaskTLS(tls.processor, tls.sch_uid, tls.sch_handle, tls.task_spec, tls.cancel_token)

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
    DTASK_TLS[] = DTaskTLS(tls.processor, tls.sch_uid, tls.sch_handle, tls.task_spec, tls.cancel_token)
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
    task_cancelled() -> Bool

Returns `true` if the current [`DTask`](@ref) has been cancelled, else `false`.
"""
task_cancelled() = get_tls().cancel_token.cancelled[]

"""
    task_may_cancel!()

Throws an `InterruptException` if the current [`DTask`](@ref) has been cancelled.
"""
function task_may_cancel!()
    if task_cancelled()
        throw(InterruptException())
    end
end
