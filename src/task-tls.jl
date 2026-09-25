# In-Thunk Helpers

mutable struct DTaskTLS
    processor::Processor
    sch_uid::UInt
    sch_handle::Any # FIXME: SchedulerHandle
    task_spec::Any # FIXME: TaskSpec
    cancel_token::CancelToken
    logging_enabled::Bool
    acceleration::Acceleration
    # Scratch metrics cache for the executing thunk, so instrumentation deep
    # inside `execute!` (e.g. `move_toplevel!`) can record into the same
    # cache `do_task` drains. Reached through the TLS rather than a
    # `ScopedValue`/`TaskLocalValue` because `ThreadProc.execute!` runs the
    # thunk on a *sub-task*, which neither of those propagate back out of.
    metrics_cache::Union{MT.MetricsCache, Nothing}
    # Facts about the executing thunk that its metrics record. Held here rather
    # than in `ScopedValue`s: entering a scope for them cost ~939 allocations
    # and 32 KB *per task* (measured on a `fetch(@spawn 1+1)` round-trip),
    # which dominated the whole scheduler path. `set_tls!` already runs once per
    # thunk, so carrying them costs nothing extra.
    metrics_transfer_size::UInt64
    metrics_transfer_time::UInt64
    # CPU time of the thread that ran the thunk's function, as measured by
    # the processor's `execute!` around the call (0 if it measured nothing);
    # see `KernelTimeMetric`. Reset by `set_tls!`.
    metrics_kernel_time::UInt64
end

const DTASK_TLS = TaskLocalValue{Union{DTaskTLS,Nothing}}(()->nothing)

Base.copy(tls::DTaskTLS) =
    DTaskTLS(tls.processor,
             tls.sch_uid,
             tls.sch_handle,
             tls.task_spec,
             tls.cancel_token,
             tls.logging_enabled,
             tls.acceleration,
             tls.metrics_cache,
             tls.metrics_transfer_size,
             tls.metrics_transfer_time,
             tls.metrics_kernel_time)

"""
    get_tls() -> DTaskTLS

Gets all Dagger TLS variable as a `DTaskTLS`.
"""
get_tls() = DTASK_TLS[]::DTaskTLS

"""
    set_tls!(tls::NamedTuple)

Sets all Dagger TLS variables from `tls`, which may be a `DTaskTLS` or a `NamedTuple`.
"""
set_tls!(tls) = set_tls!(tls.processor, tls.sch_uid, tls.sch_handle,
                         tls.task_spec, tls.cancel_token,
                         tls.logging_enabled, tls.acceleration,
                         hasproperty(tls, :metrics_cache) ? tls.metrics_cache : nothing,
                         hasproperty(tls, :metrics_transfer_size) ? tls.metrics_transfer_size : UInt64(0),
                         hasproperty(tls, :metrics_transfer_time) ? tls.metrics_transfer_time : UInt64(0))
# Positional form: hot callers (do_task) avoid building a NamedTuple per task
function set_tls!(processor, sch_uid, sch_handle, task_spec, cancel_token,
                  logging_enabled::Bool, acceleration, metrics_cache=nothing,
                  metrics_transfer_size::UInt64=UInt64(0),
                  metrics_transfer_time::UInt64=UInt64(0))
    # Reuse the existing DTaskTLS in place: pooled scheduler tasks call this
    # once per executed thunk, and nothing retains the old TLS across thunks
    # (`get_tls()` callers copy it if they need to keep it).
    dtls = DTASK_TLS[]
    if dtls isa DTaskTLS
        dtls.processor = processor
        dtls.sch_uid = sch_uid
        dtls.sch_handle = sch_handle
        dtls.task_spec = task_spec
        dtls.cancel_token = cancel_token
        dtls.logging_enabled = logging_enabled
        dtls.acceleration = acceleration
        dtls.metrics_cache = metrics_cache
        dtls.metrics_transfer_size = metrics_transfer_size
        dtls.metrics_transfer_time = metrics_transfer_time
        dtls.metrics_kernel_time = UInt64(0)
    else
        DTASK_TLS[] = DTaskTLS(processor, sch_uid, sch_handle, task_spec,
                               cancel_token, logging_enabled, acceleration,
                               metrics_cache,
                               metrics_transfer_size, metrics_transfer_time,
                               UInt64(0))
    end
    set_task_acceleration!(acceleration)
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
