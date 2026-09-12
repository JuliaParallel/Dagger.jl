@inline function _gc_snapshot()
    return CAPTURE_GC[] ? Base.gc_num() : GC_PLACEHOLDER[]
end

@inline function _emit_legacy(phase::UInt8, category::Symbol, @nospecialize(id), @nospecialize(tl))
    st = thread_state()
    ev = LegacyEvent(phase, time_ns(), category, id, tl, _gc_snapshot())
    push_event!(st.legacy, ev)
    return nothing
end

@inline function _emit(::Type{C}, phase::UInt8, id, data) where C <: LogCategory
    st = thread_state()
    buf = typed_buffer(C, st)
    ev = EventRecord{C, id_type(C), data_type(C)}(
        phase, time_ns(), adapt_id(C, id), adapt_data(C, data))
    push_event!(buf, ev)
    return nothing
end

"""
    @logstart ctx Category id data

Record a start event when `ctx`'s log sink is not `NoOpLog` (and the
category bit is enabled, if a filter is installed). `id` and `data` are
only evaluated when logging is on.
"""
macro logstart(ctx, cat, id, data)
    quote
        if $(TimespanLogging).logging_enabled($(esc(ctx)), $(esc(cat)))
            $(TimespanLogging)._emit($(esc(cat)), 0x00, $(esc(id)), $(esc(data)))
        end
        nothing
    end
end

"""
    @logfinish ctx Category id data
"""
macro logfinish(ctx, cat, id, data)
    quote
        if $(TimespanLogging).logging_enabled($(esc(ctx)), $(esc(cat)))
            $(TimespanLogging)._emit($(esc(cat)), 0x01, $(esc(id)), $(esc(data)))
        end
        nothing
    end
end

# Category-only form (no ctx): gated solely by `enable!` bits.
macro logstart(cat, id, data)
    quote
        if $(TimespanLogging).category_enabled($(esc(cat)))
            $(TimespanLogging)._emit($(esc(cat)), 0x00, $(esc(id)), $(esc(data)))
        end
        nothing
    end
end

macro logfinish(cat, id, data)
    quote
        if $(TimespanLogging).category_enabled($(esc(cat)))
            $(TimespanLogging)._emit($(esc(cat)), 0x01, $(esc(id)), $(esc(data)))
        end
        nothing
    end
end

"""
    timespan_start(ctx, category::Symbol, id, tl)

Legacy emit path. When the sink is `NoOpLog`, this is a no-op (and
`@maybelog` already skipped constructing `id`/`tl`). Otherwise the event
is appended to the calling thread's legacy chunk list — no process-wide
lock, no consumer dispatch, no `ProfilerResult` allocation.
"""
function timespan_start(ctx, category::Symbol, @nospecialize(id), @nospecialize(tl))
    sink = log_sink(ctx)
    isa(sink, NoOpLog) && return
    _maybe_start_profile(ctx, category, id, tl)
    _emit_legacy(0x00, category, id, tl)
    return nothing
end

"""
    timespan_finish(ctx, category::Symbol, id, tl; tasks=nothing)

Legacy finish path. Profiling (`profile(ctx, ...)`) still uses the old
`Profile.fetch` machinery; the common path only stores a typed-enough
legacy record.
"""
function timespan_finish(ctx, category::Symbol, @nospecialize(id), @nospecialize(tl);
                         tasks=nothing)
    sink = log_sink(ctx)
    isa(sink, NoOpLog) && return
    if profile(ctx, category, id, tl)
        _timespan_finish_profile(sink, category, id, tl, tasks)
        return nothing
    end
    _emit_legacy(0x01, category, id, tl)
    return nothing
end
