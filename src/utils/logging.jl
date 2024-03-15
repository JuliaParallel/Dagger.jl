function enable_logging!(ctx::Context; log_sink=nothing, linfo::Bool=false)
    if log_sink === nothing
        log_sink = TimespanLogging.MultiEventLog()
        log_sink[:core] = TimespanLogging.Events.CoreMetrics()
        log_sink[:id] = TimespanLogging.Events.IDMetrics()
        log_sink[:timeline] = TimespanLogging.Events.TimelineMetrics()
        log_sink[:esat] = TimespanLogging.Events.EventSaturation()
        log_sink[:psat] = Dagger.Events.ProcessorSaturation()
        if linfo
            log_sink[:linfo] = TimespanLogging.Events.LineInfoMetrics()
        end
    end
    ctx.log_sink = log_sink
    return
end
enable_logging!(; kwargs...) = enable_logging!(Sch.eager_context(); kwargs...)
