using DaggerWebDash
import DaggerWebDash: LinePlot, GanttPlot
using TimespanLogging
using Dagger
using Test

@testset "Basics" begin
    ml = TimespanLogging.MultiEventLog()
    ml[:core] = TimespanLogging.Events.CoreMetrics()
    ml[:id] = TimespanLogging.Events.IDMetrics()
    #profile && (ml[:profile] = DaggerWebDash.ProfileMetrics())
    ml[:wsat] = Dagger.Events.WorkerSaturation()
    ml[:loadavg] = TimespanLogging.Events.CPULoadAverages()
    ml[:bytes] = Dagger.Events.BytesAllocd()
    ml[:mem] = TimespanLogging.Events.MemoryFree()
    ml[:esat] = TimespanLogging.Events.EventSaturation()
    ml[:psat] = Dagger.Events.ProcessorSaturation()
    lw = TimespanLogging.Events.LogWindow(5*10^9, :core)
    # FIXME: logs_df = DataFrame([key=>[] for key in keys(ml.consumers)]...)
    # ts = DaggerWebDash.TableStorage(logs_df)
    # push!(lw.creation_handlers, ts)
    d3r = DaggerWebDash.D3Renderer(8080) #; seek_store=ts)
    push!(lw.creation_handlers, d3r)
    push!(lw.deletion_handlers, d3r)
    push!(d3r, GanttPlot(:core, :id, :esat, :psat))
    # TODO: push!(d3r, ProfileViewer(:core, :profile, "Profile Viewer"))
    push!(d3r, LinePlot(:core, :wsat, "Worker Saturation", "Running Tasks"))
    push!(d3r, LinePlot(:core, :loadavg, "CPU Load Average", "Average Running Threads"))
    push!(d3r, LinePlot(:core, :bytes, "Allocated Bytes", "Bytes"))
    push!(d3r, LinePlot(:core, :mem, "Available Memory", "% Free"))
    #push!(d3r, GraphPlot(:core, :id, :timeline, :profile, "DAG"))
    ml.aggregators[:d3r] = d3r
    ml.aggregators[:logwindow] = lw
    ctx = Context(; log_sink=ml)
    Dagger.Sch.EAGER_CONTEXT[] = ctx
    fetch(Dagger.@spawn 1+1)
    run(pipeline(`curl -s localhost:8080/index.html`; stdout=devnull))
end
