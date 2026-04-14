# Logs Visualization

To make Dagger's logging facilities useful without having to write custom code,
Dagger has built-in and easily accessible logs visualization capabilities.
Currently, there are two general mechanisms to visualize logs:
`show_logs`/`render_logs`, and `MultiEventLog` consumers.

## Logs visualization with show_logs/render_logs
The former (`show_logs`/`render_logs`) renders a logs `Dict` (acquired from
`fetch_logs!`) either to an `IO` (via `show_logs`) or by returning a renderable
object (via `render_logs`). This system is designed for rendering a single
snapshot of logs into one or a few renderable objects, and is easily extensible
by libraries or directly by the user, using multiple dispatch on
`show_logs(io::IO, logs::Dict, ::Val{mode})` and
`render_logs(logs::Dict, ::Val{mode})`, where `mode` is a unique `Symbol`
identifying the rendering mode to use. From the user's perspective, `show_logs`
and `render_logs` take not a `Val` but a raw `Symbol`, which will be internally
converted to a `Val` for dispatch purposes
(i.e. `render_logs(logs::Dict, :myrenderer)` -> `render_logs(logs, Val{:myrenderer}())`).

Built-in `IO` support exists for:
- `show_logs(io, logs, :graphviz)` to write a Graphviz dot graph of executed tasks and their dependencies (requires `GraphViz.jl` to be loaded)
- `show_logs(io, logs, :chrome_trace)` to write a task execution timeline in the chrome-trace format (view in [perfetto web UI](https://ui.perfetto.dev/) or `about:tracing` in a chrome-based browser) (requires `JSON3.jl` to be loaded)
- `show_logs(io, logs, :summary)` to print a plain-text summary of per-category event statistics (no extra packages required)

Built-in rendering support exists for:
- `render_logs(logs, :graphviz)` to generate a graph diagram of executed tasks and their dependencies (requires `GraphViz.jl` to be loaded)
- `render_logs(logs, :plots_gantt)` to generate a Gantt chart of task execution across all processors (requires `Plots.jl` and `DataFrames.jl` to be loaded)

### Text summaries with show_logs(..., :summary)

The `:summary` mode prints, for every event category (`:compute`, `:move`,
`:schedule`, etc.), the mean/min/max run time across all recorded events of
that category. If enabled via `enable_logging!`, it also reports mean/min/max
of any additional per-event metrics that were collected: `LinuxPerf.jl`
counters (`linuxperf`), GC bytes allocated (`gc_stats`), lock conflicts
(`lock_contend`), and Julia compile time (`compile_time`); see
[Logging: Basics](logging.md) for more on these metrics. For the
`:compute` and `:move` categories, statistics are additionally broken down
per task function name.

```julia
using Dagger

Dagger.enable_logging!(;all_task_deps=true, gc_stats=true, lock_contend=true, compile_time=true)

wait(Dagger.@spawn sum([1, 2, 3]))

logs = Dagger.fetch_logs!()
Dagger.disable_logging!()

# Print directly to stdout
Dagger.show_logs(logs, :summary)

# Or write to any IO, such as a file or IOBuffer
open("summary.txt", "w") do io
    Dagger.show_logs(io, logs, :summary)
end
```

This produces output similar to:

```
=== Dagger Execution Summary ===

[compute] 2 events, 2 unique task(s)
  Run Time          mean=31.1 us, min=7.0 us, max=55.3 us
  Bytes Allocated   mean=1.2 MiB, min=219.7 KiB, max=2.2 MiB
  Compile Time      mean=37.5 us, min=9.8 us, max=65.2 us
  Lock Conflicts    mean=0, min=0, max=0

  [compute :: +] 1 events
    Run Time         mean=55.3 us, min=55.3 us, max=55.3 us
    ...
```

## Continuous visualization with MultiEventLog
The `MultiEventLog` mechanism is designed for continuous rendering of logs as they are generated,
which permits real-time visualization of Dagger's operations. This
logic is utilized in `DaggerWebDash`, which provides a web-based dashboard for
visualizing Dagger's operations as a real-time Gantt chart and set of plots for
various system metrics (CPU usage, memory usage, worker utilization, etc.).

## Visualization with DaggerWebDash

When working with Dagger, especially when working with its scheduler, it can be
helpful to visualize what Dagger is doing internally in near-real-time. To
assist with this, a web dashboard is available in the DaggerWebDash.jl package.
This web dashboard uses a web server running within each Dagger worker, along
with event logging information, to expose details about the scheduler.
Information like worker and processor saturation, memory allocations, profiling
traces, and much more are available in easy-to-interpret plots.

Using the dashboard is relatively simple and straightforward; if you run
Dagger's benchmarking script, it's enabled for you automatically if the
`BENCHMARK_RENDER` environment variable is set to `webdash`. This is the
easiest way to get started with the web dashboard for new users.

For manual usage, the following snippet of code will suffice:

```julia
using Dagger, DaggerWebDash, TimespanLogging

ctx = Context() # or `ctx = Dagger.Sch.eager_context()` for eager API usage
ml = TimespanLogging.MultiEventLog()

## Add some logging events of interest

ml[:core] = TimespanLogging.Events.CoreMetrics()
ml[:id] = TimespanLogging.Events.IDMetrics()
ml[:timeline] = TimespanLogging.Events.TimelineMetrics()
# ...

# (Optional) Enable profile flamegraph generation with ProfileSVG
ml[:profile] = DaggerWebDash.ProfileMetrics()
ctx.profile = true

# Create a LogWindow; necessary for real-time event updates
lw = TimespanLogging.Events.LogWindow(20*10^9, :core)
ml.aggregators[:logwindow] = lw

# Create the D3Renderer server on port 8080
d3r = DaggerWebDash.D3Renderer(8080)

## Add some plots! Rendered top-down in order

# Show an overview of all generated events as a Gantt chart
push!(d3r, DaggerWebDash.GanttPlot(:core, :id, :esat, :psat; title="Overview"))

# Show various numerical events as line plots over time
push!(d3r, DaggerWebDash.LinePlot(:core, :wsat, "Worker Saturation", "Running Tasks"))
push!(d3r, DaggerWebDash.LinePlot(:core, :loadavg, "CPU Load Average", "Average Running Threads"))
push!(d3r, DaggerWebDash.LinePlot(:core, :bytes, "Allocated Bytes", "Bytes"))
push!(d3r, DaggerWebDash.LinePlot(:core, :mem, "Available Memory", "% Free"))

# Show a graph rendering of compute tasks and data movement between them
# Note: Profile events are ignored if absent from the log
push!(d3r, DaggerWebDash.GraphPlot(:core, :id, :timeline, :profile, "DAG"))

# TODO: Not yet functional
#push!(d3r, DaggerWebDash.ProfileViewer(:core, :profile, "Profile Viewer"))

# Add the D3Renderer as a consumer of special events generated by LogWindow
push!(lw.creation_handlers, d3r)
push!(lw.deletion_handlers, d3r)

# D3Renderer is also an aggregator
ml.aggregators[:d3r] = d3r

ctx.log_sink = ml
# ... use `ctx`
```

Once the server has started, you can browse to `http://localhost:8080/` (if
running on your local machine) to view the plots in real time. The dashboard
also provides options at the top of the page to control the drawing speed,
enable and disable reading updates from the server (disabling freezes the
display at the current instant), and a selector for which worker to look at. If
the connection to the server is lost for any reason, the dashboard will attempt
to reconnect at 5 second intervals. The dashboard can usually survive restarts
of the server perfectly well, although refreshing the page is usually a good
idea. Informational messages are also logged to the browser console for
debugging.
