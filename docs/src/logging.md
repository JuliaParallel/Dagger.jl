# Logging

Dagger provides mechanisms to log and visualize scheduler events. This can be useful for debugging and performance analysis.

## Basic Logging Functions

The primary functions for controlling logging are:

- `Dagger.enable_logging!`: Enables logging. This function uses the `MultiEventLog` by default, which is flexible and performant. You can customize its behavior with keyword arguments.
- `Dagger.disable_logging!`: Disables logging.
- `Dagger.fetch_logs!`: Fetches the logs from all workers. This returns a `Dict` where keys are worker IDs and values are the logs.

## Example Usage

```julia
using Dagger

# Enable logging
Dagger.enable_logging!()

# Run some Dagger computations
wait(Dagger.@spawn sum([1, 2, 3]))

# Fetch logs
logs = Dagger.fetch_logs!()

# Disable logging
Dagger.disable_logging!()

# You can now inspect the `logs` Dict or use visualization tools
# like `show_logs` and `render_logs` (see [Logging: Visualization](@ref logging-visualization.md)).
```

For more advanced logging configurations, such as custom log sinks and consumers, see [Logging: Advanced](@ref).

## Performance Metrics

`enable_logging!` accepts a variety of keyword arguments to enable additional
per-event metrics beyond the always-on core events; see the `enable_logging!`
docstring for the full list. A few notable ones for performance analysis are:

- `gc_stats::Bool`: Records the number of bytes allocated by the GC during each event.
- `lock_contend::Bool`: Records the number of lock conflicts (contended lock acquisitions) that occurred during each event.
- `compile_time::Bool`: Records how much time was spent in Julia's compiler during each event.
- `linuxperf::String`: Records hardware/software performance counters (via [LinuxPerf.jl](https://github.com/JuliaPerf/LinuxPerf.jl)) for each event, on Linux systems.

The `linuxperf` argument takes a perf-style event/group specification string
(the same format accepted by `LinuxPerf.parse_groups`), such as
`"cpu-clock, page-faults, (cpu-cycles,instructions)"`. `LinuxPerf.jl` must be
loaded (`using LinuxPerf`) before passing a non-empty `linuxperf` argument,
otherwise an error will be thrown:

```julia
using Dagger, LinuxPerf

Dagger.enable_logging!(;linuxperf="cpu-clock, page-faults, cache-misses",
                        gc_stats=true, lock_contend=true, compile_time=true)

wait(Dagger.@spawn sum([1, 2, 3]))

logs = Dagger.fetch_logs!()
Dagger.disable_logging!()

# logs[1][:linuxperf] contains a `Dict{String,Int64}` of counter deltas for
# each finished event; logs[1][:gc_stats], logs[1][:lock_contend], and
# logs[1][:compile_time] contain per-event scalar values.
```

These metrics are also picked up automatically by the `:summary` text
visualizer; see [Logging: Visualization](logging-visualization.md) for details.
