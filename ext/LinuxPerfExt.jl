module LinuxPerfExt

import Dagger
import LinuxPerf

import TimespanLogging
import TimespanLogging: Event, init_similar

const DEFAULT_METRIC_SPEC =
    "cpu-clock, page-faults, context-switches, cpu-migrations, minor-faults, major-faults"

mutable struct LinuxPerfMetrics
    # Perf-style grouping string, e.g. "(cpu-cycles,instructions), page-faults"
    metric_spec::String
    # Lazily constructed bench; metric names are read from bench.groups[*].event_types
    bench::Union{Nothing, LinuxPerf.PerfBench}
    # One atomic refcount per EventGroup: enable on 0->1, disable on 1->0
    refcounts::Vector{Threads.Atomic{Int}}
    # Baseline counter snapshots per active event, keyed by (category, id)
    active_baselines::Dict{Any, Vector{UInt64}}
end

LinuxPerfMetrics(spec::String) =
    LinuxPerfMetrics(spec, nothing, Threads.Atomic{Int}[], Dict{Any,Vector{UInt64}}())
LinuxPerfMetrics() = LinuxPerfMetrics(DEFAULT_METRIC_SPEC)
init_similar(m::LinuxPerfMetrics) = LinuxPerfMetrics(m.metric_spec)

# Build the PerfBench from the metric spec string on first use.
function _ensure_bench!(metrics::LinuxPerfMetrics)
    if metrics.bench === nothing
        groups_spec = LinuxPerf.parse_groups(metrics.metric_spec)
        event_groups = LinuxPerf.EventGroup[LinuxPerf.EventGroup(g) for g in groups_spec]
        metrics.bench = LinuxPerf.PerfBench(0, event_groups)
        metrics.refcounts = [Threads.Atomic{Int}(0) for _ in event_groups]
    end
    return metrics.bench
end

# Read raw counter values from every group, ordered group-by-group.
# Read format per group: [nr, time_enabled, time_running, value_1, ..., value_nr]
function _read_raw_counters(bench::LinuxPerf.PerfBench)
    vals = UInt64[]
    for g in bench.groups
        g.leader_fd == -1 && continue
        buf = Vector{UInt64}(undef, length(g) + 3)
        read!(g.leader_io, buf)
        for i in 1:length(g)
            push!(vals, buf[3 + i])
        end
    end
    return vals
end

# Subtract baseline from current snapshot; build name->delta dict using
# metric names sourced from the bench's EventGroup event_types.
function _delta_to_dict(bench::LinuxPerf.PerfBench, baseline::Vector{UInt64})
    current = _read_raw_counters(bench)
    result  = Dict{String, Int64}()
    idx = 1
    for g in bench.groups
        g.leader_fd == -1 && continue
        for et in g.event_types
            name = get(LinuxPerf.EVENT_TO_NAME, et, string(et))
            result[name] = Int64(current[idx] - baseline[idx])
            idx += 1
        end
    end
    return result
end

function (metrics::LinuxPerfMetrics)(ev::Event{:start})
    bench = _ensure_bench!(metrics)
    isempty(bench.groups) && return nothing

    # Enable each group on the 0->1 transition only
    for (group, rc) in zip(bench.groups, metrics.refcounts)
        old = Threads.atomic_add!(rc, 1)
        if old == 0
            LinuxPerf.reset!(group)
            LinuxPerf.enable!(group)
        end
    end

    metrics.active_baselines[(ev.category, ev.id)] = _read_raw_counters(bench)
    nothing
end

function (metrics::LinuxPerfMetrics)(ev::Event{:finish})
    bench = metrics.bench
    bench === nothing && return nothing

    baseline = pop!(metrics.active_baselines, (ev.category, ev.id), nothing)
    baseline === nothing && return nothing

    # Read delta before potentially disabling any group
    result = _delta_to_dict(bench, baseline)

    # Disable each group on the 1->0 transition only
    for (group, rc) in zip(bench.groups, metrics.refcounts)
        old = Threads.atomic_sub!(rc, 1)
        if old == 1
            LinuxPerf.disable!(group)
        end
    end

    return result
end

end # module LinuxPerfExt
