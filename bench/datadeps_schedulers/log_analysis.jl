# Utilities for parsing Dagger logs into per-phase timings.
#
# Dagger emits :core events per worker; each entry is a NamedTuple
# (; timestamp, category, kind) where timestamp is `time_ns()` and kind is
# :start or :finish. Datadeps-specific categories are :datadeps_execute,
# :datadeps_copy, :datadeps_copy_skip. Compute is :compute.
#
# Julian asked us to break runtime down by "datadeps scheduling time versus
# runtime" — the harness measures the scheduling phase explicitly (wall-clock
# around `datadeps_schedule_dag_aot!`, in `driver.jl`) and uses these log
# parsers to compute the *actual-execution* portion of the wall-clock for
# verification and breakdown.

using Statistics

const DATADEPS_EXEC_CATEGORY = :datadeps_execute
const DATADEPS_COPY_CATEGORIES = (:datadeps_copy, :datadeps_copy_skip)
const COMPUTE_CATEGORY = :compute
const MOVE_CATEGORY = :move

struct PhaseStats
    n_events::Int
    total_ns::UInt64
end
PhaseStats() = PhaseStats(0, UInt64(0))

# Walk one worker's log and pair :start/:finish events by (category, id),
# invoking `f(category, start_ns, finish_ns)` for each closed pair.
function _foreach_pair(f, core::Vector, ids::Vector)
    open_starts = Dict{Tuple{Symbol, Any}, UInt64}()
    n = length(core)
    @assert length(ids) == n
    for i in 1:n
        entry = core[i]
        cat = entry.category
        kind = entry.kind
        id = ids[i]
        key = (cat, id)
        if kind === :start
            open_starts[key] = UInt64(entry.timestamp)
        elseif kind === :finish
            start_ns = pop!(open_starts, key, UInt64(0))
            start_ns == 0 && continue   # unmatched finish — skip
            finish_ns = UInt64(entry.timestamp)
            finish_ns < start_ns && continue   # corrupt — skip
            f(cat, start_ns, finish_ns)
        end
    end
    return
end

# Accumulate per-category total time and event count across all workers.
function category_totals(logs::Dict)
    totals = Dict{Symbol, PhaseStats}()
    for worker_id in keys(logs)
        worker_log = logs[worker_id]
        core = worker_log[:core]
        ids = worker_log[:id]
        _foreach_pair(core, ids) do cat, s, f
            prev = get(totals, cat, PhaseStats())
            totals[cat] = PhaseStats(prev.n_events + 1, prev.total_ns + (f - s))
        end
    end
    return totals
end

# Wall-clock span of the datadeps execution phase: earliest :start to latest
# :finish across :datadeps_execute, :datadeps_copy, :datadeps_copy_skip events.
# This is the portion of `spawn_datadeps` after AOT scheduling completes; the
# difference (spawn_datadeps wall-clock − this span) approximates AOT-sched
# overhead plus orchestration that does not appear as a logged timespan.
function datadeps_execution_span_ns(logs::Dict)
    earliest = typemax(UInt64)
    latest = UInt64(0)
    saw_any = false
    for worker_id in keys(logs)
        worker_log = logs[worker_id]
        core = worker_log[:core]
        ids = worker_log[:id]
        _foreach_pair(core, ids) do cat, s, f
            if cat === DATADEPS_EXEC_CATEGORY ||
               cat === :datadeps_copy ||
               cat === :datadeps_copy_skip
                saw_any = true
                earliest = min(earliest, s)
                latest = max(latest, f)
            end
        end
    end
    saw_any || return UInt64(0)
    return latest - earliest
end

# Return a NamedTuple summary of (sched_time_ns, exec_time_ns, n_tasks,
# n_copies, compute_total_ns, move_total_ns) from a wall-clock total and the
# parsed logs.
function summarize_phases(total_wallclock_ns::UInt64,
                          sched_phase_ns::UInt64,
                          logs::Dict)
    totals = category_totals(logs)
    exec_span = datadeps_execution_span_ns(logs)
    n_tasks = get(totals, DATADEPS_EXEC_CATEGORY, PhaseStats()).n_events
    copy_stats = PhaseStats()
    for cat in DATADEPS_COPY_CATEGORIES
        c = get(totals, cat, PhaseStats())
        copy_stats = PhaseStats(copy_stats.n_events + c.n_events,
                                copy_stats.total_ns + c.total_ns)
    end
    compute_total = get(totals, COMPUTE_CATEGORY, PhaseStats()).total_ns
    move_total = get(totals, MOVE_CATEGORY, PhaseStats()).total_ns
    return (;
        total_wallclock_ns,
        sched_phase_ns,
        exec_span_ns = exec_span,
        n_tasks,
        n_copies = copy_stats.n_events,
        copy_total_ns = copy_stats.total_ns,
        compute_total_ns = compute_total,
        move_total_ns = move_total,
    )
end
