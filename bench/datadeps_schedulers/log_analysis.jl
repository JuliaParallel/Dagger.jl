# Dagger has no `:datadeps_schedule` timespan, so the driver wall-clocks the
# AOT pass externally; these parsers compute the execution-side breakdown
# from `:datadeps_execute`, `:datadeps_copy`, `:compute`, and `:move` events.

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
            start_ns == 0 && continue   # unmatched finish
            finish_ns = UInt64(entry.timestamp)
            finish_ns < start_ns && continue   # corrupt
            f(cat, start_ns, finish_ns)
        end
    end
    return
end

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

# Earliest :start to latest :finish across datadeps_execute / copy events;
# approximates the post-AOT portion of `spawn_datadeps`.
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
