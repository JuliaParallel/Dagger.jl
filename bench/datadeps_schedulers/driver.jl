using Dagger
using Printf
using LinearAlgebra

# MILP scheduler wires in only when both JuMP and HiGHS are installed.
const HAS_MILP = Base.find_package("JuMP") !== nothing &&
                 Base.find_package("HiGHS") !== nothing
if HAS_MILP
    @eval using JuMP
    @eval using HiGHS
end

include("workloads.jl")
include("log_analysis.jl")
include("summarize.jl")

mutable struct TimedScheduler{S<:Dagger.DataDepsScheduler} <: Dagger.DataDepsScheduler
    inner::S
    # Atomic accumulator so under Dagger's default hierarchical scheduling —
    # which shards the scheduler via `similar` and runs partitions in
    # parallel — every shard's AOT time is summed into a single counter
    # without races. Under single-instance scheduling (no partitioning),
    # exactly one `datadeps_schedule_dag_aot!` fires and the atomic holds
    # the sole scheduling time. Under hierarchical, the value is the sum
    # of scheduler CPU time across partition shards, which is the honest
    # "total scheduler work per DAG" metric for Fig 3 / sched_phase_ms.
    last_aot_ns::Threads.Atomic{UInt64}
    # Primary ctor: fresh atomic per top-level wrapper.
    TimedScheduler(inner::S) where {S<:Dagger.DataDepsScheduler} =
        new{S}(inner, Threads.Atomic{UInt64}(0))
    # Shard ctor: reuse the parent's atomic so shard-writes accumulate
    # into the parent-visible counter. Called only from `Base.similar` below.
    TimedScheduler(inner::S, shared::Threads.Atomic{UInt64}) where {S<:Dagger.DataDepsScheduler} =
        new{S}(inner, shared)
end

function Dagger.datadeps_schedule_dag_aot!(sched::TimedScheduler, schedule, dag_spec, all_procs, all_scope)
    t0 = time_ns()
    Dagger.datadeps_schedule_dag_aot!(sched.inner, schedule, dag_spec, all_procs, all_scope)
    Threads.atomic_add!(sched.last_aot_ns, time_ns() - t0)
    return
end

function Dagger.datadeps_schedule_task_jit!(sched::TimedScheduler, all_procs, all_scope, task_scope, spec, task)
    return Dagger.datadeps_schedule_task_jit!(sched.inner, all_procs, all_scope, task_scope, spec, task)
end

# Hierarchical scheduling calls `similar` per partition to obtain a fresh
# scheduler shard. The default `Base.similar(::DataDepsScheduler) = typeof(s)()`
# fails on `TimedScheduler` since the primary ctor requires an inner arg.
# We provide an explicit `similar` that (1) recursively `similar`s the inner
# scheduler so its own mutable state (RNG etc.) is refreshed per shard, and
# (2) shares the parent's atomic accumulator so all shard-recorded AOT times
# sum into the counter the driver reads after the run.
Base.similar(s::TimedScheduler) = TimedScheduler(similar(s.inner), s.last_aot_ns)

# Forward equivalence/cache hooks so cached schedules stay partitioned by the
# inner scheduler's type (TimedScheduler{Greedy} reuses Greedy entries).
Dagger.datadeps_schedule_cache(sched::TimedScheduler) =
    Dagger.datadeps_schedule_cache(sched.inner)
Dagger.datadeps_dag_equivalent(sched::TimedScheduler, dspec1, dspec2) =
    Dagger.datadeps_dag_equivalent(sched.inner, dspec1, dspec2)
Dagger.datadeps_argspec_equivalent(sched::TimedScheduler, a1, a2) =
    Dagger.datadeps_argspec_equivalent(sched.inner, a1, a2)
Dagger.datadeps_ainfo_equivalent(sched::TimedScheduler, a1, a2) =
    Dagger.datadeps_ainfo_equivalent(sched.inner, a1, a2)

scheduler_name(s::TimedScheduler) = scheduler_name(s.inner)
scheduler_name(s::Dagger.DataDepsScheduler) = string(nameof(typeof(s)))

# Clear the per-spec schedule cache only — leave MT.GLOBAL_METRICS_CACHE
# alone since Greedy consults it for cost-model lookups.
function reset_scheduler!(inner::Dagger.DataDepsScheduler)
    empty!(Dagger.datadeps_schedule_cache(inner))
    return
end

function build_inputs(workload::Symbol, nt::Int, bs::Int)
    sz = nt * bs
    if workload === :cholesky
        M = make_spd_tiles(sz, bs)
        return (; M, dense_reference = _assemble_dense(M))
    elseif workload === :matmul
        A, B, C = make_matmul_tiles(sz, bs)
        return (; A, B, C,
                  dense_A = _assemble_dense(A),
                  dense_B = _assemble_dense(B))
    else
        error("Unknown workload $workload")
    end
end

function run_workload!(workload::Symbol, inputs, sched::Dagger.DataDepsScheduler)
    if workload === :cholesky
        Dagger.spawn_datadeps(; scheduler=sched) do
            tiled_cholesky!(inputs.M)
        end
    elseif workload === :matmul
        Dagger.spawn_datadeps(; scheduler=sched) do
            tiled_matmul!(inputs.C, inputs.A, inputs.B)
        end
    end
    return
end

# Materialise a `tiles[i,j]` element into a plain matrix regardless of
# whether it's a locally-resident `Matrix` (single-process paths) or a
# distributed `Dagger.Chunk` (multi-process paths — see
# `make_spd_tiles` / `make_matmul_tiles`). For a `Chunk`, `fetch` is a
# `collect` / `move` from the chunk's owning worker back to master,
# which is exactly what we need to reassemble the tiles into a dense
# reference on master for `verify_workload` to compare against.
_fetch_tile(t::AbstractMatrix) = t
_fetch_tile(t::Dagger.Chunk) = fetch(t)::AbstractMatrix

function _assemble_dense(tiles::AbstractMatrix)
    nt = size(tiles, 1)
    first_tile = _fetch_tile(tiles[1, 1])
    bs = size(first_tile, 1)
    out = zeros(eltype(first_tile), nt * bs, nt * bs)
    @inbounds for i in 1:nt, j in 1:nt
        tile = _fetch_tile(tiles[i, j])
        out[(i-1)*bs+1:i*bs, (j-1)*bs+1:j*bs] .= tile
    end
    return out
end

function verify_workload(workload::Symbol, inputs)
    if workload === :cholesky
        # Tiled Cholesky writes the lower triangle; zero strict-upper before
        # comparing L*L' to the original SPD.
        L = _assemble_dense(inputs.M)
        rows, cols = axes(L)
        for i in rows, j in cols
            j > i && (L[i, j] = 0.0)
        end
        ref = inputs.dense_reference
        denom = max(norm(ref), eps(Float64))
        rel = norm(L * L' - ref) / denom
        return (rel < 1e-8, rel)
    elseif workload === :matmul
        C = _assemble_dense(inputs.C)
        ref = inputs.dense_A * inputs.dense_B
        denom = max(norm(ref), eps(Float64))
        rel = norm(C - ref) / denom
        return (rel < 1e-10, rel)
    else
        error("Unknown workload $workload")
    end
end

struct RunResult
    workload::Symbol
    scheduler::String
    tile_count::Int
    block_size::Int
    trial::Int
    total_wallclock_ns::UInt64
    sched_phase_ns::UInt64
    exec_span_ns::UInt64
    n_tasks::Int
    n_copies::Int
    copy_total_ns::UInt64
    compute_total_ns::UInt64
    move_total_ns::UInt64
    metrics_warm::Bool
end

function run_once(workload::Symbol, inner::Dagger.DataDepsScheduler, nt::Int, bs::Int,
                  trial::Int; collect_logs::Bool, metrics_warm::Bool)
    timed = TimedScheduler(inner)
    reset_scheduler!(inner)

    if collect_logs
        Dagger.enable_logging!(all_task_deps=false, tasknames=false)
    end

    # Inputs built outside the timed region so allocation does not pollute
    # the measured wall-clock.
    inputs = build_inputs(workload, nt, bs)
    t0 = time_ns()
    run_workload!(workload, inputs, timed)
    t1 = time_ns()

    total_ns = t1 - t0
    sched_ns = timed.last_aot_ns[]

    if collect_logs
        logs = Dagger.fetch_logs!()
        Dagger.disable_logging!()
        s = summarize_phases(total_ns, sched_ns, logs)
        return RunResult(workload, scheduler_name(inner), nt, bs, trial,
                         s.total_wallclock_ns, s.sched_phase_ns, s.exec_span_ns,
                         s.n_tasks, s.n_copies, s.copy_total_ns,
                         s.compute_total_ns, s.move_total_ns, metrics_warm)
    else
        return RunResult(workload, scheduler_name(inner), nt, bs, trial,
                         total_ns, sched_ns, UInt64(0),
                         0, 0, UInt64(0), UInt64(0), UInt64(0), metrics_warm)
    end
end

# Unmeasured RoundRobin pass to populate MT.GLOBAL_METRICS_CACHE so Greedy's
# subsequent cost lookups see real samples instead of GREEDY_DEFAULT_* fallbacks.
# RoundRobin is intentional: warm-pass behaviour stays deterministic across
# scheduler cells because we're recording ground-truth metrics, not the
# scheduler-under-test's self-prediction.
function warm_metrics_for!(workload::Symbol, nt::Int, bs::Int)
    inputs = build_inputs(workload, nt, bs)
    warm_sched = Dagger.RoundRobinScheduler()
    run_workload!(workload, inputs, warm_sched)
    empty!(Dagger.datadeps_schedule_cache(warm_sched))
    return
end

const DEFAULT_TILE_COUNTS = [2, 4, 8]
const DEFAULT_BLOCK_SIZE = 128
const DEFAULT_TRIALS = 3
const DEFAULT_WARMUP = 1

# Factories — not instances — because RoundRobin holds mutable state and each
# trial needs a fresh copy. Default MILP budget is set generously since a
# K~64 solve can exceed a minute; callers override as needed. Heuristic
# (IG, SA) wall-clock budgets default to 60 s per Przemek's & Julian's ask:
# "quick-and-dirty 1 minute timeout" so metaheuristic runs stay within a
# reasonable everyday-user budget instead of exhausting the full iteration
# schedule at K~10000. Pass `Inf` to reproduce the pre-budget behaviour.
function default_scheduler_factories(; milp_time_limit_sec::Real=120.0,
                                       heuristic_time_limit_sec::Real=60.0)
    factories = [
        "RoundRobinScheduler"         => () -> Dagger.RoundRobinScheduler(),
        "GreedyScheduler"             => () -> Dagger.GreedyScheduler(),
        "IteratedGreedyScheduler"     => () -> Dagger.IteratedGreedyScheduler(;
                                                    time_limit_sec=heuristic_time_limit_sec),
        "SimulatedAnnealingScheduler" => () -> Dagger.SimulatedAnnealingScheduler(;
                                                    time_limit_sec=heuristic_time_limit_sec),
    ]
    if HAS_MILP
        push!(factories,
              "JuMPScheduler" => () -> Dagger.JuMPScheduler(HiGHS.Optimizer;
                                                            time_limit_sec=milp_time_limit_sec))
        push!(factories,
              "OptimizingScheduler" => () -> Dagger.OptimizingScheduler(;
                                                optimizer=HiGHS.Optimizer,
                                                milp_time_limit_sec=milp_time_limit_sec,
                                                ig_time_limit_sec=heuristic_time_limit_sec,
                                                sa_time_limit_sec=heuristic_time_limit_sec))
    else
        push!(factories,
              "OptimizingScheduler" => () -> Dagger.OptimizingScheduler(;
                                                ig_time_limit_sec=heuristic_time_limit_sec,
                                                sa_time_limit_sec=heuristic_time_limit_sec))
    end
    return factories
end

# Serialize a RunResult row to a CSV line (shared by streaming + batch writers).
function _run_result_csv_row(r::RunResult)
    return (
        r.workload, r.scheduler, r.tile_count, r.block_size, r.trial,
        ns_to_ms(r.total_wallclock_ns),
        ns_to_ms(r.sched_phase_ns),
        ns_to_ms(r.exec_span_ns),
        r.n_tasks, r.n_copies,
        ns_to_ms(r.copy_total_ns),
        ns_to_ms(r.compute_total_ns),
        ns_to_ms(r.move_total_ns),
        r.metrics_warm,
    )
end

function run_sweep(; workloads = (:cholesky, :matmul),
                     tile_counts = DEFAULT_TILE_COUNTS,
                     block_size = DEFAULT_BLOCK_SIZE,
                     trials = DEFAULT_TRIALS,
                     warmup = DEFAULT_WARMUP,
                     scheduler_factories = default_scheduler_factories(),
                     collect_logs = true,
                     metrics_warm = false,
                     check_correctness = false,
                     verbose = true,
                     # Incremental CSV output — if provided, rows are appended
                     # and flushed after each trial completes, so a mid-sweep
                     # crash (Dagger, HiGHS abort, segfault, OOM) preserves
                     # data up to the crash. The batch `write_csv` at end of
                     # `main` is skipped when these are set. Correctness rows
                     # stream to `correctness_output`; per-cell correctness
                     # crashes are also caught and logged, not fatal.
                     output::Union{Nothing, AbstractString} = nothing,
                     correctness_output::Union{Nothing, AbstractString} = nothing)
    results = RunResult[]
    correctness = NamedTuple[]

    # Open output files immediately and write headers so crash-time state on
    # disk always has valid CSV. `open ... "w"` truncates; if the caller wants
    # to preserve prior partial data they should rename it first (matches the
    # previous end-of-run write_csv semantics — truncate on start).
    output_io = output === nothing ? nothing : open(output, "w")
    if output_io !== nothing
        println(output_io, join(CSV_HEADER, ","))
        flush(output_io)
    end
    correctness_io = correctness_output === nothing ? nothing : open(correctness_output, "w")
    if correctness_io !== nothing
        println(correctness_io, "workload,scheduler,tile_count,passed,rel_err")
        flush(correctness_io)
    end

    try
        for workload in workloads
            for nt in tile_counts
                for (name, factory) in scheduler_factories
                    verbose && println("→ $workload nt=$nt $name  (warmup×$warmup, trials×$trials, metrics_warm=$metrics_warm)")
                    if metrics_warm
                        try
                            warm_metrics_for!(workload, nt, block_size)
                        catch e
                            @warn "metrics-warm crashed, skipping cell" workload=workload tile_count=nt scheduler=name exception=(e, catch_backtrace())
                            continue
                        end
                    end
                    # Warmup mirrors the measured-trial logging state so JIT
                    # compilation amortizes on the same specializations. Wrap
                    # so a scheduler that consistently crashes (e.g. HiGHS
                    # `std::length_error` at K=512) skips the cell instead of
                    # aborting the whole sweep.
                    warmup_ok = true
                    for _ in 1:warmup
                        try
                            run_once(workload, factory(), nt, block_size, 0;
                                     collect_logs, metrics_warm)
                        catch e
                            @warn "warmup crashed, skipping cell" workload=workload tile_count=nt scheduler=name exception=(e, catch_backtrace())
                            warmup_ok = false
                            break
                        end
                    end
                    warmup_ok || continue

                    if check_correctness
                        try
                            inputs = build_inputs(workload, nt, block_size)
                            run_workload!(workload, inputs, factory())
                            passed, rel = verify_workload(workload, inputs)
                            row = (; workload, scheduler=name, tile_count=nt,
                                     passed, rel_err=rel)
                            push!(correctness, row)
                            if correctness_io !== nothing
                                println(correctness_io,
                                        "$(row.workload),$(row.scheduler),$(row.tile_count),$(row.passed),$(row.rel_err)")
                                flush(correctness_io)
                            end
                            if verbose
                                status = passed ? "PASS" : "FAIL"
                                @printf("    correctness: %s (rel_err=%.2e)\n", status, rel)
                            end
                            if !passed
                                @warn "Correctness check FAILED" workload scheduler=name tile_count=nt rel_err=rel
                            end
                        catch e
                            @warn "correctness crashed, continuing" workload=workload tile_count=nt scheduler=name exception=(e, catch_backtrace())
                        end
                    end
                    for trial in 1:trials
                        try
                            r = run_once(workload, factory(), nt, block_size, trial;
                                         collect_logs, metrics_warm)
                            push!(results, r)
                            # Streaming CSV write — flush after each row so a
                            # subsequent crash (this trial or later) doesn't
                            # lose the trials that have already succeeded.
                            if output_io !== nothing
                                println(output_io, join(_run_result_csv_row(r), ","))
                                flush(output_io)
                            end
                            if verbose
                                @printf("    trial %d: total=%.3f ms  sched=%.3f ms  exec=%.3f ms  tasks=%d\n",
                                        trial,
                                        r.total_wallclock_ns / 1e6,
                                        r.sched_phase_ns / 1e6,
                                        r.exec_span_ns / 1e6,
                                        r.n_tasks)
                            end
                        catch e
                            @warn "trial crashed, continuing sweep" workload=workload tile_count=nt scheduler=name trial=trial exception=(e, catch_backtrace())
                        end
                    end
                end
            end
        end
    finally
        output_io === nothing || close(output_io)
        correctness_io === nothing || close(correctness_io)
    end
    return (; results, correctness)
end

const CSV_HEADER = [
    "workload", "scheduler", "tile_count", "block_size", "trial",
    "total_wallclock_ms", "sched_phase_ms", "exec_span_ms",
    "n_tasks", "n_copies",
    "copy_total_ms", "compute_total_ms", "move_total_ms",
    "metrics_warm",
]

ns_to_ms(x::UInt64) = x / 1e6

function write_csv(path::AbstractString, results::Vector{RunResult})
    open(path, "w") do io
        println(io, join(CSV_HEADER, ","))
        for r in results
            row = (
                r.workload, r.scheduler, r.tile_count, r.block_size, r.trial,
                ns_to_ms(r.total_wallclock_ns),
                ns_to_ms(r.sched_phase_ns),
                ns_to_ms(r.exec_span_ns),
                r.n_tasks, r.n_copies,
                ns_to_ms(r.copy_total_ns),
                ns_to_ms(r.compute_total_ns),
                ns_to_ms(r.move_total_ns),
                r.metrics_warm,
            )
            println(io, join(row, ","))
        end
    end
    return path
end

function write_correctness_csv(path::AbstractString, rows::Vector{NamedTuple})
    isempty(rows) && return path
    open(path, "w") do io
        println(io, "workload,scheduler,tile_count,passed,rel_err")
        for r in rows
            println(io, "$(r.workload),$(r.scheduler),$(r.tile_count),$(r.passed),$(r.rel_err)")
        end
    end
    return path
end

function main(args = ARGS)
    workloads = (:cholesky, :matmul)
    tile_counts = DEFAULT_TILE_COUNTS
    block_size = DEFAULT_BLOCK_SIZE
    trials = DEFAULT_TRIALS
    warmup = DEFAULT_WARMUP
    output = "datadeps_schedulers_results.csv"
    metrics_warm = false
    check_correctness = false
    summary_path = ""

    i = 1
    while i <= length(args)
        a = args[i]
        if a == "--workloads"
            workloads = Tuple(Symbol.(split(args[i+1], ",")))
            i += 2
        elseif a == "--tile-counts"
            tile_counts = parse.(Int, split(args[i+1], ","))
            i += 2
        elseif a == "--block-size"
            block_size = parse(Int, args[i+1]); i += 2
        elseif a == "--trials"
            trials = parse(Int, args[i+1]); i += 2
        elseif a == "--warmup"
            warmup = parse(Int, args[i+1]); i += 2
        elseif a == "--output"
            output = args[i+1]; i += 2
        elseif a == "--metrics-warm"
            metrics_warm = true; i += 1
        elseif a == "--check-correctness"
            check_correctness = true; i += 1
        elseif a == "--summary"
            summary_path = args[i+1]; i += 2
        elseif a == "--help" || a == "-h"
            println("""
Usage: julia --project bench/datadeps_schedulers/driver.jl [options]

Options:
  --workloads          Comma list of workloads: cholesky,matmul   (default both)
  --tile-counts        Comma list of nt values                    (default 2,4,8)
  --block-size         Tile side length in elements               (default 128)
  --trials             Measured trials per cell                   (default 3)
  --warmup             Warmup runs per cell                       (default 1)
  --output             CSV output path                            (default datadeps_schedulers_results.csv)
  --metrics-warm       Pre-warm global MetricsTracker cache before measured trials so Greedy has real cost data
  --check-correctness  Verify each cell's result against a dense reference (Cholesky: L*L'≈A; matmul: A*B)
  --summary PATH       Also write a Markdown median-aggregated summary table to PATH
""")
            return nothing
        else
            error("Unknown arg $a (use --help)")
        end
    end

    # Stream rows to disk during the sweep so a mid-run crash (Dagger
    # aborts, HiGHS `std::length_error`, segfault, OOM) preserves data
    # up to the crash rather than losing an entire ~10h sweep.
    correctness_output = check_correctness ?
        replace(output, r"\.csv$" => "_correctness.csv") : nothing
    out = run_sweep(; workloads, tile_counts, block_size, trials, warmup,
                      metrics_warm, check_correctness,
                      output=output, correctness_output=correctness_output)
    println("Wrote $(length(out.results)) rows to $output (streamed incrementally)")

    if !isempty(out.correctness) && correctness_output !== nothing
        println("Wrote $(length(out.correctness)) correctness rows to $correctness_output (streamed incrementally)")
    end

    if !isempty(summary_path)
        summarize_to_markdown(output, summary_path)
        println("Wrote summary to $summary_path")
    end

    return out
end

if abspath(PROGRAM_FILE) == @__FILE__
    main()
end
