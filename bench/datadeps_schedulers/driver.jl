using Dagger
using Printf
using LinearAlgebra

include("workloads.jl")
include("log_analysis.jl")
include("summarize.jl")

# A timing wrapper around any DataDepsScheduler. Records the wall-clock cost
# of `datadeps_schedule_dag_aot!` into a mutable `Ref` so the driver can read
# back the AOT cost after `spawn_datadeps` returns.
#
# JIT calls and equivalence/cache lookups are delegated to the inner scheduler,
# so wrapping does not change scheduling decisions.
mutable struct TimedScheduler{S<:Dagger.DataDepsScheduler} <: Dagger.DataDepsScheduler
    inner::S
    last_aot_ns::Base.RefValue{UInt64}
    TimedScheduler(inner::S) where {S<:Dagger.DataDepsScheduler} =
        new{S}(inner, Ref(UInt64(0)))
end

function Dagger.datadeps_schedule_dag_aot!(sched::TimedScheduler, schedule, dag_spec, all_procs, all_scope)
    t0 = time_ns()
    Dagger.datadeps_schedule_dag_aot!(sched.inner, schedule, dag_spec, all_procs, all_scope)
    sched.last_aot_ns[] = time_ns() - t0
    return
end

function Dagger.datadeps_schedule_task_jit!(sched::TimedScheduler, all_procs, all_scope, task_scope, spec, task)
    return Dagger.datadeps_schedule_task_jit!(sched.inner, all_procs, all_scope, task_scope, spec, task)
end

# Forward cache-key and equivalence so cached schedules remain partitioned per
# *inner* type (TimedScheduler{Greedy} reuses Greedy entries, not its own).
Dagger.datadeps_schedule_cache(sched::TimedScheduler) =
    Dagger.datadeps_schedule_cache(sched.inner)
Dagger.datadeps_dag_equivalent(sched::TimedScheduler, dspec1, dspec2) =
    Dagger.datadeps_dag_equivalent(sched.inner, dspec1, dspec2)
Dagger.datadeps_argspec_equivalent(sched::TimedScheduler, a1, a2) =
    Dagger.datadeps_argspec_equivalent(sched.inner, a1, a2)
Dagger.datadeps_ainfo_equivalent(sched::TimedScheduler, a1, a2) =
    Dagger.datadeps_ainfo_equivalent(sched.inner, a1, a2)

# Names a scheduler instance for output rows. Falls back to its type name.
scheduler_name(s::TimedScheduler) = scheduler_name(s.inner)
scheduler_name(s::Dagger.DataDepsScheduler) = string(nameof(typeof(s)))

# Per-DAG-spec schedule cache is task-local and partitioned by scheduler type.
# Clear before each measured run so cached schedules from prior cells do not
# bias the next. Leaves the *global* MetricsTracker cache alone — that's the
# data Greedy actually consults at decision time.
function reset_scheduler!(inner::Dagger.DataDepsScheduler)
    empty!(Dagger.datadeps_schedule_cache(inner))
    return
end

# ---------- Workload-side helpers (run, warm, verify) ----------

# Build inputs for a workload. Returned tuple is passed to `run_workload!` and
# (optionally) `verify_workload`.
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

# Run the workload under the given scheduler. Mutates inputs in place.
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

# Reassemble a tile-grid back into a dense matrix for verification.
function _assemble_dense(tiles::Matrix{<:Matrix})
    nt = size(tiles, 1)
    bs = size(tiles[1, 1], 1)
    out = zeros(eltype(tiles[1, 1]), nt * bs, nt * bs)
    for i in 1:nt, j in 1:nt
        out[(i-1)*bs+1:i*bs, (j-1)*bs+1:j*bs] .= tiles[i, j]
    end
    return out
end

# Check the workload result against a dense reference. Returns
# (passed::Bool, max_relative_error::Float64).
function verify_workload(workload::Symbol, inputs)
    if workload === :cholesky
        # Tiled Cholesky writes the lower triangle. Reassemble L from tiles,
        # zero the strict upper, then compare L * L' to the original SPD.
        result_tiles = inputs.M
        L = _assemble_dense(result_tiles)
        rows, cols = axes(L)
        for i in rows, j in cols
            j > i && (L[i, j] = 0.0)
        end
        reconstructed = L * L'
        ref = inputs.dense_reference
        denom = max(norm(ref), eps(Float64))
        rel = norm(reconstructed - ref) / denom
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

# ---------- Single measurement ----------

struct RunResult
    workload::Symbol           # :cholesky or :matmul
    scheduler::String
    tile_count::Int            # nt (matrix is nt × nt blocks of size bs × bs)
    block_size::Int            # bs
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

    # Build inputs OUTSIDE the timed region so we measure scheduling +
    # execution, not allocation of the input tiles.
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

# Run an unmeasured workload purely to populate the *global* MetricsTracker
# cache with per-signature runtime and per-(src,dst) move-rate samples. Greedy
# then has real data to consult on the next call instead of falling back to
# `GREEDY_DEFAULT_RUNTIME_NS` constants.
#
# Uses a fresh RoundRobin so the warm pass behaviour is deterministic across
# scheduler cells (we are populating ground-truth metrics, not Greedy's
# self-prediction). Schedule cache for the warm scheduler is cleared
# immediately afterward so it does not affect later cells.
function warm_metrics_for!(workload::Symbol, nt::Int, bs::Int)
    inputs = build_inputs(workload, nt, bs)
    warm_sched = Dagger.RoundRobinScheduler()
    run_workload!(workload, inputs, warm_sched)
    empty!(Dagger.datadeps_schedule_cache(warm_sched))
    return
end

# ---------- Sweep ----------

const DEFAULT_TILE_COUNTS = [2, 4, 8]   # nt values; total side = nt * bs
const DEFAULT_BLOCK_SIZE = 128
const DEFAULT_TRIALS = 3
const DEFAULT_WARMUP = 1

# Returns a Vector{Pair{String, ()->DataDepsScheduler}} so each trial gets a
# fresh scheduler instance (RoundRobin holds mutable state).
function default_scheduler_factories()
    return [
        "RoundRobinScheduler"    => () -> Dagger.RoundRobinScheduler(),
        "GreedyScheduler"        => () -> Dagger.GreedyScheduler(),
        "IteratedGreedyScheduler" => () -> Dagger.IteratedGreedyScheduler(),
    ]
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
                     verbose = true)
    results = RunResult[]
    correctness = NamedTuple[]   # (workload, scheduler, nt, passed, rel_err)
    for workload in workloads
        for nt in tile_counts
            for (name, factory) in scheduler_factories
                # Per (workload, nt, scheduler) cell. If metrics_warm is on,
                # do one unmeasured RoundRobin pass to populate the global
                # metrics cache so Greedy has real data to consult.
                verbose && println("→ $workload nt=$nt $name  (warmup×$warmup, trials×$trials, metrics_warm=$metrics_warm)")
                if metrics_warm
                    warm_metrics_for!(workload, nt, block_size)
                end
                # Warmup mirrors the measured-trial code path (same logging
                # state) so JIT compilation is amortized on the same dispatch
                # specializations the measured trials will hit.
                for _ in 1:warmup
                    run_once(workload, factory(), nt, block_size, 0;
                             collect_logs, metrics_warm)
                end
                # Optional correctness check (uses one fresh run per cell;
                # does not contribute to timing).
                if check_correctness
                    inputs = build_inputs(workload, nt, block_size)
                    run_workload!(workload, inputs, factory())
                    passed, rel = verify_workload(workload, inputs)
                    push!(correctness,
                          (; workload, scheduler=name, tile_count=nt,
                             passed, rel_err=rel))
                    if verbose
                        status = passed ? "PASS" : "FAIL"
                        @printf("    correctness: %s (rel_err=%.2e)\n", status, rel)
                    end
                    if !passed
                        @warn "Correctness check FAILED" workload scheduler=name tile_count=nt rel_err=rel
                    end
                end
                for trial in 1:trials
                    r = run_once(workload, factory(), nt, block_size, trial;
                                 collect_logs, metrics_warm)
                    push!(results, r)
                    if verbose
                        @printf("    trial %d: total=%.3f ms  sched=%.3f ms  exec=%.3f ms  tasks=%d\n",
                                trial,
                                r.total_wallclock_ns / 1e6,
                                r.sched_phase_ns / 1e6,
                                r.exec_span_ns / 1e6,
                                r.n_tasks)
                    end
                end
            end
        end
    end
    return (; results, correctness)
end

# ---------- CSV emit ----------

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

# ---------- CLI entry ----------

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

    out = run_sweep(; workloads, tile_counts, block_size, trials, warmup,
                      metrics_warm, check_correctness)
    write_csv(output, out.results)
    println("Wrote $(length(out.results)) rows to $output")

    if !isempty(out.correctness)
        cpath = replace(output, r"\.csv$" => "_correctness.csv")
        write_correctness_csv(cpath, out.correctness)
        println("Wrote $(length(out.correctness)) correctness rows to $cpath")
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
