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
    elseif workload === :lu
        A = make_lu_tiles(sz, bs)
        return (; A, dense_reference = _assemble_dense(A))
    elseif workload === :random_dag
        # For random_dag, `nt` is reinterpreted as `n_tasks` (total DAG
        # nodes), not a tile-grid dimension — structured BLAS workloads
        # scale K as `f(nt)` (nt³ for matmul, ~nt³/6 for cholesky) but
        # random DAGs are naturally parameterised by task count directly.
        # `n_levels ≈ √n_tasks` matches Sinnen-Sousa 2004's recommended
        # aspect ratio (balanced depth vs. width so no level dominates).
        # edge_probability=0.3 is the Topcuoglu 2002 §5.1 HEFT-eval
        # default; produces DAGs with average in-degree ≈ 1.5 after
        # the 2-parent cap, matching typical HEFT benchmark densities.
        n_tasks = nt
        n_levels = max(2, round(Int, sqrt(nt)))
        s = make_random_dag_tiles(n_tasks, n_levels, 0.3, bs; seed=42)
        return (; tiles = s.tiles,
                  parents = s.parents,
                  initial_A = s.initial_A,
                  initial_B = s.initial_B,
                  level_of = s.level_of)
    else
        error("Unknown workload $workload")
    end
end

"""
    _bench_scope_all_procs() -> Union{Nothing, Dagger.UnionScope}

Scope admitting every enumerated processor, or `nothing` on a CPU-only
session.

`spawn_datadeps` derives its candidate set from `get_compute_scope()`, whose
default carries `DefaultEnabledTaint` and therefore admits only processors
with `default_enabled(proc) == true`. `CuArrayDeviceProc` (and the ROCm
equivalent) return `false` there by design, so GPUs are dropped from
`all_procs` before any scheduler sees them -- see the `filter!` in
`distribute_tasks_hierarchical!` (src/datadeps/hierarchical.jl) and its twin
in `distribute_tasks!` (src/datadeps/queue.jl). The effect is that a datadeps
region under the default scope is CPU-only regardless of scheduler, cost
model, or available hardware.

Passing an explicit `UnionScope` of `ExactScope`s bypasses the taint, which
is the same mechanism `warm_gpu_metrics_for!` already relies on to force its
warmup onto GPUs. Returns `nothing` when no accelerator is present so
CPU-only runs take the untouched default path.
"""
function _bench_scope_all_procs()
    procs = collect(Dagger.all_processors())
    isempty(procs) && return nothing
    any(!_is_cpu_proc, procs) || return nothing   # CPU-only: no wrap needed
    return Dagger.UnionScope([Dagger.ExactScope(p) for p in procs])
end

# Outer entry point: wraps the region in an all-processor scope when
# accelerators are present. `warm_gpu_metrics_for!` calls the inner form
# directly under its own GPU-only scope to avoid nesting two scopes.
function run_workload!(workload::Symbol, inputs, sched::Dagger.DataDepsScheduler)
    scope = _bench_scope_all_procs()
    if scope === nothing
        _run_workload_inner!(workload, inputs, sched)
    else
        Dagger.with_options(; scope=scope) do
            _run_workload_inner!(workload, inputs, sched)
        end
    end
    return
end

function _run_workload_inner!(workload::Symbol, inputs, sched::Dagger.DataDepsScheduler)
    if workload === :cholesky
        Dagger.spawn_datadeps(; scheduler=sched) do
            tiled_cholesky!(inputs.M)
        end
    elseif workload === :matmul
        Dagger.spawn_datadeps(; scheduler=sched) do
            tiled_matmul!(inputs.C, inputs.A, inputs.B)
        end
    elseif workload === :lu
        Dagger.spawn_datadeps(; scheduler=sched) do
            tiled_lu!(inputs.A)
        end
    elseif workload === :random_dag
        Dagger.spawn_datadeps(; scheduler=sched) do
            tiled_random_dag!(inputs.tiles, inputs.parents,
                              inputs.initial_A, inputs.initial_B)
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
    elseif workload === :lu
        # Unpivoted LU overwrites A in place: unit-lower L in the strict
        # lower triangle, U in the upper triangle including the diagonal.
        # Reconstruct and compare L*U against the original matrix.
        F = _assemble_dense(inputs.A)
        L = tril(F, -1) + LinearAlgebra.I
        U = triu(F)
        ref = inputs.dense_reference
        denom = max(norm(ref), eps(Float64))
        rel = norm(L * U - ref) / denom
        return (rel < 1e-8, rel)
    elseif workload === :matmul
        C = _assemble_dense(inputs.C)
        ref = inputs.dense_A * inputs.dense_B
        denom = max(norm(ref), eps(Float64))
        rel = norm(C - ref) / denom
        return (rel < 1e-10, rel)
    elseif workload === :random_dag
        # Random DAG has no closed-form reference — task closures compose
        # arbitrary intermediate products, and the DAG topology is
        # randomised per `seed`. Verification is finiteness-only:
        # every output tile must have finite entries (no NaN/Inf from
        # ill-conditioned intermediate products or dispatch failures).
        # Any non-finite value indicates a scheduler / dispatch /
        # memory-transfer bug worth flagging via the correctness stream.
        all_finite = true
        worst_max = 0.0
        for t in inputs.tiles
            tile = _fetch_tile(t)
            if !all(isfinite, tile)
                all_finite = false
                break
            end
            m = maximum(abs, tile)
            m > worst_max && (worst_max = m)
        end
        # Overload `rel_err` position to carry the largest observed
        # magnitude — useful diagnostic for detecting silent value
        # explosions before they become Inf.
        return (all_finite, worst_max)
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

    Dagger.MetricsTracker.trim!(Dagger.MetricsTracker.global_metrics_cache();
                                keep_per_metric=METRICS_KEEP_PER_METRIC)

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

# GPU-scoped warmup — dedicated cost-model priming for non-CPU procs.
#
# Why this is needed: `_eft_runtime_ns` at `src/datadeps/scheduling.jl:807`
# falls back to `GREEDY_DEFAULT_RUNTIME_NS = 1_000_000_000` (1 second) when
# MetricsTracker has no `(signature, proc)` sample. On a CPU-only cache,
# the regular `warm_metrics_for!` RoundRobin pass eventually cycles through
# GPU procs and would populate them — but under any Greedy-driven cell that
# follows, Greedy compares CPU (~20 ms observed) vs GPU (~1000 ms fallback)
# and never picks GPU. GPU thus never gets any samples in the subsequent
# measured trials either. Result on hudson H100: 0% GPU utilisation across
# a full sweep despite the CPU+GPU code path being wired up correctly.
#
# Fix: before the CPU RR warmup, run a small workload with scope forced to
# the union of every discovered GPU proc. Every task in the forced pass
# lands on a GPU proc; `MT.GLOBAL_METRICS_CACHE` gets primed with real GPU
# `(signature, proc) -> runtime_ns` samples. Greedy's subsequent EFT
# calculation then compares real CPU vs real GPU runtimes and picks GPU
# for tasks where it's actually faster.
#
# Vendor-agnostic: enumerates non-CPU procs via `Dagger.all_processors()`
# and constructs a `UnionScope(ExactScope(p) for p in gpu_procs)`. Works
# unchanged across CUDAExt, ROCExt, MetalExt, oneAPIExt, OpenCLExt — the
# scope machinery is proc-type-agnostic and every extension registers its
# proc type via `add_processor_callback!`, so `all_processors()` naturally
# includes whatever accelerator the session loaded.
#
# Wrapped in try/catch: any GPU warmup failure (e.g. an untested
# cross-space transfer path or a driver hiccup) is logged and swallowed
# rather than killing the sweep. The subsequent CPU warmup + measured
# trials continue on CPU-only cost data; the cell is honestly labelled
# "GPU cost model uncalibrated" via the log message. Better degraded
# data than no data.
function warm_gpu_metrics_for!(workload::Symbol, nt::Int, bs::Int)
    procs = collect(Dagger.all_processors())
    # Reuse `_is_cpu_proc` from `workloads.jl` (included above) so the
    # CPU/GPU partition is single-sourced with `_placement_procs`.
    gpu_procs = filter(!_is_cpu_proc, procs)
    isempty(gpu_procs) && return  # CPU-only session, nothing to warm
    # UnionScope of every non-CPU proc — force each warmup task onto a
    # GPU proc so cost-model samples are populated for every GPU the
    # scheduler will consider during measured runs. Cross-GPU transfers
    # that this scope permits are made safe by the `CUDA.synchronize()`
    # / `AMDGPU.synchronize()` barrier in the vendor extensions'
    # `Dagger.unsafe_free!(::CuArray|::ROCArray)` — see the detailed
    # rationale in `ext/CUDAExt.jl` where the barrier is applied.
    gpu_scope = Dagger.UnionScope([Dagger.ExactScope(p) for p in gpu_procs])
    inputs = build_inputs(workload, nt, bs)
    warm_sched = Dagger.RoundRobinScheduler()
    try
        Dagger.with_options(; scope=gpu_scope) do
            _run_workload_inner!(workload, inputs, warm_sched)
        end
    catch e
        @warn "GPU metrics-warm crashed, continuing with CPU-only cost model" workload=workload tile_count=nt block_size=bs n_gpu_procs=length(gpu_procs) exception=(e, catch_backtrace())
    end
    empty!(Dagger.datadeps_schedule_cache(warm_sched))
    return
end

const DEFAULT_TILE_COUNTS = [2, 4, 8]
const DEFAULT_BLOCK_SIZE = 128
const DEFAULT_TRIALS = 3
const DEFAULT_WARMUP = 1

# Bounded to keep the metrics cache from degrading the write path (unbounded
# growth took a run from 14.8s to 48.0s), but large enough that compute-task
# samples are not evicted by the `unsafe_free!`/`move!` entries that dominate
# by volume -- at 100 they crowded out ~60% of the cache.
const METRICS_KEEP_PER_METRIC = 10000

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
                        # GPU-scoped warmup FIRST so cost model has real GPU
                        # samples before CPU-RR warmup or measured trials.
                        # Without this, GPU procs stay at the 1s fallback in
                        # `_eft_runtime_ns` and Greedy never picks them --
                        # the chicken-and-egg documented on `warm_gpu_metrics_for!`.
                        # GPU warmup is a no-op on CPU-only sessions.
                        try
                            warm_gpu_metrics_for!(workload, nt, block_size)
                        catch e
                            @warn "GPU metrics-warm outer crashed, continuing with CPU-only warmup" workload=workload tile_count=nt scheduler=name exception=(e, catch_backtrace())
                        end
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
  --workloads          Comma list of workloads:                   (default cholesky,matmul)
                          cholesky      — tiled Cholesky on SPD, K ~ nt(nt+1)(nt+2)/6
                          matmul        — tiled A*B accumulate,  K = nt^3
                          random_dag    — Sinnen-Sousa Layer-by-Layer random DAG,
                                          K = nt  (use larger --tile-counts values;
                                          40..320 covers HEFT-eval literature range)
  --tile-counts        Comma list of nt values                    (default 2,4,8)
                          For cholesky/matmul: nt = tile-grid side length.
                          For random_dag:      nt = n_tasks total in the DAG.
  --block-size         Tile side length in elements               (default 128)
  --trials             Measured trials per cell                   (default 3)
  --warmup             Warmup runs per cell                       (default 1)
  --output             CSV output path                            (default datadeps_schedulers_results.csv)
  --metrics-warm       Pre-warm global MetricsTracker cache before measured trials so Greedy has real cost data
                          On CPU+GPU sessions, the RoundRobin warm pass naturally hits GPU
                          procs enumerated via Dagger.all_processors() and populates per-proc
                          runtimes for both proc classes.
  --check-correctness  Verify each cell's result against a reference
                          (Cholesky: L*L'≈A; matmul: A*B; random_dag: finiteness only)
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
