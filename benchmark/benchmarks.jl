# Dagger.jl benchmark suite for AirspeedVelocity.jl
#
# This script defines a top-level `const SUITE::BenchmarkGroup`, which is the
# entry point that AirspeedVelocity (`benchpkg`/`benchpkgplot`/`benchpkgtable`)
# discovers and runs. It can be used both locally and from CI to track
# regressions/improvements over time, across a range of scales, execution
# backends (CPU/GPU), and process/thread topologies.
#
# Because AirspeedVelocity re-runs this exact script unchanged across many git
# revisions, all configuration is done through environment variables (which stay
# constant across the compared revisions).
#
# Architecture: shared engine + thin adapter
# -------------------------------------------
# The heavy lifting (the timing loop, per-sample measurement, subprocess/OOM
# orchestration, memory-budget skipping, and result storage) lives in Dagger's
# built-in autotuner, `Dagger.Autotune`. This script is a thin front-end over
# that engine:
#
#   1. `suite_ops.jl` registers every regression case as a `benchmark_only`
#      Autotune `OperationSpec` + a single `:dagger` `AlgorithmSpec` running the
#      exact Dagger call the old suite ran. `benchmark_only` guarantees these
#      ops are never chosen by runtime `invoke_best`/`select_plan`.
#   2. `Autotune.run_sweep(cfg)` expands the trials and runs them out-of-process
#      in restartable worker subprocesses, returning raw `TrialResult`s. This is
#      the OOM-avoidance core: a benchmark that OOMs/wedges only takes down its
#      worker, never this orchestrator, and its scale (plus every larger scale of
#      the same series) is skipped before a fresh worker resumes.
#   3. We adapt each `TrialResult` back into a `BenchmarkTools.Trial` (times, GC
#      time, memory, allocations) wrapped in a `PrecomputedTrial` leaf, so
#      AirspeedVelocity's `run(SUITE)`/`tune!(SUITE)` just hand back the
#      already-measured data.
#
# Unlike the previous bespoke orchestrator, this front-end *does* load Dagger:
# it needs the registry to enumerate trials, and the capability probes in
# `suite_ops.jl` run tiny (8×8) Dagger ops to decide which cases a given
# revision supports. Actual benchmark computation still happens only in the
# worker subprocesses.
#
# Compare a tagged release against the current working tree, pointing
# AirspeedVelocity at this working-tree script (used for both revisions). The
# sparse suite's Krylov solvers need `Krylov`, so add it with `-a`:
#
#     BENCHMARK_SCALE="[100,1000]" \
#         benchpkg Dagger --rev v0.18.0,dirty --path . \
#             --script benchmark/benchmarks.jl -a Krylov
#
# When you pass `--script`, AirspeedVelocity does not read `benchmark/Project.toml`,
# so suites that need extra packages must list them via `-a` (e.g. `Krylov` for
# `sparse`).
#
# In CI against published versions you can instead let AirspeedVelocity fetch
# the script and `benchmark/Project.toml` automatically (omit `--script`):
#
#     benchpkg Dagger --rev v0.18.0,v0.19.0
#
# Environment variables
# ---------------------
# - BENCHMARK: Which suites to run, and with which execution/acceleration
#   methods. Format: "suite1:method1+accel1,method2;suite2:method1".
#   Available suites:
#     - "array"  : distributed elementwise/reduction DArray ops
#     - "linalg" : dense distributed linear algebra (gemm, gemv, syrk,
#                  cholesky, lu, qr, solve)
#     - "sparse" : sparse distributed linear algebra (spmv, spgemm, and
#                  iterative Krylov solve)
#     - "stencil": @stencil kernels
#   Available methods: "dagger" (Dagger DArray) and "raw" (native array, no
#   Dagger; only for the dense array/linalg cases). Combine with an accelerator
#   as "method+accel", e.g. "dagger+amdgpu" (GPU-backed DArray) or "raw+amdgpu"
#   (native GPU array).
#   Available accelerations: "cuda", "amdgpu", "opencl", "metal", "oneapi"
#   (require the relevant package, see below; a backend with no usable device is
#   skipped). Defaults to "array:dagger;linalg:dagger;sparse:dagger;stencil:dagger".
# - BENCHMARK_SCALE: Problem sizes, interpreted as the matrix dimension N. Given
#   as a Julia expression evaluating to an integer or an iterable of integers
#   (e.g. "1000", "[100, 1000, 10000]", or "100:100:1000"). When unset, a
#   log-spaced default ladder from N=100 to N=100_000 is used; each suite then
#   skips sizes whose estimated peak memory would exceed BENCHMARK_MEM_FRACTION
#   of total RAM, so the effective sizes adapt to the machine and avoid OOM.
# - BENCHMARK_MEM_FRACTION: Fraction of *total* system RAM a single benchmark's
#   estimated peak allocation may use before a size is skipped. Defaults to
#   "0.2" (conservative). Raise it to allow larger sizes, lower it for more
#   headroom.
# - BENCHMARK_BLOCKSIZE: Target square tile size (elements per side) for dense
#   suites. Defaults to "512".
# - BENCHMARK_SPARSE_BLOCKS: Number of blocks per dimension for sparse/banded
#   operators (keeps tile counts bounded at large N). Defaults to "16".
# - BENCHMARK_PROCS: Worker/thread topology, as "numprocs:numthreads". This
#   starts `numprocs` Distributed workers *inside the benchmark worker process*,
#   each with `numthreads` threads. Defaults to no extra workers (everything
#   runs on the worker process, which uses the orchestrator's thread count).
#   All workers are local; distributed/remote execution is out of scope.
# - BENCHMARK_SECONDS: Time budget (seconds) per benchmark. Defaults to "30".
# - BENCHMARK_SAMPLES: Max samples per benchmark. Defaults to "5".
# - BENCHMARK_PROC_TIMEOUT: Wall-clock seconds to wait for a single benchmark
#   before assuming the worker is wedged, killing it, and treating it like an
#   OOM (skip this and larger scales, restart). Defaults to "3600".
# - BENCHMARK_WORKDIR: Directory for the engine's per-trial files and worker
#   logs. Defaults to a fresh temp dir; set it to keep the intermediates around
#   for debugging.
# - BENCHMARK_JSON_OUTPUT: Only consulted for standalone execution (see below;
#   ignored under AirspeedVelocity). Path to write a JSON summary of results
#   to. Defaults to "benchmark_results/results_standalone.json" (both are
#   already `.gitignore`d). See `benchmark/plot.jl` for a quick way to render
#   these as plots.
#
# GPU acceleration
# ----------------
# GPU support is built into Dagger via package extensions: an accelerator just
# needs its backend package loaded ("amdgpu" -> AMDGPU, "cuda" -> CUDA,
# "opencl" -> OpenCL, "metal" -> Metal, "oneapi" -> oneAPI), which activates the
# extension and registers that backend's GPU processors. These packages are
# intentionally not hard dependencies of the benchmark environment; make the one
# you need available to AirspeedVelocity with the `--add` flag, e.g.:
#
#     BENCHMARK="linalg:dagger+amdgpu,raw+amdgpu" \
#         benchpkg Dagger --rev dirty --path . \
#             --script benchmark/benchmarks.jl -a AMDGPU
#
# (OpenCL additionally needs an ICD/driver, e.g. `-a OpenCL,pocl_jll` for a CPU
# device, or a system-installed GPU driver.)

using Dagger
using BenchmarkTools

# `common.jl` (Dagger-free config: scales, block sizes, memory helpers) is
# always safe to load; it holds no Dagger/engine dependencies.
include(joinpath(@__DIR__, "common.jl"))

# The shared benchmark engine (`Dagger.Autotune`) is a relatively new Dagger
# feature. Because AirspeedVelocity runs *this* working-tree script against each
# compared revision's *own* Dagger, a baseline revision predating the engine
# won't have it. Rather than crash the whole comparison, such a revision yields
# an empty SUITE (only benchmarks present on *both* revisions are compared) --
# the same graceful-degradation contract the op-level capability probes provide.
const HAS_ENGINE = isdefined(Dagger, :Autotune) &&
    isdefined(Dagger.Autotune, :run_sweep) &&
    isdefined(Dagger.Autotune, :BenchmarkConfig) &&
    :benchmark_only in fieldnames(Dagger.Autotune.OperationSpec)

# --- AirspeedVelocity integration ------------------------------------------
# AirspeedVelocity calls `run(SUITE)` and serializes whatever it returns. We run
# everything out-of-process at include time and wrap each measured `Trial` in a
# `PrecomputedTrial` leaf whose `run` just returns it, so AirspeedVelocity's
# `run(SUITE)`/`tune!(SUITE)` become no-ops that hand back the precomputed data.

struct PrecomputedTrial
    trial::BenchmarkTools.Trial
end
Base.run(b::PrecomputedTrial, args...; kwargs...) = b.trial
BenchmarkTools.tune!(b::PrecomputedTrial, args...; kwargs...) = b

# --- Engine configuration from the BENCHMARK_* environment -----------------

const WORKDIR = let d = get(ENV, "BENCHMARK_WORKDIR", "")
    isempty(d) ? mktempdir() : (mkpath(d); abspath(d))
end
const NTHREADS = Threads.nthreads()
const PROC_TIMEOUT = parse(Float64, get(ENV, "BENCHMARK_PROC_TIMEOUT", "3600"))

# Build `SUITE` by registering the regression suite ops (via `suite_ops.jl`) and
# running the shared engine over them. Kept in a function so it can be skipped
# wholesale on a revision whose Dagger predates the engine (see `HAS_ENGINE`);
# everything that touches `Dagger.Autotune` lives in here.
function build_suite()
    Autotune = Dagger.Autotune

    # Ops to sweep: the registered ops for each requested suite (capability
    # probes may leave a suite empty on a revision lacking a feature). These
    # globals were populated by including `suite_ops.jl` (a *separate* top-level
    # statement below, so the world age advances before this runs and the
    # enumeration/generation closures it defines are callable from here).
    selected_ops = Symbol[]
    for s in sort(collect(suites))
        append!(selected_ops, get(SUITE_OPS_BY_SUITE, s, Symbol[]))
    end

    # `@everywhere using <backend>` lines for the worker. The orchestrator itself
    # loads these accelerator packages at top level (see `load_accelerators!`
    # below) so the enumeration/`supports` gate can query each backend. Drivers
    # are best-effort so a missing optional driver doesn't abort the worker.
    accel_block = ""
    for accel in accelerations
        a = BENCH_ACCELS[accel]  # validated by `load_accelerators!`
        accel_block *= "Distributed.@everywhere using $(a.pkg)\n"
        for drv in a.drivers
            accel_block *= "try; Distributed.@everywhere using $(drv); catch; end\n"
        end
    end

    common_path = abspath(joinpath(@__DIR__, "common.jl"))
    ops_path = abspath(joinpath(@__DIR__, "suite_ops.jl"))

    # Code spliced into every worker subprocess after `Dagger.Autotune` is
    # resolved (see `Autotune._WORKER_SCRIPT_HEAD`) but before any trial runs. It
    # establishes the (local) Distributed topology inside the worker (honoring
    # BENCHMARK_PROCS), loads Dagger and any accelerator package everywhere,
    # disables runtime autotuning so the native Dagger paths are measured, and
    # registers the suite ops into the worker's registry. `\$`-escaped
    # interpolations are evaluated in the worker; the paths/accel block are
    # interpolated here.
    worker_preamble = """
    if haskey(ENV, "BENCHMARK_PROCS")
        local npnt = parse.(Int, split(ENV["BENCHMARK_PROCS"], ":"))
        npnt[1] > 0 && Distributed.addprocs(npnt[1];
            exeflags=`--project=\$(Base.active_project()) -t \$(npnt[2])`)
    end
    Distributed.@everywhere using Dagger
    $(accel_block)# Measure the native Dagger paths, never a tuned redirection.
    try; Distributed.@everywhere Dagger.Autotune.disable!(); catch; end
    # Make Dagger's eager scheduler enumerate every process in this worker.
    try; Dagger.Sch.EAGER_CONTEXT[] = Dagger.Context(); catch; end
    include(raw"$(common_path)")
    include(raw"$(ops_path)")
    """

    cfg = Autotune.BenchmarkConfig(;
        ops = selected_ops,
        # One worker process, `NTHREADS` threads (the orchestrator's thread
        # count, matching the old worker). BENCHMARK_PROCS adds Distributed
        # procs *inside* that worker via the preamble, so `nprocs=[0]` (no `-p`).
        nthreads = [NTHREADS],
        nprocs = [0],
        worker_preamble = worker_preamble,
        project = :current,
        trial_time_limit = PROC_TIMEOUT,
        sample_time = bench_seconds,
        max_samples = bench_samples,
        gcsample = true,
        memory_fraction = MEM_FRACTION,
        workdir = WORKDIR,
        # `run_sweep` performs no database I/O; this path is never written.
        db_path = joinpath(WORKDIR, "autotune_db_unused.toml"),
        verbose = true,
    )

    # Convert one `TrialResult` into a `BenchmarkTools.Trial`, preserving every
    # per-sample wall time plus the fastest sample's memory/allocs (the engine
    # doesn't capture GC time, so it's zeroed).
    to_trial = function (r)
        params = BenchmarkTools.Parameters(; samples=max(1, length(r.times_s)),
                                           seconds=Float64(bench_seconds), evals=1,
                                           gcsample=true)
        times = isempty(r.times_s) ? Float64[Float64(r.time_s) * 1e9] :
                Float64[t * 1e9 for t in r.times_s]
        gctimes = zeros(Float64, length(times))
        return BenchmarkTools.Trial(params, times, gctimes, Int(r.memory), Int(r.allocs))
    end

    suite = BenchmarkGroup()
    results = isempty(selected_ops) ? Autotune.TrialResult[] : Autotune.run_sweep(cfg)
    results === nothing && (results = Autotune.TrialResult[])
    for r in results
        r.status == "ok" || continue
        haskey(SUITE_OP_META, r.op) || continue
        meta = SUITE_OP_META[r.op]
        N = Int(r.features["n"])
        # `block` mirrors what the suite op used (a function of N/structure); it
        # is not stored as a feature (see `suite_ops.jl`).
        b = get(r.features, "structure", "dense") == "sparse" ?
            banded_block(N) : square_block(N)
        # Method label encodes the execution variant: "dagger"/"raw" for CPU,
        # "dagger+amdgpu"/"raw+cuda"/... for an accelerator backend (matching the
        # BENCHMARK spec's `method[+accel]`).
        exec = get(r.features, "exec", String(r.algorithm))
        backend = get(r.features, "backend", "cpu")
        method = backend == "cpu" ? exec : "$(exec)+$(backend)"
        suite[String[meta.suite, method, "N=$N (block $b)", meta.case]] =
            PrecomputedTrial(to_trial(r))
    end
    return suite
end

# Register the suite ops (populating the globals `build_suite` reads) as its own
# top-level statement, so the world age advances before `build_suite` runs and
# can call the enumeration/generation closures `suite_ops.jl` defines.
HAS_ENGINE && include(joinpath(@__DIR__, "suite_ops.jl"))

# Load each requested accelerator's package (which activates the corresponding
# built-in Dagger GPU extension: GPU processors, `Dagger.scope` keywords, etc.).
# This MUST run at top level -- not inside `build_suite` -- so the freshly
# loaded backend's methods (`functional`, `to_scope`, ...) are in a world age
# visible to the `supports`/functional gate that `build_suite`'s enumeration
# runs; loading them mid-function would make those calls "too new" and every GPU
# trial would be silently gated out. A backend that fails to load (or has no
# usable device) simply contributes no trials.
function load_accelerators!()
    for accel in accelerations
        a = get(BENCH_ACCELS, accel, nothing)
        a === nothing && error("Unknown acceleration: $accel " *
            "(known: $(join(sort(collect(keys(BENCH_ACCELS))), ", ")))")
        for pkg in vcat(a.pkg, a.drivers)
            try
                Base.require(Main, Symbol(pkg))
            catch err
                pkg == a.pkg &&
                    @warn "Could not load $pkg for accel '$accel'; its trials will be skipped." exception=err
            end
        end
    end
end
HAS_ENGINE && load_accelerators!()

const SUITE = if HAS_ENGINE
    build_suite()
else
    @warn "Dagger.Autotune benchmark engine not present in this Dagger revision; " *
          "producing an empty SUITE (only benchmarks available on both compared " *
          "revisions are reported)."
    BenchmarkGroup()
end

# --- Standalone execution (skipped when run under AirspeedVelocity) ---------
# AirspeedVelocity includes this file inside a module and runs SUITE itself, so
# the block below only triggers when the script is executed directly.
if abspath(PROGRAM_FILE) == @__FILE__
    import JSON3

    println()
    leaves = collect(BenchmarkTools.leaves(run(SUITE)))
    foreach(leaves) do (keys, trial)
        println(join(keys, " / "), " => ", minimum(trial))
    end

    # Also dump a JSON summary so results can be inspected/plotted later
    # without re-running anything (see `benchmark/plot.jl`).
    json_path = let p = get(ENV, "BENCHMARK_JSON_OUTPUT", "")
        isempty(p) ? joinpath("benchmark_results", "results_standalone.json") : p
    end
    let d = dirname(json_path)
        isempty(d) || mkpath(d)
    end
    entries = [
        (; keypath = String[string(k) for k in keys],
           min_time_ns = minimum(trial).time,
           mean_time_ns = BenchmarkTools.mean(trial).time,
           memory = minimum(trial).memory,
           allocs = minimum(trial).allocs,
           samples = length(trial.times))
        for (keys, trial) in leaves
    ]
    open(json_path, "w") do io
        JSON3.write(io, (; results = entries))
    end
    println("\nWrote JSON results to $(abspath(json_path))")
end
