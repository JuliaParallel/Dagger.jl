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
# `sparse`, or `DTables,CSV,Arrow,OnlineStats,MemPool` for the legacy `dtable`).
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
#     - "dtable" : DTables data operations (legacy; opt-in, see below)
#   Available methods: "raw" (non-Dagger), "dagger" (Dagger).
#   Available accelerations: "cuda", "amdgpu" (require the relevant packages,
#   see below). Defaults to "array:dagger;linalg:dagger;sparse:dagger".
#
#   "dtable" is intentionally NOT part of the default set (DTables is not
#   actively developed). It remains available when explicitly requested.
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
#   starts `numprocs` Distributed workers, each with `numthreads` threads.
#   Defaults to no extra workers (everything runs on the driver process). Note
#   that the driver process's own thread count is controlled by Julia's `-t`
#   flag (pass it via `benchpkg -e "-t N"`).
# - BENCHMARK_REMOTES: Remote hosts on which to start workers, in the format
#   accepted by `Distributed.addprocs` (colon-separated). Optional.
# - BENCHMARK_SECONDS: Time budget (seconds) per benchmark. Defaults to "30".
# - BENCHMARK_SAMPLES: Max samples per benchmark. Defaults to "5".
# - BENCHMARK_DTABLE_ROWS: Row count for the legacy "dtable" suite only.
#   Defaults to "1000000".
#
# GPU acceleration
# ----------------
# The "cuda"/"amdgpu" accelerations require `DaggerGPU` plus `CUDA`/`AMDGPU`.
# These are intentionally not hard dependencies of the benchmark environment.
# Make them available to AirspeedVelocity with the `--add` flag, e.g.:
#
#     BENCHMARK=linalg:dagger+cuda \
#         benchpkg Dagger --rev dirty --path . \
#             --script benchmark/benchmarks.jl -a DaggerGPU,CUDA

using BenchmarkTools
using Distributed
using Dates, Random, Statistics, LinearAlgebra, InteractiveUtils

# --- Parse the benchmark specification -------------------------------------

const benches = Dict{String,Vector}()
const suites = Set{String}()
const accelerations = Set{String}()
for bench_spec in split(get(ENV, "BENCHMARK", "array:dagger;linalg:dagger;sparse:dagger"), ';')
    suite, bench_spec_methods = split(bench_spec, ':')
    if !isfile(joinpath(@__DIR__, "suites", suite * ".jl"))
        error("Unknown benchmark suite: $suite")
    end
    push!(suites, suite)
    for method_spec in split(bench_spec_methods, ',')
        method, accels... = split(method_spec, '+')
        for accel in accels
            push!(accelerations, accel)
        end
        accels = String.(accels)
        _benches = get!(benches, suite, [])
        push!(_benches, (; method, accels))
    end
end

# --- Set up the Distributed topology ---------------------------------------

if haskey(ENV, "BENCHMARK_PROCS")
    const np, nt = parse.(Ref(Int), split(ENV["BENCHMARK_PROCS"], ":"))
else
    const np = 0
    const nt = 1
end

# Workers must share the active (AirspeedVelocity-provided) project so that
# Dagger and the suite dependencies are available on them.
const worker_exeflags = `--project=$(Base.active_project()) -t $nt`
if np > 0
    addprocs(np; exeflags=worker_exeflags)
end
if haskey(ENV, "BENCHMARK_REMOTES")
    const remotes = split(ENV["BENCHMARK_REMOTES"], ":")
    if !isempty(remotes)
        addprocs(remotes; exeflags=worker_exeflags)
    end
end

@everywhere using Dagger

# --- Load acceleration backends (only if requested) ------------------------

for accel in accelerations
    if accel == "cuda"
        try
            @everywhere using DaggerGPU, CUDA
        catch err
            error("Failed to load CUDA acceleration; ensure DaggerGPU and CUDA " *
                  "are available (e.g. `benchpkg ... -a DaggerGPU,CUDA`)\n$err")
        end
    elseif accel == "amdgpu"
        try
            @everywhere using DaggerGPU, AMDGPU
        catch err
            error("Failed to load AMDGPU acceleration; ensure DaggerGPU and " *
                  "AMDGPU are available (e.g. `benchpkg ... -a DaggerGPU,AMDGPU`)\n$err")
        end
    else
        error("Unknown acceleration: $accel")
    end
end

# --- Shared configuration consumed by the suite files ----------------------

# Problem sizes (matrix dimension N). Default: a log-spaced ladder from 100 to
# 100_000. Suites individually skip sizes that don't fit the memory budget, so
# the effective set adapts to the machine (see `fits_budget`).
const scales = let s = get(ENV, "BENCHMARK_SCALE", "")
    if isempty(s)
        [100, 316, 1_000, 3_162, 10_000, 31_623, 100_000]
    else
        parsed = eval(Meta.parse(s))
        parsed isa Integer ? [parsed] : collect(parsed)
    end
end

# Conservative memory budget: a single benchmark's estimated peak allocation may
# use at most this many bytes. We size it from *total* system RAM rather than
# `Sys.free_memory()`, which on macOS (and with filesystem cache on Linux)
# reports only truly-free pages and would spuriously skip almost everything.
const MEM_FRACTION = parse(Float64, get(ENV, "BENCHMARK_MEM_FRACTION", "0.2"))
const MEM_BUDGET = floor(Int, MEM_FRACTION * Sys.total_memory())

"""Estimated bytes for `nmats` dense N×N matrices of element type `T`."""
dense_bytes(N; nmats=1, T=Float64) = nmats * Int(N)^2 * sizeof(T)

"""Estimated bytes for `nmats` sparse N×N matrices with the given `density`
(plus the dense N-vectors a typical kernel keeps live)."""
sparse_bytes(N; nmats=1, density=0.0, T=Float64, nvecs=2) =
    nmats * (round(Int, density * Int(N)^2) * (sizeof(T) + sizeof(Int)) + Int(N) * sizeof(Int)) +
    nvecs * Int(N) * sizeof(T)

"""Whether an estimated allocation of `bytes` fits the conservative budget."""
fits_budget(bytes) = bytes <= MEM_BUDGET

"""Square tile size (elements per side) for a dense N×N matrix, targeting
`tile` elements per side but never exceeding N."""
function square_block(N; tile=parse(Int, get(ENV, "BENCHMARK_BLOCKSIZE", "512")))
    N <= tile && return N
    return cld(N, cld(N, tile))
end

"""Block size for sparse/banded N×N operators, keeping the per-dimension block
count bounded (so we don't create an enormous number of mostly-empty tiles)."""
function banded_block(N; maxblocks=parse(Int, get(ENV, "BENCHMARK_SPARSE_BLOCKS", "16")))
    return cld(N, min(N, maxblocks))
end

const bench_seconds = parse(Float64, get(ENV, "BENCHMARK_SECONDS", "30"))
const bench_samples = parse(Int, get(ENV, "BENCHMARK_SAMPLES", "5"))

# Rendering/logging are not used under AirspeedVelocity; these globals are kept
# defined because the suite files reference them.
const render = ""
const live = false
const profile = false
const savelogs = false
const RENDERS = Dict{Int,Dict}()

# --- Build the benchmark suites --------------------------------------------

const suite_setup = Dict{String,Function}()
for suite in suites
    suite_setup[suite] = include(joinpath(@__DIR__, "suites", suite * ".jl"))
end

const ctx = Context()  # enumerates all live processes (driver + workers)
Dagger.Sch.EAGER_CONTEXT[] = ctx

const SUITE = BenchmarkGroup()
for (suite_name, bench_list) in benches
    suite_group = BenchmarkGroup()
    for bench in bench_list
        method_key = isempty(bench.accels) ? bench.method :
                     "$(bench.method)+$(join(bench.accels, "+"))"
        @info "Creating benchmarks for suite=$suite_name method=$method_key"
        suite_group[method_key] =
            suite_setup[suite_name](ctx; method=bench.method, accels=bench.accels)
    end
    SUITE[suite_name] = suite_group
end

# Apply consistent run parameters to every benchmark in the tree. `evals=1` is
# required because the suites use `setup`/`teardown`.
for (_, b) in BenchmarkTools.leaves(SUITE)
    b.params.seconds = bench_seconds
    b.params.samples = bench_samples
    b.params.evals = 1
    b.params.gcsample = true
end

# --- Standalone execution (skipped when run under AirspeedVelocity) ---------
# AirspeedVelocity includes this file inside a module and runs SUITE itself, so
# the block below only triggers when the script is executed directly.
if abspath(PROGRAM_FILE) == @__FILE__
    results = BenchmarkTools.run(SUITE; verbose=true)
    println()
    foreach(BenchmarkTools.leaves(results)) do (keys, trial)
        println(join(keys, " / "), " => ", minimum(trial))
    end
end
