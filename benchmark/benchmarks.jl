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
# Out-of-process execution & OOM robustness
# -----------------------------------------
# This file acts as an *orchestrator*: it never loads Dagger or runs any
# computation itself. Instead it spawns a child `julia` worker process (see
# `worker.jl`) -- a plain OS subprocess, NOT a Distributed worker, so Dagger
# does not co-opt the orchestrator as a compute process -- and drives it one
# benchmark at a time. All benchmarks run in a single worker (to amortize
# compilation) until one is killed by the OS OOM-killer or raises
# `OutOfMemoryError`. When that happens the orchestrator: records the failure,
# *skips that scale and all larger scales of the same suite/method*, starts a
# fresh worker, and resumes with the remaining benchmarks. Each benchmark's
# `BenchmarkTools.Trial` (timings, GC time, memory, allocations) is serialized
# back and injected into `SUITE`, so AirspeedVelocity sees normal results even
# though they were measured out-of-process. Benchmarks that never ran (skipped)
# are simply absent from the results.
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
#   starts `numprocs` Distributed workers *inside the benchmark worker process*,
#   each with `numthreads` threads. Defaults to no extra workers (everything
#   runs on the worker process, which uses the orchestrator's thread count).
# - BENCHMARK_REMOTES: Remote hosts on which to start workers, in the format
#   accepted by `Distributed.addprocs` (colon-separated). Optional.
# - BENCHMARK_SECONDS: Time budget (seconds) per benchmark. Defaults to "30".
# - BENCHMARK_SAMPLES: Max samples per benchmark. Defaults to "5".
# - BENCHMARK_PROC_TIMEOUT: Wall-clock seconds to wait for a single benchmark
#   before assuming the worker is wedged, killing it, and treating it like an
#   OOM (skip this and larger scales, restart). Defaults to "3600".
# - BENCHMARK_WORKDIR: Directory for the orchestrator<->worker control files and
#   per-benchmark raw result JSONs. Defaults to a fresh temp dir; set it to keep
#   the intermediates around for debugging.
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
import JSON3

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

# --- Orchestration ---------------------------------------------------------

const WORKDIR = let d = get(ENV, "BENCHMARK_WORKDIR", "")
    isempty(d) ? mktempdir() : (mkpath(d); abspath(d))
end
const WORKER_SCRIPT = get(ENV, "BENCHMARK_WORKER_SCRIPT", joinpath(@__DIR__, "worker.jl"))
const JULIA_BIN = first(Base.julia_cmd().exec)
const NTHREADS = Threads.nthreads()
const PROC_TIMEOUT = parse(Float64, get(ENV, "BENCHMARK_PROC_TIMEOUT", "3600"))
const POLL = 0.05

worker_cmd() =
    `$JULIA_BIN --project=$(Base.active_project()) --startup-file=no -t$NTHREADS $WORKER_SCRIPT $WORKDIR`

function _atomic_write(path, data)
    tmp = path * ".tmp"
    open(io -> write(io, data), tmp, "w")
    mv(tmp, path; force=true)
    return nothing
end

# Spawn a fresh worker, clearing the stale control files first. Inherits the
# orchestrator's stdout/stderr (so worker logs are visible) and environment.
function spawn_worker()
    rm(joinpath(WORKDIR, "ready"); force=true)
    rm(joinpath(WORKDIR, "request.json"); force=true)
    rm(joinpath(WORKDIR, "request.json.tmp"); force=true)
    return run(worker_cmd(); wait=false)
end

function wait_ready(proc; timeout=600)
    readypath = joinpath(WORKDIR, "ready")
    t0 = time()
    while !isfile(readypath)
        process_running(proc) || return false
        time() - t0 > timeout && return false
        sleep(POLL)
    end
    return true
end

send_request(id, action, keypath) =
    _atomic_write(joinpath(WORKDIR, "request.json"),
                  JSON3.write((; id=id, action=action, keypath=keypath)))

# Wait for a response to request `id`, or for the worker to die / time out.
# Returns the status string ("ok"/"error"/"missing"/"oom"/"died"/"timeout").
function await_response(proc, id)
    resppath = joinpath(WORKDIR, "response_$(id).json")
    t0 = time()
    while true
        if isfile(resppath)
            resp = try
                JSON3.read(read(resppath, String))
            catch
                nothing  # caught mid-write; retry
            end
            resp === nothing || return String(resp.status)
        end
        process_running(proc) || return "died"
        time() - t0 > PROC_TIMEOUT && return "timeout"
        sleep(POLL)
    end
end

function run_all_external()
    results = Dict{Vector{String},BenchmarkTools.Trial}()

    proc = spawn_worker()
    if !wait_ready(proc)
        @error "Benchmark worker failed to start; producing empty SUITE."
        return results
    end

    plan = JSON3.read(read(joinpath(WORKDIR, "plan.json"), String))
    # Process in ascending-N order within each (suite, method) so that, after an
    # OOM at some N, every remaining same-group item is at N >= the failing N and
    # can be skipped.
    items = [(; keypath=String[string(k) for k in p.keypath],
               suite=String(p.suite), method=String(p.method), N=Int(p.N))
             for p in plan]
    sort!(items; by=it -> (it.suite, it.method, it.N))

    # (suite, method) => smallest N known to OOM; that N and anything larger is skipped.
    skip_at = Dict{Tuple{String,String},Int}()
    req_id = 0

    for it in items
        thr = get(skip_at, (it.suite, it.method), typemax(Int))
        if it.N >= 0 && it.N >= thr
            @info "Skipping (>= OOM scale): $(join(it.keypath, " / "))"
            continue
        end

        req_id += 1
        send_request(req_id, "run", it.keypath)
        status = await_response(proc, req_id)

        if status == "ok"
            results[it.keypath] =
                BenchmarkTools.load(joinpath(WORKDIR, "result_$(req_id).json"))[1]
        elseif status == "missing"
            @warn "Worker has no such benchmark (capability probe differs?): $(join(it.keypath, " / "))"
        elseif status == "error"
            @warn "Benchmark errored (skipped): $(join(it.keypath, " / "))"
        else  # "oom" / "died" / "timeout"
            @warn "Benchmark $(status); skipping this and larger scales of $(it.suite)/$(it.method)" benchmark = join(it.keypath, " / ")
            if it.N >= 0
                skip_at[(it.suite, it.method)] = min(get(skip_at, (it.suite, it.method), typemax(Int)), it.N)
            end
            if process_running(proc)
                kill(proc)
                try; wait(proc); catch; end
            end
            proc = spawn_worker()
            if !wait_ready(proc)
                @error "Worker failed to restart; returning partial results."
                return results
            end
        end
    end

    req_id += 1
    send_request(req_id, "exit", String[])
    if process_running(proc)
        try; wait(proc); catch; end
    end
    return results
end

# --- Assemble SUITE from the externally-measured trials ---------------------

const SUITE = BenchmarkGroup()
for (keypath, trial) in run_all_external()
    SUITE[keypath] = PrecomputedTrial(trial)
end

# --- Standalone execution (skipped when run under AirspeedVelocity) ---------
# AirspeedVelocity includes this file inside a module and runs SUITE itself, so
# the block below only triggers when the script is executed directly.
if abspath(PROGRAM_FILE) == @__FILE__
    println()
    foreach(BenchmarkTools.leaves(run(SUITE))) do (keys, trial)
        println(join(keys, " / "), " => ", minimum(trial))
    end
end
