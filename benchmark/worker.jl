# Per-run benchmark worker process.
#
# Spawned by the orchestrator (benchmarks.jl) as a plain OS subprocess -- NOT a
# Distributed worker -- so that all of Dagger's computation happens here, never
# in the orchestrator. This process loads Dagger, builds the full benchmark
# suite once (amortizing compilation), and then serves the orchestrator one
# benchmark at a time over a tiny file-based protocol rooted at the work
# directory passed as ARGS[1].
#
# Protocol (all files written atomically via tmp-then-rename):
# - On startup the worker writes `plan.json` (every leaf's key path + metadata)
#   then a `ready` sentinel.
# - The orchestrator writes `request.json` = {id, action, keypath}. `id` is
#   strictly increasing; the worker ignores ids it has already handled.
# - For action "run", the worker runs the benchmark at `keypath`, saves the
#   `BenchmarkTools.Trial` to `result_<id>.json`, and writes
#   `response_<id>.json` = {status, message}. status is one of:
#     "ok"      -> result_<id>.json holds the Trial
#     "error"   -> the benchmark threw (worker stays alive; orchestrator skips it)
#     "missing" -> no such benchmark in this worker's suite (e.g. a probe differs)
#     "oom"     -> caught OutOfMemoryError; the worker then exits so the
#                  orchestrator starts a fresh process
# - If the OS OOM-killer kills the worker mid-run, no response appears and the
#   orchestrator detects the dead process directly.
# - For action "exit", the worker exits cleanly.

using BenchmarkTools
using Distributed
using Dates, Random, Statistics, LinearAlgebra, InteractiveUtils
import JSON3

include(joinpath(@__DIR__, "common.jl"))

const WORKDIR = abspath(ARGS[1])

function atomic_write(path, data)
    tmp = path * ".tmp"
    open(io -> write(io, data), tmp, "w")
    mv(tmp, path; force=true)
    return nothing
end

# --- Set up the Distributed topology (within this worker only) -------------
# Honors BENCHMARK_PROCS/BENCHMARK_REMOTES exactly as before, but the processes
# are children of *this* worker, not of the orchestrator.

if haskey(ENV, "BENCHMARK_PROCS")
    const np, nt = parse.(Ref(Int), split(ENV["BENCHMARK_PROCS"], ":"))
else
    const np = 0
    const nt = 1
end

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

# Don't use the autotuner, we want to measure Dagger's algorithms specifically
@everywhere Dagger.Autotune.disable!()

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
        @info "[worker] Creating benchmarks for suite=$suite_name method=$method_key"
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

# --- Enumerate the plan and advertise it -----------------------------------

# Parse the matrix dimension N out of a suite group key like "N=4096 (block 512)".
function _parse_N(key::AbstractString)
    m = match(r"^N=(\d+)", key)
    return m === nothing ? -1 : parse(Int, m.captures[1])
end

const BENCH_BY_PATH = Dict{Vector{String},Any}()
let plan = Vector{Any}()
    for (keypath, bench) in BenchmarkTools.leaves(SUITE)
        kp = String[string(k) for k in keypath]
        BENCH_BY_PATH[kp] = bench
        suite = length(kp) >= 1 ? kp[1] : ""
        method = length(kp) >= 2 ? kp[2] : ""
        N = length(kp) >= 3 ? _parse_N(kp[3]) : -1
        push!(plan, (; keypath=kp, suite=suite, method=method, N=N))
    end
    atomic_write(joinpath(WORKDIR, "plan.json"), JSON3.write(plan))
end
atomic_write(joinpath(WORKDIR, "ready"), "1")
@info "[worker] Ready; $(length(BENCH_BY_PATH)) benchmark(s) available."

# --- Serve benchmarks one at a time ----------------------------------------
# Kept in a function so loop variables have ordinary local scope (a top-level
# `while` would put them in soft scope, silently breaking `last_id`).

function serve(bench_by_path, workdir)
    reqpath = joinpath(workdir, "request.json")
    last_id = 0
    while true
        if !isfile(reqpath)
            sleep(0.05)
            continue
        end
        req = try
            JSON3.read(read(reqpath, String))
        catch
            sleep(0.02)  # likely caught mid-write; retry
            continue
        end
        id = Int(req.id)
        if id <= last_id
            sleep(0.05)
            continue
        end
        last_id = id

        if String(req.action) == "exit"
            @info "[worker] Exiting."
            return
        end

        kp = String[string(k) for k in req.keypath]
        resppath = joinpath(workdir, "response_$(id).json")

        if !haskey(bench_by_path, kp)
            atomic_write(resppath, JSON3.write((; status="missing")))
            continue
        end

        bench = bench_by_path[kp]
        @info "[worker] Running: $(join(kp, " / "))"
        try
            trial = BenchmarkTools.run(bench)
            BenchmarkTools.save(joinpath(workdir, "result_$(id).json"), trial)
            atomic_write(resppath, JSON3.write((; status="ok")))
        catch err
            if err isa OutOfMemoryError
                @warn "[worker] OutOfMemoryError; exiting for a fresh process" benchmark = join(kp, " / ")
                try
                    atomic_write(resppath, JSON3.write((; status="oom", message=sprint(showerror, err))))
                catch
                end
                flush(stdout); flush(stderr)
                exit(137)
            else
                @warn "[worker] Benchmark errored" benchmark = join(kp, " / ") exception = (err, catch_backtrace())
                atomic_write(resppath, JSON3.write((; status="error", message=sprint(showerror, err))))
            end
        end
    end
end

serve(BENCH_BY_PATH, WORKDIR)
