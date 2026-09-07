# Full-SPMD, single-shot MPI benchmark worker.
#
# Spawned by the orchestrator (benchmarks.jl) via `mpiexec -n N julia
# worker_mpi.jl workdir` when BENCHMARK_MPI_RANKS is set. Unlike worker.jl (a
# plain OS subprocess driven one benchmark at a time over a file-based
# request/response protocol), MPI ranks are SPMD: every rank must call the
# same collective Dagger operations at the same time, so there is no
# orchestrator-in-the-loop per benchmark here. Instead every rank builds the
# identical benchmark suite (mirroring test/mpi.jl's bootstrap) and runs a
# single flat pass over every benchmark in lockstep; rank 0 alone talks to
# the filesystem, recording each Trial and finishing with a manifest plus a
# `done` sentinel that the orchestrator polls for.
#
# Protocol (rank 0 only, all writes atomic via tmp-then-rename):
# - `result_mpi_<i>.json`: a BenchmarkTools.save'd Trial for the i'th leaf
#   that completed (in sorted-keypath order).
# - `results_mpi_manifest.json`: [{keypath, file}, ...] pairing each
#   completed leaf's key path to its result file.
# - `done`: written last, once every benchmark has been attempted.
#
# Note this worker does not have worker.jl's per-scale OOM isolation: a
# caught `OutOfMemoryError` aborts the *whole* MPI job (`MPI.Abort`) rather
# than exiting only the local rank and letting the orchestrator retry smaller
# scales, since a partially-alive rank set can't make collective progress.
# Other exceptions are caught per-rank and just skip that one benchmark:
# since every rank runs the identical deterministic computation, ranks are
# expected to fail together at the same call, so no cross-rank coordination
# is needed for the common case.

using BenchmarkTools
using Distributed
using Dates, Random, Statistics, LinearAlgebra, InteractiveUtils
import JSON3
using MPI

include(joinpath(@__DIR__, "common.jl"))

const WORKDIR = abspath(ARGS[1])

function atomic_write(path, data)
    tmp = path * ".tmp"
    open(io -> write(io, data), tmp, "w")
    mv(tmp, path; force=true)
    return nothing
end

using Dagger
Dagger.accelerate!(:mpi)
Dagger.check_uniformity!(true)
const comm = MPI.COMM_WORLD
const rank = MPI.Comm_rank(comm)

# --- Load acceleration backends (only if requested) -------------------------
# No Distributed workers exist under MPI (each rank is its own OS process),
# so a plain `using` suffices here (worker.jl uses `@everywhere using` because
# it may have addprocs'd extra Distributed workers).

for accel in accelerations
    if accel == "cuda"
        try
            using DaggerGPU, CUDA
        catch err
            error("Failed to load CUDA acceleration; ensure DaggerGPU and CUDA " *
                  "are available (e.g. `benchpkg ... -a DaggerGPU,CUDA`)\n$err")
        end
    elseif accel == "amdgpu"
        try
            using DaggerGPU, AMDGPU
        catch err
            error("Failed to load AMDGPU acceleration; ensure DaggerGPU and " *
                  "AMDGPU are available (e.g. `benchpkg ... -a DaggerGPU,AMDGPU`)\n$err")
        end
    else
        error("Unknown acceleration: $accel")
    end
end

# --- Build the benchmark suites ---------------------------------------------
# Every rank builds the identical suite (deterministic; no per-rank
# branching), so BenchmarkTools.leaves(SUITE) enumerates the same benchmarks
# everywhere. `ctx` is accepted-but-unused by every suite (verified: array,
# linalg, sparse, stencil), so it's passed as `nothing` here rather than a
# Distributed-flavored `Context()` -- test/mpi.jl doesn't set
# `Dagger.Sch.EAGER_CONTEXT[]` either, and doing so here would risk pinning
# the scheduler to a single-process view instead of the MPI-aware processor
# set that `Dagger.accelerate!(:mpi)` establishes.

const suite_setup = Dict{String,Function}()
for suite in suites
    suite_setup[suite] = include(joinpath(@__DIR__, "suites", suite * ".jl"))
end

const SUITE = BenchmarkGroup()
for (suite_name, bench_list) in benches
    suite_group = BenchmarkGroup()
    for bench in bench_list
        method_key = isempty(bench.accels) ? bench.method :
                     "$(bench.method)+$(join(bench.accels, "+"))"
        rank == 0 && @info "[worker_mpi] Creating benchmarks for suite=$suite_name method=$method_key"
        suite_group[method_key] =
            suite_setup[suite_name](nothing; method=bench.method, accels=bench.accels)
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

# --- Run every benchmark once, identically on every rank --------------------
# Sorted by keypath (not Dict/BenchmarkGroup insertion order) so ranks agree
# even if suite construction were ever to introduce nondeterministic
# iteration order.

leaves = sort(collect(BenchmarkTools.leaves(SUITE)); by = kv -> String[string(k) for k in kv[1]])

const results = Vector{Tuple{Vector{String},BenchmarkTools.Trial}}()  # rank 0 only

for (keypath, bench) in leaves
    kp = String[string(k) for k in keypath]
    rank == 0 && @info "[worker_mpi] Running: $(join(kp, " / "))"
    try
        trial = BenchmarkTools.run(bench)
        rank == 0 && push!(results, (kp, trial))
    catch err
        if err isa OutOfMemoryError
            rank == 0 && @warn "[worker_mpi] OutOfMemoryError; aborting MPI job" benchmark = join(kp, " / ")
            flush(stdout); flush(stderr)
            MPI.Abort(comm, 137)
            exit(137)  # unreachable unless MPI.Abort fails to terminate us
        else
            rank == 0 && @warn "[worker_mpi] Benchmark errored (skipped)" benchmark = join(kp, " / ") exception = (err, catch_backtrace())
        end
    end
end

# --- Rank 0: persist results and advertise completion -----------------------

if rank == 0
    manifest = Vector{Any}()
    for (i, (kp, trial)) in enumerate(results)
        fname = "result_mpi_$(i).json"
        BenchmarkTools.save(joinpath(WORKDIR, fname), trial)
        push!(manifest, (; keypath=kp, file=fname))
    end
    atomic_write(joinpath(WORKDIR, "results_mpi_manifest.json"), JSON3.write(manifest))
    atomic_write(joinpath(WORKDIR, "done"), "1")
    @info "[worker_mpi] Done; $(length(results))/$(length(leaves)) benchmark(s) succeeded."
end
