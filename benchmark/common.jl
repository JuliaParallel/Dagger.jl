# Shared configuration for the Dagger benchmark suites.
#
# Included by BOTH the orchestrator (benchmarks.jl) and each per-run worker
# (worker.jl). This file must NOT load Dagger or start any Distributed/Dagger
# processes: keeping it Dagger-free is what lets the orchestrator coordinate
# benchmarks without itself becoming a Dagger compute process, so an
# out-of-memory benchmark can only take down a worker, never the orchestrator
# that holds the collected results.

# --- Parse the benchmark specification -------------------------------------

const benches = Dict{String,Vector}()
const suites = Set{String}()
const accelerations = Set{String}()
for bench_spec in split(get(ENV, "BENCHMARK", "array:dagger;linalg:dagger;sparse:dagger;stencil:dagger"), ';')
    parts = split(bench_spec, ':')
    suite = parts[1]
    bench_spec_methods = length(parts) >= 2 ? parts[2] : "dagger"
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

"""Probe whether an operation is supported by the Dagger revision under test.

Runs `f()` once (discarding the result) and returns `true` if it completes, or
`false` (with a warning) if it throws. Because AirspeedVelocity runs this same
script across multiple revisions, a baseline revision may lack a feature the
current branch adds; suites use this to skip benchmarks that revision can't run
rather than aborting the whole comparison. Capability does not depend on N, so
probe once (at a tiny size) and reuse the result across scales."""
function supported(f, label)
    try
        f()
        return true
    catch err
        @warn "Skipping unsupported benchmark(s): $label" exception = (err, catch_backtrace())
        return false
    end
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
