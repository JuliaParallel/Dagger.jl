# Steady-state allocation regression tests.
#
# Dagger's scheduler and datadeps hot paths have been heavily optimized to
# minimize per-task allocations (object pooling, reusable tasks, typed
# argument storage, etc.). These tests guard those gains: each key operation
# is deeply warmed up to reach allocation steady state, then measured several
# times, and the *minimum* allocation count/bytes across measurement runs is
# checked against an upper bound.
#
# Bounds are set at roughly 2x the steady state measured at the time of
# writing (Julia 1.12, x86_64-linux), so they are robust to run-to-run
# scheduling variance, Julia version differences, and platform differences,
# while still catching order-of-magnitude regressions (an accidental
# reintroduction of per-task boxing or object churn typically costs 3-10x).
# If an intentional change shifts steady state, re-measure and update the
# bounds table below (the suite prints a measured-vs-bound table on each run).
#
# All operations are pinned to a single processor (worker 1, thread 1) so the
# measured counts do not depend on the worker/thread topology the testsuite
# happens to run under. Measurement uses process-global GC counters, so
# allocations from Dagger's background tasks are (correctly) included.

using LinearAlgebra
using FFTW
import Dagger: Blocks, @stencil, Wrap

# Allow downstream/platform-specific loosening without editing the table,
# e.g. DAGGER_ALLOCATIONS_TEST_MULTIPLIER=2 for an exotic platform.
const ALLOC_BOUND_MULTIPLIER =
    parse(Float64, get(ENV, "DAGGER_ALLOCATIONS_TEST_MULTIPLIER", "1.0"))

"""
    measure_steady_state_allocs(f; warmup=10, runs=5) -> (allocs, bytes)

Run `f()` `warmup` times (reaching compilation + pool/cache steady state),
then measure `runs` further calls and return the minimum GC allocation count
and allocated bytes observed for a single call. The minimum (not mean) is
used so that one-off noise (lazily-compiled async paths, incidental GC
activity from other tasks) doesn't flake the bound check; a real regression
raises every run, including the minimum.
"""
function measure_steady_state_allocs(f; warmup=10, runs=5)
    for _ in 1:warmup
        f()
    end
    GC.gc()
    best_allocs = typemax(Int)
    best_bytes = typemax(Int)
    for _ in 1:runs
        before = Base.gc_num()
        f()
        diff = Base.GC_Diff(Base.gc_num(), before)
        best_allocs = min(best_allocs, Base.gc_alloc_count(diff))
        best_bytes = min(best_bytes, Int(diff.allocd))
    end
    return (allocs=best_allocs, bytes=best_bytes)
end

# name => (; allocs, bytes) upper bounds (see header for how these are set).
# Measured steady-state values at the time of writing are noted inline.
const ALLOC_BOUNDS = Dict(
    #                                                  measured 2026-08 (min, Julia 1.12):
    "task spawn"           => (allocs=     500, bytes=     24_000), #     236 /   9.6 KiB
    "datadeps: empty"      => (allocs=     150, bytes=      8_000), #      51 /   2.3 KiB
    "matmul! (in-place)"   => (allocs=  60_000, bytes=  2_500_000), #  28,597 /  1.18 MiB
    "matmul (out-of-place)"=> (allocs=  70_000, bytes=  3_200_000), #  33,060 /  1.49 MiB
    "cholesky + solve"     => (allocs= 320_000, bytes= 15_000_000), # 159,214 /  7.15 MiB
    "stencil"              => (allocs=  50_000, bytes=  2_200_000), #  24,978 /  1.03 MiB
    "fft!"                 => (allocs=  60_000, bytes=  3_700_000), #  29,058 /  1.75 MiB
    "broadcast"            => (allocs=  40_000, bytes=  1_900_000), #  18,139 / 881.6 KiB
    "reduce (sum)"         => (allocs=  30_000, bytes=  1_300_000), #  14,572 / 581.0 KiB
)

function alloc_bound(name)
    b = ALLOC_BOUNDS[name]
    return (allocs=ceil(Int, b.allocs * ALLOC_BOUND_MULTIPLIER),
            bytes=ceil(Int, b.bytes * ALLOC_BOUND_MULTIPLIER))
end

# Pin all work to one processor so results are topology-independent.
const ALLOC_TEST_SCOPE = Dagger.scope(worker=1, thread=1)

fmt_bytes(b) = b < 1024 ? "$(b) B" :
               b < 1024^2 ? string(round(b / 1024; digits=1), " KiB") :
               string(round(b / 1024^2; digits=2), " MiB")

const ALLOC_RESULTS = Vector{Tuple{String,Int,Int,Int,Int}}()

function test_allocs(f, name; warmup=10, runs=5)
    bound = alloc_bound(name)
    measured = Dagger.with_options(; scope=ALLOC_TEST_SCOPE) do
        measure_steady_state_allocs(f; warmup, runs)
    end
    push!(ALLOC_RESULTS, (name, measured.allocs, bound.allocs,
                          measured.bytes, bound.bytes))
    @testset "$name" begin
        @test measured.allocs <= bound.allocs
        @test measured.bytes <= bound.bytes
    end
end

function print_alloc_report()
    namew = maximum(length(r[1]) for r in ALLOC_RESULTS) + 2
    println()
    println("Steady-state allocations per call (minimum of measured runs):")
    println(rpad("operation", namew), " | ",
            lpad("allocs", 10), " / ", lpad("bound", 10), " | ",
            lpad("bytes", 12), " / ", lpad("bound", 12))
    println(repeat("-", namew + 60))
    for (name, allocs, allocs_bound, bytes, bytes_bound) in ALLOC_RESULTS
        println(rpad(name, namew), " | ",
                lpad(string(allocs), 10), " / ", lpad(string(allocs_bound), 10), " | ",
                lpad(fmt_bytes(bytes), 12), " / ", lpad(fmt_bytes(bytes_bound), 12))
    end
    println()
end

@testset "Steady-state allocations" begin
    N, B = 128, 32
    T = Float64

    # Single eager task round-trip: the core scheduler path.
    test_allocs("task spawn"; warmup=20, runs=10) do
        fetch(Dagger.@spawn 1+1)
    end

    # Empty datadeps region: planner + region setup/teardown overhead.
    test_allocs("datadeps: empty"; warmup=20, runs=10) do
        Dagger.spawn_datadeps(() -> nothing)
    end

    # In-place tiled matmul: the flagship datadeps benchmark (4x4 grid of
    # 32x32 blocks => 80 tasks/call). Pure scheduling+execution overhead; no
    # array data is allocated in steady state.
    let A = rand(Blocks(B, B), T, N, N),
        C = zeros(Blocks(B, B), T, N, N)
        wait(A); wait(C)
        test_allocs("matmul! (in-place)") do
            mul!(C, A, A)
        end
    end

    # Out-of-place matmul additionally allocates the result DArray each call
    # (and GC-frees the previous one), so its bounds include the data itself.
    let A = rand(Blocks(B, B), T, N, N)
        wait(A)
        test_allocs("matmul (out-of-place)") do
            wait(A * A)
        end
    end

    # Cholesky factorization + triangular solve (both allocate fresh outputs).
    let G = rand(Blocks(B, B), T, N, N),
        A = G * G',  # SPD almost surely
        b = rand(Blocks(B), T, N)
        wait(A); wait(b)
        test_allocs("cholesky + solve") do
            wait(cholesky(A) \ b)
        end
    end

    # 9-point stencil with Wrap boundary: exercises the aliasing/neighborhood
    # machinery in the datadeps planner.
    let A = ones(Blocks(B, B), T, N, N),
        S = zeros(Blocks(B, B), T, N, N)
        wait(A); wait(S)
        test_allocs("stencil") do
            @stencil S[idx] = sum(@neighbors(A[idx], 1, Wrap()))
        end
    end

    # In-place 2D FFT (pencil decomposition, via the AbstractFFTs extension).
    let A = rand(Blocks(B, B), ComplexF64, N, N)
        wait(A)
        test_allocs("fft!") do
            fft!(A)
        end
    end

    # In-place broadcast over three DArrays.
    let A = rand(Blocks(B, B), T, N, N),
        Bm = rand(Blocks(B, B), T, N, N),
        C = zeros(Blocks(B, B), T, N, N)
        wait(A); wait(Bm); wait(C)
        test_allocs("broadcast") do
            C .= A .+ Bm
        end
    end

    # Full reduction to a scalar.
    let A = rand(Blocks(B, B), T, N, N)
        wait(A)
        test_allocs("reduce (sum)") do
            sum(A)
        end
    end

    print_alloc_report()
end

# Return the process to a virgin eager-scheduler state: this suite runs first
# in the canonical order (its measurements need a pristine process — see
# runtests.jl), and later suites assert lazy scheduler initialization
# (test/thunk.jl checks `EAGER_CONTEXT[] === nothing` before the first
# `@spawn`). `cancel!(;halt_sch=true)` halts the scheduler and waits for it to
# exit (clearing `EAGER_STATE`/`EAGER_INIT`); the context ref is ours to reset.
Dagger.cancel!(;halt_sch=true)
Dagger.Sch.EAGER_CONTEXT[] = nothing
