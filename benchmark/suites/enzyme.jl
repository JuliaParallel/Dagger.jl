# Enzyme reverse-mode AD benchmark suite (OPTIONAL).
#
# Measures `DifferentiationInterface.value_and_gradient(f, AutoEnzyme(), x)` --
# Dagger's task-graph-level reverse-mode AD -- across one representative
# operation from each supported category (copies, reductions, user maps,
# broadcasting, GEMM, GEMV, and unpivoted LU). Its purpose is to catch
# performance regressions in the AD path.
#
# Enzyme and DifferentiationInterface are heavy and are NOT part of the
# benchmark environment. Supply them at run time (like the GPU backends) and
# select this suite explicitly, e.g.:
#
#     BENCHMARK=enzyme:dagger \
#         benchpkg Dagger --rev dirty --path . \
#             --script benchmark/benchmarks.jl -a Enzyme,DifferentiationInterface
#
# A baseline revision that predates AD support is handled gracefully: the
# capability probe below skips the whole suite for that revision.

using Enzyme
using DifferentiationInterface
const _DI = DifferentiationInterface

# Load Enzyme on every worker so reverse-pass VJP kernels can run wherever
# Dagger schedules them.
@everywhere using Enzyme
@everywhere using DifferentiationInterface

# Force the entire reverse pass to complete by waiting on the gradient `DArray`.
function _force_grad(f, A)
    _, g = _DI.value_and_gradient(f, AutoEnzyme(), A)
    wait(g)
    return nothing
end

# A diagonally-dominant `DMatrix` so unpivoted LU is well-conditioned.
_dd(T, N, b) = rand(Blocks(b, b), T, N, N) .+ (T(N) .* DMatrix(I, N, N, Blocks(b, b)))

function enzyme_suite(ctx; method, accels)
    @assert method == "dagger" "Enzyme suite only supports `dagger` execution"
    accel = isempty(accels) ? "cpu" : only(accels)
    @assert accel == "cpu" "Enzyme suite only supports CPU execution"

    T = Float64
    suite = BenchmarkGroup()

    # The reverse pass keeps the tape, per-buffer shadows, and input snapshots
    # live at once, so budget generously and cap the size -- AD is much heavier
    # than a single primal evaluation.
    ad_fits(N; nmats=8) = fits_budget(dense_bytes(N; nmats=nmats, T=T)) && N <= 4096

    # Probe once that this revision supports task-graph AD at all.
    have_ad = supported("value_and_gradient(AutoEnzyme)") do
        A = rand(Blocks(4, 4), T, 8, 8); wait(A)
        _force_grad(X -> sum(abs2, X), A)
    end
    have_ad || return suite

    for N in scales
        ad_fits(N) || continue
        b = square_block(N)
        sub = BenchmarkGroup()

        sub["copy (sum)"] = @benchmarkable(_force_grad(X -> sum(copy(X)), A),
            setup = (A = rand(Blocks($b, $b), $T, $N, $N); wait(A)),
            teardown = (A = nothing; @everywhere GC.gc()))

        sub["reduction (sum abs2)"] = @benchmarkable(_force_grad(X -> sum(abs2, X), A),
            setup = (A = rand(Blocks($b, $b), $T, $N, $N); wait(A)),
            teardown = (A = nothing; @everywhere GC.gc()))

        sub["map (sin)"] = @benchmarkable(_force_grad(X -> sum(map(sin, X)), A),
            setup = (A = rand(Blocks($b, $b), $T, $N, $N); wait(A)),
            teardown = (A = nothing; @everywhere GC.gc()))

        sub["broadcast"] = @benchmarkable(_force_grad(X -> sum(X .* 2 .+ 1), A),
            setup = (A = rand(Blocks($b, $b), $T, $N, $N); wait(A)),
            teardown = (A = nothing; @everywhere GC.gc()))

        # GEMM: gradient of sum(X * B). `B` is a `DMatrix` so the tiled path runs.
        if ad_fits(N; nmats=10)
            sub["gemm (X*B)"] = @benchmarkable(_force_grad(X -> X * B, A),
                setup = (A = rand(Blocks($b, $b), $T, $N, $N);
                         B = rand(Blocks($b, $b), $T, $N, $N); wait(A); wait(B)),
                teardown = (A = nothing; B = nothing; @everywhere GC.gc()))
        end

        # GEMV: gradient of sum(X * v).
        sub["gemv (X*v)"] = @benchmarkable(_force_grad(X -> X * v, A),
            setup = (A = rand(Blocks($b, $b), $T, $N, $N);
                     v = rand(Blocks($b), $T, $N); wait(A); wait(v)),
            teardown = (A = nothing; v = nothing; @everywhere GC.gc()))

        # LU (unpivoted): gradient of sum over the factors.
        sub["lu (NoPivot)"] = @benchmarkable(_force_grad(X -> lu(X, NoPivot()), A),
            setup = (A = _dd($T, $N, $b); wait(A)),
            teardown = (A = nothing; @everywhere GC.gc()))

        isempty(sub) || (suite["N=$N (block $b)"] = sub)
    end

    suite
end

enzyme_suite
