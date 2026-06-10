# Sparse distributed linear algebra (DArray) benchmark suite.
#
# Exercises Dagger's sparse-tile operations: sparse matrix-vector multiply
# (SpMV), sparse-sparse matrix multiply (SpGEMM), and -- when `Krylov` is
# available -- a distributed iterative solve. Banded/structured operators keep
# the nonzero count ~O(N) so sizes can scale to large N without OOM.
#
# Operands are allocated inside each benchmark's `setup` (and freed in
# `teardown`) so only the running size is resident; sizes whose estimated peak
# allocation exceeds the memory budget are skipped.

@everywhere using SparseArrays

# Iterative Krylov solvers live in Dagger's KrylovExt and require `Krylov` to be
# loaded (on the driver and all workers). It is an optional dependency.
const KRYLOV_AVAILABLE = try
    @everywhere using Krylov
    true
catch err
    @warn "Krylov unavailable; skipping iterative-solver benchmarks" exception = err
    false
end

# Strongly diagonally-dominant tridiagonal SPD matrix (1-D Laplacian). The large
# diagonal keeps the condition number small so iterative solvers converge fast.
laplacian_1d(T, n) = SparseArrays.spdiagm(
    -1 => fill(-one(T), n - 1),
     0 => fill(T(4), n),
     1 => fill(-one(T), n - 1),
)

function sparse_suite(ctx; method, accels)
    @assert method == "dagger" "Sparse suite only supports `dagger` execution"
    accel = isempty(accels) ? "cpu" : only(accels)
    @assert accel == "cpu" "Sparse suite only supports CPU execution"

    T = Float64
    # Target a fixed number of nonzeros per row, so density shrinks with N and
    # the nonzero count stays ~O(N).
    nnz_per_row = 16

    suite = BenchmarkGroup()

    for N in scales
        b = banded_block(N)
        density = min(0.1, nnz_per_row / N)
        sub = BenchmarkGroup()

        # SpMV: sparse matrix tiles, dense vectors.
        if fits_budget(sparse_bytes(N; nmats=2, density=density, T=T))
            sub["spmv (S*x)"] = @benchmarkable(wait(S * x),
                setup = (S = distribute(sprand($T, $N, $N, $density), Blocks($b, $b));
                         x = distribute(rand($T, $N), Blocks($b)); wait(S)),
                teardown = (S = nothing; x = nothing; @everywhere GC.gc()))
        end

        # SpGEMM: the result fills in, so budget generously.
        if fits_budget(sparse_bytes(N; nmats=6, density=density, T=T))
            sub["spgemm (S*S)"] = @benchmarkable(wait(S * S),
                setup = (S = distribute(sprand($T, $N, $N, $density), Blocks($b, $b)); wait(S)),
                teardown = (S = nothing; @everywhere GC.gc()))
        end

        # Iterative solve of an SPD system via conjugate gradients.
        if KRYLOV_AVAILABLE && fits_budget(sparse_bytes(N; nmats=2, density=3 / N, T=T))
            sub["cg solve (laplacian)"] = @benchmarkable(wait(first(Dagger.cg(A, rhs; atol=1e-8, rtol=1e-6, itmax=200))),
                setup = (A = distribute(laplacian_1d($T, $N), Blocks($b, $b));
                         rhs = distribute(rand($T, $N), Blocks($b)); wait(A)),
                teardown = (A = nothing; rhs = nothing; @everywhere GC.gc()))
        end

        isempty(sub) || (suite["N=$N (block $b)"] = sub)
    end

    suite
end

sparse_suite
