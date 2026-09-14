# Dense distributed linear algebra (DArray) benchmark suite.
#
# Covers the BLAS-3 / LAPACK-style operations Dagger implements over `DMatrix`:
# matrix-matrix and matrix-vector products, symmetric rank-k, the Cholesky,
# LU, QR and (Jacobi) SVD factorizations, plus a full linear solve.
#
# Square matrices with a square block grid are used throughout so that
# transposed products (`A' * A`) and the tile factorizations are well-formed.
# Operands are allocated inside each benchmark's `setup` (and freed in
# `teardown`) so that only the currently-running size is resident in memory;
# sizes whose estimated peak allocation exceeds the memory budget are skipped.

# Build a distributed symmetric positive-definite matrix (for Cholesky). `G*G'`
# is PD almost surely for a full-rank square `G`.
_spd(T, N, b) = (G = rand(Blocks(b, b), T, N, N); A = G * G'; wait(A); A)

function linalg_suite(ctx; method, accels)
    @assert method == "dagger" "Linalg suite only supports `dagger` execution"
    accel = isempty(accels) ? "cpu" : only(accels)
    @assert accel == "cpu" "Linalg suite only supports CPU execution"

    T = Float64
    # Some older Dagger revisions use a Distributed-only processor grid for
    # tiled SVD. Under MPI that grid is empty and `_tile_index` divides by zero.
    # This script is shared by both Airspeed revisions, so probe once and omit
    # SVD where the revision/backend combination cannot execute it.
    svd_ok = supported("linalg/svd") do
        A = rand(Blocks(4, 4), T, 8, 8)
        wait(A)
        wait(svd(A).U)
    end
    suite = BenchmarkGroup()

    for N in scales
        for b in blocks_for(N)
            sub = BenchmarkGroup()

            # gemm needs A and the result resident; factorizations copy internally.
            if fits_budget(dense_bytes(N; nmats=3, T=T))
                sub["matmul (A*A)"] = @benchmarkable(wait(A * A),
                    setup = (A = rand(Blocks($b, $b), $T, $N, $N); wait(A)),
                    teardown = (A = nothing; @everywhere GC.gc()))

                sub["syrk (A'*A)"] = @benchmarkable(wait(A' * A),
                    setup = (A = rand(Blocks($b, $b), $T, $N, $N); wait(A)),
                    teardown = (A = nothing; @everywhere GC.gc()))

                sub["lu"] = @benchmarkable(wait(lu(A, RowMaximum()).factors),
                    setup = (A = rand(Blocks($b, $b), $T, $N, $N); wait(A)),
                    teardown = (A = nothing; @everywhere GC.gc()))

                sub["qr"] = @benchmarkable(wait(qr(A).factors),
                    setup = (A = rand(Blocks($b, $b), $T, $N, $N); wait(A)),
                    teardown = (A = nothing; @everywhere GC.gc()))

                sub["solve (A\\b via lu)"] = @benchmarkable(wait(lu(A, RowMaximum()) \ b),
                    setup = (A = rand(Blocks($b, $b), $T, $N, $N);
                             b = rand(Blocks($b), $T, $N); wait(A); wait(b)),
                    teardown = (A = nothing; b = nothing; @everywhere GC.gc()))
            end

            # Cholesky additionally holds the SPD-construction temporary.
            if fits_budget(dense_bytes(N; nmats=4, T=T))
                sub["cholesky"] = @benchmarkable(wait(cholesky(A).factors),
                    setup = (A = _spd($T, $N, $b)),
                    teardown = (A = nothing; @everywhere GC.gc()))
            end

            # SVD (tiled one-sided Jacobi) additionally holds the internally-copied
            # scratch matrix, the accumulated V factor, and (across multiple
            # workers) a restaged copy of A, on top of the resident input.
            if svd_ok && fits_budget(dense_bytes(N; nmats=5, T=T))
                sub["svd"] = @benchmarkable(wait(svd(A).U),
                    setup = (A = rand(Blocks($b, $b), $T, $N, $N); wait(A)),
                    teardown = (A = nothing; @everywhere GC.gc()))
            end

            # gemv is cheap (one matrix + two vectors).
            if fits_budget(dense_bytes(N; nmats=1, T=T))
                sub["matvec (A*x)"] = @benchmarkable(wait(A * x),
                    setup = (A = rand(Blocks($b, $b), $T, $N, $N);
                             x = rand(Blocks($b), $T, $N); wait(A); wait(x)),
                    teardown = (A = nothing; x = nothing; @everywhere GC.gc()))
            end

            isempty(sub) || (suite["N=$N (block $b)"] = sub)
        end
    end

    suite
end

linalg_suite
