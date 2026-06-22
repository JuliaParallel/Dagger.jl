# Dense distributed linear algebra (DArray) benchmark suite.
#
# Covers the BLAS-3 / LAPACK-style operations Dagger implements over `DMatrix`:
# matrix-matrix and matrix-vector products, symmetric rank-k, and the Cholesky,
# LU and QR factorizations plus a full linear solve.
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
    suite = BenchmarkGroup()

    for N in scales
        b = square_block(N)
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
                         b = rand(Blocks($b), $T, $N); wait(A)),
                teardown = (A = nothing; b = nothing; @everywhere GC.gc()))
        end

        # Cholesky additionally holds the SPD-construction temporary.
        if fits_budget(dense_bytes(N; nmats=4, T=T))
            sub["cholesky"] = @benchmarkable(wait(cholesky(A).factors),
                setup = (A = _spd($T, $N, $b)),
                teardown = (A = nothing; @everywhere GC.gc()))
        end

        # gemv is cheap (one matrix + two vectors).
        if fits_budget(dense_bytes(N; nmats=1, T=T))
            sub["matvec (A*x)"] = @benchmarkable(wait(A * x),
                setup = (A = rand(Blocks($b, $b), $T, $N, $N);
                         x = rand(Blocks($b), $T, $N); wait(A)),
                teardown = (A = nothing; x = nothing; @everywhere GC.gc()))
        end

        isempty(sub) || (suite["N=$N (block $b)"] = sub)
    end

    suite
end

linalg_suite
