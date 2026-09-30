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
function _spd(T, N, b; assignment=:arbitrary)
    G = rand(Blocks(b, b), T, N, N; assignment)
    wait(G)
    A = if assignment === :arbitrary
        G * G'
    else
        # G's assignment does not propagate through similar(G). Allocate the
        # SPD destination explicitly too, so Cholesky's input layout is fixed.
        result = DArray{T}(undef, Blocks(b, b), N, N; assignment)
        wait(result)
        mul!(result, G, G')
        result
    end
    wait(A)
    return A
end

function linalg_suite(ctx; method, accels)
    @assert method == "dagger" "Linalg suite only supports `dagger` execution"
    accel = isempty(accels) ? "cpu" : only(accels)
    @assert accel == "cpu" || haskey(GPU_BACKENDS, accel) "Unknown backend"

    T = benchmark_eltype()
    # Match the array/stencil suites: arbitrary fixture placement changes both
    # data movement and the driver's allocation share across revisions/samples.
    # Named cyclic grids are Distributed-only; leave MPI on its native allocator.
    fixture_assignment = benchmark_assignment()
    # Probe GPU operations on a multi-tile input. GPU libraries can implement
    # multiplication but lack a triangular copy or factorization used by syrk.
    matmul_ok = accel == "cpu" || supported("linalg/matmul") do
        A = rand(Blocks(4, 4), T, 8, 8)
        wait(A * A)
    end
    syrk_ok = accel == "cpu" || supported("linalg/syrk") do
        A = rand(Blocks(4, 4), T, 8, 8)
        wait(A' * A)
    end
    matvec_ok = accel == "cpu" || supported("linalg/matvec") do
        A = rand(Blocks(4, 4), T, 8, 8)
        x = rand(Blocks(4), T, 8)
        wait(A * x)
    end
    # Older revisions use a Distributed-only grid for tiled SVD. Keep GPU
    # factorization fallbacks out of these native-device benchmarks as well.
    svd_ok = accel == "cpu" && supported("linalg/svd") do
        A = rand(Blocks(4, 4), T, 8, 8; assignment=fixture_assignment)
        wait(A)
        wait(svd(A).U)
    end
    cholesky_ok = accel == "cpu" || supported("linalg/cholesky") do
        wait(cholesky(_spd(T, 8, 4)).factors)
    end
    suite = BenchmarkGroup()

    for N in scales
        for b in blocks_for(N)
            sub = BenchmarkGroup()

            # gemm needs A and the result resident; factorizations copy internally.
            if fits_budget(dense_bytes(N; nmats=3, T=T))
                if matmul_ok
                    sub["matmul (A*A)"] = @benchmarkable(wait(A * A),
                        setup = (A = rand(Blocks($b, $b), $T, $N, $N; assignment=$fixture_assignment); wait(A)),
                        teardown = (A = nothing; @everywhere GC.gc()))
                end

                if syrk_ok
                    sub["syrk (A'*A)"] = @benchmarkable(wait(A' * A),
                        setup = (A = rand(Blocks($b, $b), $T, $N, $N; assignment=$fixture_assignment); wait(A)),
                        teardown = (A = nothing; @everywhere GC.gc()))
                end

                # GPU panels need backend-specific factorization support; keep
                # these CPU fallbacks out of GPU performance measurements.
                if accel == "cpu"
                    sub["lu"] = @benchmarkable(wait(lu(A, RowMaximum()).factors),
                        setup = (A = rand(Blocks($b, $b), $T, $N, $N; assignment=$fixture_assignment); wait(A)),
                        teardown = (A = nothing; @everywhere GC.gc()))

                    sub["qr"] = @benchmarkable(wait(qr(A).factors),
                        setup = (A = rand(Blocks($b, $b), $T, $N, $N; assignment=$fixture_assignment); wait(A)),
                        teardown = (A = nothing; @everywhere GC.gc()))

                    sub["solve (A\\b via lu)"] = @benchmarkable(wait(lu(A, RowMaximum()) \ b),
                        setup = (A = rand(Blocks($b, $b), $T, $N, $N; assignment=$fixture_assignment);
                                 b = rand(Blocks($b), $T, $N; assignment=$fixture_assignment); wait(A); wait(b)),
                        teardown = (A = nothing; b = nothing; @everywhere GC.gc()))
                end
            end

            # Cholesky additionally holds the SPD-construction temporary.
            if cholesky_ok && fits_budget(dense_bytes(N; nmats=4, T=T))
                sub["cholesky"] = @benchmarkable(wait(cholesky(A).factors),
                    setup = (A = _spd($T, $N, $b; assignment=$fixture_assignment)),
                    teardown = (A = nothing; @everywhere GC.gc()))
            end

            # SVD (tiled one-sided Jacobi) additionally holds the internally-copied
            # scratch matrix, the accumulated V factor, and (across multiple
            # workers) a restaged copy of A, on top of the resident input.
            if svd_ok && fits_budget(dense_bytes(N; nmats=5, T=T))
                sub["svd"] = @benchmarkable(wait(svd(A).U),
                    setup = (A = rand(Blocks($b, $b), $T, $N, $N; assignment=$fixture_assignment); wait(A)),
                    teardown = (A = nothing; @everywhere GC.gc()))
            end

            # gemv is cheap (one matrix + two vectors).
            if matvec_ok && fits_budget(dense_bytes(N; nmats=1, T=T))
                sub["matvec (A*x)"] = @benchmarkable(wait(A * x),
                    setup = (A = rand(Blocks($b, $b), $T, $N, $N; assignment=$fixture_assignment);
                             x = rand(Blocks($b), $T, $N; assignment=$fixture_assignment); wait(A); wait(x)),
                    teardown = (A = nothing; x = nothing; @everywhere GC.gc()))
            end

            isempty(sub) || (suite["N=$N (block $b)"] = sub)
        end
    end

    suite
end

linalg_suite
