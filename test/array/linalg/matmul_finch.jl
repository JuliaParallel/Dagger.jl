# Fast, focused sparse matrix-multiply tests for the Finch backend.
#
# Mirrors `matmul_sparse.jl` (SparseArrays) but exercises Finch tensor tiles via
# `DSparseMatrix`. Finch is an optional (weak) dependency, so this test loads it
# on all workers and skips gracefully if it cannot be loaded.
#
# Run with:
#
#     julia test/runtests.jl --test array/linalg/matmul_finch
#
# N.B. Finch precompilation is heavy (several minutes); this test is intended for
# local validation rather than fast CI.

# Make the test-project environment (which provides Finch) resolvable on the
# workers, which are launched with Dagger's project.
const _FINCH_TEST_ENV = abspath(joinpath(@__DIR__, "..", ".."))
@everywhere pushfirst!(LOAD_PATH, $_FINCH_TEST_ENV)

const FINCH_AVAILABLE = try
    @eval using Finch
    @everywhere using Finch
    true
catch err
    @warn "Finch unavailable; skipping Finch matmul tests" exception=err
    false
end

if !FINCH_AVAILABLE
    @testset "Finch GEMM (quick)" begin
        @test_skip false
    end
else
    const FINCH_DENSITY = 0.3

    # Self-consistent: build distributed Finch operands, take the dense
    # `collect` as the reference, and compare against the distributed result.
    function test_finch_gemm!(T, sz, partA, partB)
        partC = Blocks(partA.blocksize[1], partB.blocksize[2])

        DSA = Finch.fsprand(partA, T, sz, FINCH_DENSITY)
        DSB = Finch.fsprand(partB, T, sz, FINCH_DENSITY)
        A = collect(DSA)
        B = collect(DSB)

        ## Out-of-place
        @test collect(DSA * DSB)   ≈ A * B     # N / N
        @test collect(DSA * DSB')  ≈ A * B'    # N / C
        @test collect(DSA' * DSB)  ≈ A' * B    # C / N
        @test collect(DSA' * DSB') ≈ A' * B'   # C / C

        ## Symmetric rank-k (syrk path: A === B with one operand transposed)
        @test collect(DSA' * DSA) ≈ A' * A
        @test collect(DSA * DSA') ≈ A * A'

        ## In-place
        DSC = Finch.fspzeros(partC, T, sz)
        mul!(DSC, DSA, DSB)
        @test collect(DSC) ≈ A * B
    end

    const FINCH_QUICK_CASES = [
        ((8, 8), Blocks(4, 4), Blocks(4, 4)),
        ((8, 8), Blocks(2, 4), Blocks(4, 2)),
    ]

    @testset "Finch GEMM (quick)" begin
        @testset "size=$sz part=$(partA.blocksize)/$(partB.blocksize)" for (sz, partA, partB) in FINCH_QUICK_CASES
            @testset "T=$T" for T in (Float64, ComplexF64)
                test_finch_gemm!(T, sz, partA, partB)
            end
        end
    end

    # Sparse matrix-vector multiply (SpMV): Finch sparse matrix tiles, dense vectors.
    function test_finch_spmv!(T, n, part)
        bs = part.blocksize[1]
        DSA = Finch.fsprand(part, T, (n, n), FINCH_DENSITY)
        A = collect(DSA)
        x = rand(T, n)
        Dx = distribute(x, Blocks(bs))

        @test collect(DSA * Dx)            ≈ A * x
        @test collect(transpose(DSA) * Dx) ≈ transpose(A) * x
        @test collect(DSA' * Dx)           ≈ A' * x

        y = rand(T, n)
        Dy = distribute(copy(y), Blocks(bs))
        alpha, beta = T(2), T(3)
        mul!(Dy, DSA, Dx, alpha, beta)
        @test collect(Dy) ≈ alpha * (A * x) + beta * y
    end

    const FINCH_SPMV_CASES = [
        (8, Blocks(4, 4)),
        (8, Blocks(2, 2)),
    ]

    @testset "Finch SpMV (quick)" begin
        @testset "n=$n part=$(part.blocksize)" for (n, part) in FINCH_SPMV_CASES
            @testset "T=$T" for T in (Float64, ComplexF64)
                test_finch_spmv!(T, n, part)
            end
        end
    end
end
