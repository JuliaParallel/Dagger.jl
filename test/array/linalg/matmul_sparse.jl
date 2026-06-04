# Fast, focused sparse matrix-multiply tests.
#
# This is intentionally a *small* subset of the full GEMM suite in `matmul.jl`,
# meant for quick iteration while developing sparse-array support (the full
# suite takes ~1hr). Run with:
#
#     julia test/runtests.jl --test array/linalg/matmul_sparse
#
# Keep this fast: only a couple of sizes/partitionings, just enough to exercise
# the out-of-place and in-place paths (incl. transposes) for both real and
# complex element types.

const SPARSE_DENSITY = 0.3

function test_sparse_gemm!(T, sz, partA, partB)
    rows, cols = sz
    @assert rows == cols "sparse quick-test uses square matrices so transposes line up"
    partC = Blocks(partA.blocksize[1], partB.blocksize[2])

    SA = sprand(T, sz..., SPARSE_DENSITY)
    SB = sprand(T, sz..., SPARSE_DENSITY)

    DSA = distribute(SA, partA)
    DSB = distribute(SB, partB)

    ## Out-of-place
    @test collect(DSA * DSB)   ≈ SA * SB     # N / N
    @test collect(DSA * DSB')  ≈ SA * SB'    # N / T
    @test collect(DSA' * DSB)  ≈ SA' * SB    # T / N
    @test collect(DSA' * DSB') ≈ SA' * SB'   # T / T

    ## Symmetric rank-k (syrk path: A === B with one operand transposed)
    @test collect(DSA' * DSA) ≈ Array(SA)' * Array(SA)
    @test collect(DSA * DSA') ≈ Array(SA) * Array(SA)'

    ## In-place
    SC = SA * SB
    DSC = distribute(sparse(zeros(T, sz...)), partC)
    mul!(DSC, DSA, DSB)
    @test collect(DSC) ≈ SC
end

const SPARSE_QUICK_CASES = [
    ((8, 8), Blocks(4, 4), Blocks(4, 4)),
    ((8, 8), Blocks(2, 4), Blocks(4, 2)),
    ((8, 8), Blocks(4, 2), Blocks(2, 4)),
]

@testset "Sparse GEMM (quick)" begin
    @testset "size=$sz part=$(partA.blocksize)/$(partB.blocksize)" for (sz, partA, partB) in SPARSE_QUICK_CASES
        @testset "T=$T" for T in (Float64, ComplexF64)
            test_sparse_gemm!(T, sz, partA, partB)
        end
    end
end

# Any *partial* or *reinterpreted* access to a sparse container (views,
# transposes, adjoints, reshapes, and combinations thereof) must alias the
# *entire* container, so that Datadeps never tracks stale sub-spans of storage
# that may have been reallocated on write.
@testset "Sparse whole-container aliasing" begin
    M = Dagger.DSparseMatrix{Float64}(sprand(Float64, 6, 6, 0.4))
    a_full = Dagger.aliasing(M)

    @test Dagger.will_alias(a_full, Dagger.aliasing(view(M, 1:3, :)))
    @test Dagger.will_alias(a_full, Dagger.aliasing(view(M, 4:6, 2:4)))
    @test Dagger.will_alias(a_full, Dagger.aliasing(transpose(M)))
    @test Dagger.will_alias(a_full, Dagger.aliasing(M'))
    @test Dagger.will_alias(a_full, Dagger.aliasing(reshape(M, 36)))
    @test Dagger.will_alias(a_full, Dagger.aliasing(view(transpose(M), 1:2, :)))
    # Non-overlapping views of the *same* container still alias (whole-container).
    @test Dagger.will_alias(Dagger.aliasing(view(M, 1:3, :)), Dagger.aliasing(view(M, 4:6, :)))

    # Distinct containers must not alias.
    M2 = Dagger.DSparseMatrix{Float64}(sprand(Float64, 6, 6, 0.4))
    @test !Dagger.will_alias(a_full, Dagger.aliasing(M2))

    # Dense-array views are unaffected (still strided, not whole-array).
    A = rand(Float64, 6, 6)
    @test Dagger.aliasing(view(A, 1:3, :)) isa Dagger.StridedAliasing
end
