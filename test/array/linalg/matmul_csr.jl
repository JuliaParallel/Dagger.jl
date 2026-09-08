# Host CSR tile tests (SparseMatricesCSR.jl).
#
# CSC coverage stays in `matmul_sparse.jl`. This file checks that
# `SparseMatrixCSR` tiles participate in distribute / convert / SpMV / SpGEMM
# without densifying. Run with:
#
#     julia test/runtests.jl --test array/linalg/matmul_csr
#
# Workers launch under Dagger's project (weakdep only), so the test env that
# actually provides SparseMatricesCSR has to be on their LOAD_PATH.

const _CSR_TEST_ENV = abspath(joinpath(@__DIR__, "..", ".."))
@everywhere pushfirst!(LOAD_PATH, $_CSR_TEST_ENV)

const CSR_AVAILABLE = try
    @eval using SparseMatricesCSR
    @everywhere using SparseMatricesCSR
    true
catch err
    @warn "SparseMatricesCSR unavailable; skipping CSR matmul tests" exception=err
    false
end

if !CSR_AVAILABLE
    @testset "CSR tiles (quick)" begin
        @test_skip false
    end
else
    using Krylov

    const CSR_DENSITY = 0.3

    function _csr_inner(A)
        tile = fetch(first(A.chunks))
        return tile isa Dagger.DSparseArray ? tile.mat : tile
    end

    @testset "CSR distribute / convert" begin
        Random.seed!(1234)
        SA = sprand(Float64, 8, 8, CSR_DENSITY)
        part = Blocks(4, 4)

        # Existing CSC tiles are unchanged.
        DC = distribute(SA, part)
        @test _csr_inner(DC) isa SparseArrays.SparseMatrixCSC
        @test Dagger.is_sparse_backed(DC)
        @test collect(DC) ≈ SA

        # Local CSR → CSR tiles (range getindex must not densify).
        SAcsr = sparsecsr(SA)
        DA = distribute(SAcsr, part)
        @test _csr_inner(DA) isa SparseMatricesCSR.SparseMatrixCSR
        @test Dagger.is_sparse_backed(DA)
        @test collect(DA) ≈ SA
        @test SparseArrays.sparse(DA) ≈ SA

        # Tile-preserving convert from CSC DMatrix; gather convert to one CSR.
        DR = sparsecsr(DC, part)
        @test _csr_inner(DR) isa SparseMatricesCSR.SparseMatrixCSR
        @test collect(DR) ≈ SA
        Sg = sparsecsr(DC)
        @test Sg isa SparseMatricesCSR.SparseMatrixCSR
        @test Array(Sg) ≈ Array(SA)
        @test convert(SparseMatricesCSR.SparseMatrixCSR, DC) ≈ Sg

        # COO + empty CSR allocation.
        I, J, V = findnz(SA)
        DA2 = sparsecsr(I, J, V, 8, 8, part)
        @test _csr_inner(DA2) isa SparseMatricesCSR.SparseMatrixCSR
        @test collect(DA2) ≈ SA
        Z = SparseArrays.spzeros(SparseMatricesCSR.SparseMatrixCSR, part, Float64, 8, 8)
        @test _csr_inner(Z) isa SparseMatricesCSR.SparseMatrixCSR
        @test iszero(sum(abs, collect(Z)))

        # Re-tiling stays a sparse wrapper (inner format may become CSC;
        # `sparsecsr(A, newpart)` converts back).
        DA_fine = Dagger.repartition(DA, Blocks(2, 2))
        @test Dagger.is_sparse_backed(DA_fine)
        @test collect(DA_fine) ≈ SA
        DR_fine = sparsecsr(DA, Blocks(2, 2))
        @test _csr_inner(DR_fine) isa SparseMatricesCSR.SparseMatrixCSR
        @test collect(DR_fine) ≈ SA
    end

    function test_csr_gemm!(T, sz, partA, partB)
        rows, cols = sz
        @assert rows == cols
        partC = Blocks(partA.blocksize[1], partB.blocksize[2])

        SA = sprand(T, sz..., CSR_DENSITY)
        SB = sprand(T, sz..., CSR_DENSITY)
        DSA = sparsecsr(distribute(SA, partA), partA)
        DSB = sparsecsr(distribute(SB, partB), partB)
        @test _csr_inner(DSA) isa SparseMatricesCSR.SparseMatrixCSR

        @test collect(DSA * DSB)   ≈ SA * SB
        @test collect(DSA * DSB')  ≈ SA * SB'
        @test collect(DSA' * DSB)  ≈ SA' * SB
        @test collect(DSA' * DSB') ≈ SA' * SB'
        @test collect(DSA' * DSA)  ≈ Array(SA)' * Array(SA)
        @test collect(DSA * DSA')  ≈ Array(SA) * Array(SA)'

        DSC = sparsecsr(distribute(sparse(zeros(T, sz...)), partC), partC)
        mul!(DSC, DSA, DSB)
        @test collect(DSC) ≈ SA * SB
        @test _csr_inner(DSC) isa SparseMatricesCSR.SparseMatrixCSR

        # Mixed CSC × CSR must not densify.
        DCSC = distribute(SA, partA)
        @test collect(DCSC * DSB) ≈ SA * SB
        @test collect(DSA * distribute(SB, partB)) ≈ SA * SB
    end

    function test_csr_spmv!(T, n, part)
        bs = part.blocksize[1]
        SA = sprand(T, n, n, CSR_DENSITY)
        x = rand(T, n)
        DSA = sparsecsr(distribute(SA, part), part)
        Dx = distribute(x, Blocks(bs))

        @test collect(DSA * Dx)            ≈ SA * x
        @test collect(transpose(DSA) * Dx) ≈ transpose(SA) * x
        @test collect(DSA' * Dx)           ≈ SA' * x

        y = rand(T, n)
        Dy = distribute(copy(y), Blocks(bs))
        alpha, beta = T(2), T(3)
        mul!(Dy, DSA, Dx, alpha, beta)
        @test collect(Dy) ≈ alpha * (SA * x) + beta * y
    end

    const CSR_QUICK_CASES = [
        ((8, 8), Blocks(4, 4), Blocks(4, 4)),
        ((8, 8), Blocks(2, 4), Blocks(4, 2)),
    ]

    @testset "CSR GEMM (quick)" begin
        @testset "size=$sz part=$(partA.blocksize)/$(partB.blocksize)" for (sz, partA, partB) in CSR_QUICK_CASES
            @testset "T=$T" for T in (Float64, ComplexF64)
                test_csr_gemm!(T, sz, partA, partB)
            end
        end
    end

    @testset "CSR SpMV (quick)" begin
        @testset "n=$n part=$(part.blocksize)" for (n, part) in ((8, Blocks(4, 4)), (8, Blocks(2, 2)))
            @testset "T=$T" for T in (Float64, ComplexF64)
                test_csr_spmv!(T, n, part)
            end
        end
    end

    @testset "CSR Krylov" begin
        n, k = 32, 8
        A = SparseArrays.spdiagm(
            -1 => fill(-1.0, n - 1),
             0 => fill(4.0, n),
             1 => fill(-1.0, n - 1),
        )
        b = rand(n)
        DA = sparsecsr(distribute(A, Blocks(k, k)), Blocks(k, k))
        @test _csr_inner(DA) isa SparseMatricesCSR.SparseMatrixCSR
        Db = distribute(b, Blocks(k))
        x, stats = Dagger.cg(DA, Db; atol=1e-8, rtol=1e-8, itmax=500)
        @test stats.solved
        @test collect(x) ≈ Matrix(A) \ b rtol=1e-6
    end
end
