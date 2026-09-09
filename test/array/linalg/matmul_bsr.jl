# Host BSR tiles (`Dagger.SparseMatrixBSR`). CSC remains the default; this
# file checks distribute / mul! / * / Krylov on a BSR-backed DMatrix through
# LinearAlgebra / SparseArrays / Krylov — not a Dagger.bsr_mul helper.

using SparseArrays
using LinearAlgebra
using Random
using Krylov

function _bsr_inner(A)
    tile = fetch(first(A.chunks))
    return tile isa Dagger.DSparseArray ? tile.mat : tile
end

@testset "BSR type / host mul!" begin
    SA = sparse(Float64[4 1 0 0; 1 4 1 0; 0 1 4 1; 0 0 1 4])
    B = Dagger.sparsebsr(SA, (2, 2))
    @test B isa Dagger.SparseMatrixBSR
    @test Dagger.BSR === Dagger.SparseMatrixBSR
    @test size(B) == (4, 4)
    @test B.blocksize == (2, 2)
    x = rand(4)
    y = zeros(4)
    mul!(y, B, x)
    @test y ≈ SA * x
    @test B * x ≈ SA * x
    @test Array(B) ≈ Array(SA)
end

@testset "BSR distribute / convert / assembly" begin
    Random.seed!(1234)
    SA = sprand(Float64, 8, 8, 0.4)
    part = Blocks(4, 4)
    bs = (2, 2)

    B = Dagger.sparsebsr(SA, bs)
    DA = distribute(B, part)
    @test _bsr_inner(DA) isa Dagger.SparseMatrixBSR
    @test Dagger.is_sparse_backed(DA)
    @test collect(DA) ≈ SA
    @test SparseArrays.sparse(DA) ≈ SA

    DC = distribute(SA, part)
    DR = Dagger.sparsebsr(DC, part, bs)
    @test _bsr_inner(DR) isa Dagger.SparseMatrixBSR
    @test collect(DR) ≈ SA
    Sg = Dagger.sparsebsr(DC, bs)
    @test Sg isa Dagger.SparseMatrixBSR
    @test Array(Sg) ≈ Array(SA)

    I, J, V = findnz(SA)
    DA2 = Dagger.sparsebsr(I, J, V, 8, 8, bs, part)
    @test _bsr_inner(DA2) isa Dagger.SparseMatrixBSR
    @test collect(DA2) ≈ SA

    Z = SparseArrays.spzeros(Dagger.SparseMatrixBSR, part, Float64, 8, 8; blocksize=bs)
    @test _bsr_inner(Z) isa Dagger.SparseMatrixBSR
    @test iszero(sum(abs, collect(Z)))
end

@testset "BSR SpMV / SpGEMM via mul! and *" begin
    SA = sprand(Float64, 8, 8, 0.4)
    SB = sprand(Float64, 8, 8, 0.4)
    part = Blocks(4, 4)
    bs = (2, 2)
    DA = Dagger.sparsebsr(distribute(SA, part), part, bs)
    DB = Dagger.sparsebsr(distribute(SB, part), part, bs)
    x = rand(8)
    Dx = distribute(x, Blocks(4))

    @test collect(DA * Dx) ≈ SA * x
    y = rand(8)
    Dy = distribute(copy(y), Blocks(4))
    mul!(Dy, DA, Dx, 2.0, 3.0)
    @test collect(Dy) ≈ 2.0 * (SA * x) + 3.0 * y

    @test collect(DA * DB) ≈ SA * SB
    DC = Dagger.sparsebsr(distribute(sparse(zeros(8, 8)), part), part, bs)
    mul!(DC, DA, DB)
    @test collect(DC) ≈ SA * SB
    @test _bsr_inner(DC) isa Dagger.SparseMatrixBSR
end

@testset "BSR Krylov / LinearAlgebra solve" begin
    n, k = 16, 8
    A = SparseArrays.spdiagm(
        -1 => fill(-1.0, n - 1),
         0 => fill(4.0, n),
         1 => fill(-1.0, n - 1),
    )
    b = A * ones(n)
    part = Blocks(k, k)
    DA = Dagger.sparsebsr(distribute(A, part), part, (2, 2))
    @test _bsr_inner(DA) isa Dagger.SparseMatrixBSR
    Db = distribute(b, Blocks(k))

    x, stats = Krylov.cg(DA, Db; atol=1e-12, rtol=1e-10, itmax=200)
    r = similar(Db)
    mul!(r, DA, x)
    axpy!(-1, Db, r)
    @test LinearAlgebra.norm(collect(r)) / LinearAlgebra.norm(b) < 1e-8

    xs = DA \ Db
    @test LinearAlgebra.norm(A * collect(xs) - b) / LinearAlgebra.norm(b) < 1e-8
end
