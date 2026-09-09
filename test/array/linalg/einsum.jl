# Public `Dagger.@einsum` — tiled Einstein summation. Tests call the macro the
# way a user would, plus the LinearAlgebra entries for the same 2-tensor
# products (`mul!`, `*`, `dot`).

import Dagger: @einsum

@testset "@einsum GEMM matches mul! / *" begin
    A = rand(8, 8)
    B = rand(8, 8)
    DA = distribute(A, Blocks(4, 4))
    DB = distribute(B, Blocks(4, 4))
    DC = zeros(Blocks(4, 4), 8, 8)
    @einsum DC[i, j] = DA[i, k] * DB[k, j]
    @test collect(DC) ≈ A * B
    Dmul = zeros(Blocks(4, 4), 8, 8)
    mul!(Dmul, DA, DB)
    @test collect(Dmul) ≈ collect(DC)
    @test collect(DA * DB) ≈ A * B

    DE = @einsum DA[i, k] * DB[k, j]
    @test DE isa DMatrix
    @test collect(DE) ≈ A * B

    DF = @einsum DA[i, k] * DB[j, k]
    @test collect(DF) ≈ A * transpose(B)

    α = 2.5
    DG = zeros(Blocks(4, 4), 8, 8)
    @einsum DG[i, j] = α * DA[i, k] * DB[k, j]
    @test collect(DG) ≈ α * (A * B)
end

@testset "@einsum GEMV / outer / dot match LinearAlgebra" begin
    A = rand(8, 8)
    x = rand(8)
    y = rand(8)
    DA = distribute(A, Blocks(4, 4))
    Dx = distribute(x, Blocks(4))
    Dy = distribute(y, Blocks(4))

    Dz = zeros(Blocks(4), 8)
    @einsum Dz[i] = DA[i, j] * Dx[j]
    @test collect(Dz) ≈ A * x
    Dmv = zeros(Blocks(4), 8)
    mul!(Dmv, DA, Dx)
    @test collect(Dmv) ≈ collect(Dz)

    @test (@einsum Dx[i] * Dy[i]) ≈ dot(x, y)
    @test dot(Dx, Dy) ≈ (@einsum Dx[i] * Dy[i])

    DC = zeros(Blocks(4, 4), 8, 8)
    @einsum DC[i, j] = Dx[i] * Dy[j]
    @test collect(DC) ≈ x * y'
end

@testset "@einsum Hadamard / accumulate / host Array" begin
    A = rand(8, 8)
    B = rand(8, 8)
    DA = distribute(A, Blocks(4, 4))
    DB = distribute(B, Blocks(4, 4))
    @test (@einsum DA[i, j] * DB[i, j]) ≈ dot(vec(A), vec(B))

    DC = zeros(Blocks(4, 4), 8, 8)
    @einsum DC[i, j] = DA[i, k] * DB[k, j]
    @einsum DC[i, j] += DA[i, k] * DB[k, j]
    @test collect(DC) ≈ 2 * (A * B)

    # Host Array is wrapped as one tile; mix with a DArray.
    DH = @einsum DA[i, k] * B[k, j]
    @test collect(DH) ≈ A * B
end

@testset "@einsum := and block of statements" begin
    A = rand(8, 8)
    x = rand(8)
    DA = distribute(A, Blocks(4, 4))
    Dx = distribute(x, Blocks(4))
    Dy = @einsum begin
        C[i, j] := DA[i, k] * DA[k, j]
        C[i, j] * Dx[j]
    end
    @test collect(Dy) ≈ (A * A) * x
end
