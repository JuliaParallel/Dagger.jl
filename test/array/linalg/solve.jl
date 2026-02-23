@testset "$T" for T in (Float32, Float64, ComplexF32, ComplexF64)
    for i_block_scale in (1.0, 0.5), j_block_scale in (1.0, 0.5)
        N = 128
        bs_i = round(Int, N*i_block_scale)
        bs_j = round(Int, N*j_block_scale)

        # LU decomposition
        A = rand(T, N, N)
        B = rand(T, N, N)
        DA = DArray(A, Blocks(bs_i, bs_j))
        DB = DArray(B, Blocks(bs_i, bs_j))

        lu_A = lu(A, RowMaximum())
        lu_DA = lu(DA, RowMaximum())
        LinearAlgebra.ldiv!(lu_A, B)
        LinearAlgebra.ldiv!(lu_DA, DB)
        @test collect(DB) ≈ B

        # Cholesky decomposition
        A = rand(T, N, N)
        A = A'A + I
        B = rand(T, N, N)
        DA = DArray(A, Blocks(bs_i, bs_j))
        DB = DArray(B, Blocks(bs_i, bs_j))

        chol_A = cholesky(A)
        chol_DA = cholesky(DA)
        LinearAlgebra.ldiv!(chol_A, B)
        LinearAlgebra.ldiv!(chol_DA, DB)
        @test collect(DB) ≈ B
    end
end