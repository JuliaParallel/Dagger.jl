@testset "$T" for T in (Float64, ComplexF64)
    tol = 1e-10

    for i_block_scale in (1.0, 0.5), j_block_scale in (1.0,)
        N = 128
        bs_i = round(Int, N*i_block_scale)
        bs_j = round(Int, N*j_block_scale)

        # LU decomposition — DMatrix RHS
        A = rand(T, N, N)
        B = rand(T, N, N)
        DA = DArray(A, Blocks(bs_i, bs_j))
        DB = DArray(B, Blocks(bs_i, bs_j))

        lu_A = lu(A, RowMaximum())
        lu_DA = lu(DA, RowMaximum())
        LinearAlgebra.ldiv!(lu_A, B)
        LinearAlgebra.ldiv!(lu_DA, DB)
        @test collect(DB) ≈ B

        # LU decomposition — DVector RHS
        A = rand(T, N, N)
        b = rand(T, N)
        b_ref = copy(b)
        DA = DArray(A, Blocks(bs_i, bs_j))
        Db = DArray(b, Blocks(bs_i))

        lu_A = lu(A, RowMaximum())
        lu_DA = lu(DA, RowMaximum())
        LinearAlgebra.ldiv!(lu_A, b_ref)
        LinearAlgebra.ldiv!(lu_DA, Db)
        @test collect(Db) ≈ b_ref rtol=tol

        # LU decomposition — plain Vector RHS
        A = rand(T, N, N)
        b = rand(T, N)
        b_ref = copy(b)
        DA = DArray(A, Blocks(bs_i, bs_j))

        lu_A = lu(A, RowMaximum())
        lu_DA = lu(DA, RowMaximum())
        LinearAlgebra.ldiv!(lu_A, b_ref)
        LinearAlgebra.ldiv!(lu_DA, b)
        @test b ≈ b_ref rtol=tol

        # Cholesky decomposition — DMatrix RHS
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

        # Cholesky decomposition — DVector RHS
        A = rand(T, N, N)
        A = A'A + I
        b = rand(T, N)
        b_ref = copy(b)
        DA = DArray(A, Blocks(bs_i, bs_j))
        Db = DArray(b, Blocks(bs_i))

        chol_A = cholesky(A)
        chol_DA = cholesky(DA)
        LinearAlgebra.ldiv!(chol_A, b_ref)
        LinearAlgebra.ldiv!(chol_DA, Db)
        @test collect(Db) ≈ b_ref rtol=tol
    end
end

@testset "Factorization solves retain distributed RHS storage" begin
    A = Matrix{Float64}(I, 8, 8) + fill(0.1, 8, 8)
    for b in (collect(1.0:8.0), reshape(collect(1.0:16.0), 8, 2))
        Db = distribute(b, b isa AbstractVector ? Blocks(3) : Blocks(3, 1))
        for F in (cholesky(distribute(A, Blocks(4, 4))),
                  lu(distribute(A, Blocks(4, 4))))
            x = F \ Db
            @test x isa Dagger.DArray
            @test collect(x) ≈ A \ b
            @test collect(Db) == b
            # Exercise both allocation protocols even on older Julia.
            dest = LinearAlgebra._zeros(Float64, Db, F)
            @test dest isa Dagger.DArray
            @test size(dest) == size(b)
            @test iszero(sum(dest))
        end
    end
end
