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

        # `\` must keep the result distributed. `\` routes through
        # `LinearAlgebra.ldiv`, which stages the right-hand side into a buffer
        # from `LinearAlgebra._zeros`; when Dagger's `_zeros` methods do not
        # match, that buffer is a dense `Array`, so `\` silently returns one
        # and the distribution is lost. Every other test in this file calls
        # `ldiv!` directly and so never reaches that path -- which is why
        # JuliaLang/LinearAlgebra.jl#1746, changing `_zeros`' third argument
        # from the factorization's width to the factorization itself, went
        # unnoticed here until the allocations testsuite tripped over it.
        A = rand(T, N, N); A = A'A + I
        B = rand(T, N, N)
        b = rand(T, N)

        x = cholesky(DArray(A, Blocks(bs_i, bs_j))) \ DArray(b, Blocks(bs_i))
        @test x isa DVector{T}
        @test (wait(x); true)
        @test collect(x) ≈ A \ b rtol=tol

        X = cholesky(DArray(A, Blocks(bs_i, bs_j))) \ DArray(B, Blocks(bs_i, bs_j))
        @test X isa DMatrix{T}
        @test collect(X) ≈ A \ B rtol=tol

        y = lu(DArray(A, Blocks(bs_i, bs_j)), RowMaximum()) \ DArray(b, Blocks(bs_i))
        @test y isa DVector{T}
        @test collect(y) ≈ A \ b rtol=tol
    end
end