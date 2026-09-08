# Sparse Cholesky and incomplete-Cholesky (IC(0)) tests.
#
# `cholesky` on a sparse-backed SPD `DMatrix` must gather-then-factor (CHOLMOD),
# not tiled dense potrf. `ichol` / `BlockICPreconditioner` is block IC(0) for
# Krylov `M`. Do not treat `stats.solved` as `Ax≈b` (AGENTS.md lesson 19).
#
#     julia test/runtests.jl --test array/linalg/sparsechol

using Krylov

laplacian_1d(T, n) = SparseArrays.spdiagm(
    -1 => fill(-one(T), n - 1),
     0 => fill(T(4), n),
     1 => fill(-one(T), n - 1),
)

function relres(A, x, b)
    return norm(A * x - b) / norm(b)
end

@testset "sparse Cholesky + IC(0)" begin
    n = 64
    k = 16
    A_part = Blocks(k, k)
    b_part = Blocks(k)

    Asp = laplacian_1d(Float64, n)
    Adense = Matrix(Asp)
    b = rand(n)
    xref = Adense \ b

    @testset "cholesky(sparse DMatrix) \\ b ($(backend))" for backend in (
        :sparse, :singletile, :dense_ref)
        DA = if backend === :sparse
            distribute(Asp, A_part)
        elseif backend === :singletile
            distribute(Asp, Blocks(n, n))
        else
            distribute(Adense, A_part)
        end
        Db = distribute(b, b_part)

        F = cholesky(DA)
        if backend === :dense_ref
            # Dense tiled path is unchanged.
            @test F isa LinearAlgebra.Cholesky
            @test collect(F.U) ≈ cholesky(Adense).U
        else
            @test F isa Dagger.DaggerSparseCholesky
            @test !(F isa LinearAlgebra.Cholesky)
            @test size(F) == (n, n)
        end

        x = F \ Db
        @test x isa Dagger.DVector
        @test collect(x) ≈ xref
        @test relres(Adense, collect(x), b) < 1e-10

        y = similar(Db)
        LinearAlgebra.ldiv!(y, F, Db)
        @test collect(y) ≈ xref

        # `cholesky!` on sparse gathers rather than running potrf in-place.
        F! = cholesky!(backend === :dense_ref ? copy(DA) : DA)
        if backend === :dense_ref
            @test F! isa LinearAlgebra.Cholesky
        else
            @test F! isa Dagger.DaggerSparseCholesky
            @test collect(F! \ Db) ≈ xref
        end
    end

    @testset "cholesky!(F, A) reuses the symbolic factor" begin
        DA = distribute(Asp, A_part)
        DA2 = distribute(2 * Asp, A_part)
        Db = distribute(b, b_part)
        xref2 = (2 * Adense) \ b

        F = cholesky(DA)
        id0 = fetch(Dagger.spawn(Dagger._pinned_factor_objectid,
                                 Dagger.Options(; compute_scope=F.scope), F.fact))
        @test collect(F \ Db) ≈ xref

        @test LinearAlgebra.cholesky!(F, DA2) === F
        @test collect(F \ Db) ≈ xref2
        @test relres(2 * Adense, collect(F \ Db), b) < 1e-10
        id1 = fetch(Dagger.spawn(Dagger._pinned_factor_objectid,
                                 Dagger.Options(; compute_scope=F.scope), F.fact))
        @test id0 == id1

        @test_throws DimensionMismatch LinearAlgebra.cholesky!(F, distribute(Asp[1:n÷2, 1:n÷2],
                                                              Blocks(k, k)))
    end

    @testset "dense cholesky is unchanged" begin
        DA = distribute(Adense, A_part)
        F = cholesky(DA)
        @test F isa LinearAlgebra.Cholesky
        @test collect(F.L) ≈ cholesky(Adense).L
        @test collect(cholesky(DA) \ distribute(b, b_part)) ≈ xref
    end

    @testset "ichol / BlockIC as Krylov M ($(backend))" for backend in (
        :sparse, :singletile, :dense)
        DA = if backend === :sparse
            distribute(Asp, A_part)
        elseif backend === :singletile
            distribute(Asp, Blocks(n, n))
        else
            distribute(Adense, A_part)
        end
        Db = distribute(b, b_part)

        P = backend === :singletile ? Dagger.ichol(DA) : Dagger.BlockICPreconditioner(DA)
        @test P isa Dagger.BlockICPreconditioner

        # Apply is repeatable (cached, pinned per-tile factors).
        y1 = similar(Db); mul!(y1, P, Db)
        y2 = similar(Db); mul!(y2, P, Db)
        @test all(isfinite, collect(y1))
        @test collect(y1) ≈ collect(y2)

        # Single-tile IC(0) of a tridiagonal is exact Cholesky (no fill).
        if backend === :singletile
            @test collect(y1) ≈ xref
        end

        x, stats = Dagger.cg(DA, Db; M = P, atol = 1e-12, rtol = 1e-10, itmax = 500)
        @test x isa Dagger.DVector
        xc = collect(x)
        @test relres(Adense, xc, b) < 1e-8
        @test xc ≈ xref rtol = 1e-6
        # `stats.solved` is the *preconditioned* residual; require Ax≈b too.
        @test stats.solved

        xk, statsk = Krylov.cg(DA, Db; M = P, atol = 1e-12, rtol = 1e-10, itmax = 500)
        @test relres(Adense, collect(xk), b) < 1e-8
    end

    @testset "non-square tiles are re-tiled, not rejected" begin
        DA = distribute(Asp, Blocks(k, k ÷ 2))
        Db = distribute(b, Blocks(k ÷ 2))
        P = Dagger.ichol(DA)
        x, stats = Dagger.cg(DA, Db; M = P, atol = 1e-12, rtol = 1e-10, itmax = 500)
        @test relres(Adense, collect(x), b) < 1e-8
        @test collect(x) ≈ xref rtol = 1e-6
    end
end
