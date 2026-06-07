# Distributed iterative (Krylov) linear-solver tests.
#
# Exercises the matrix-free Krylov integration (`Dagger.cg`/`minres`/`gmres`/
# `bicgstab` + the generic `krylov_solve`) over both dense and sparse-backed
# `DMatrix` operators, plus the Jacobi preconditioner. Reference solutions come
# from a dense direct solve.
#
#     julia test/runtests.jl --test array/linalg/iterativesolvers

using Krylov

# Strongly diagonally-dominant tridiagonal SPD matrix. The large diagonal keeps
# the condition number small so the Krylov methods converge in a handful of
# iterations (keeping the distributed test fast). `inv(diag) == 1/4`.
const SPD_DIAG = 4.0

laplacian_1d(T, n) = SparseArrays.spdiagm(
    -1 => fill(-one(T), n - 1),
     0 => fill(T(SPD_DIAG), n),
     1 => fill(-one(T), n - 1),
)

# Add a first-order advection term -> nonsymmetric, still well-conditioned.
function advection_diffusion_1d(T, n)
    return laplacian_1d(T, n) + SparseArrays.spdiagm(
        -1 => fill(T(-3) / 10, n - 1),
         1 => fill(T(3) / 10, n - 1),
    )
end

@testset "Iterative solvers (Krylov)" begin
    n = 64
    k = 16
    Db_part = Blocks(k)
    A_part = Blocks(k, k)

    @testset "SPD operator ($(backend))" for backend in (:dense, :sparse)
        Asp = laplacian_1d(Float64, n)
        Adense = Matrix(Asp)
        b = rand(n)
        xref = Adense \ b

        DA = backend === :dense ? distribute(Adense, A_part) : distribute(Asp, A_part)
        Db = distribute(b, Db_part)

        @testset "$(nameof(solver))" for solver in (Dagger.cg, Dagger.minres, Dagger.gmres, Dagger.bicgstab)
            x, stats = solver(DA, Db; atol = 1e-12, rtol = 1e-10, itmax = 500)
            @test stats.solved
            @test x isa Dagger.DVector
            @test collect(x) ≈ xref rtol = 1e-6
        end

        # Generic entry point.
        x, stats = Dagger.krylov_solve(:cg, DA, Db; atol = 1e-12, rtol = 1e-10)
        @test stats.solved
        @test collect(x) ≈ xref rtol = 1e-6
    end

    @testset "nonsymmetric operator ($(backend))" for backend in (:dense, :sparse)
        Asp = advection_diffusion_1d(Float64, n)
        b = rand(n)
        xref = Matrix(Asp) \ b

        DA = backend === :dense ? distribute(Matrix(Asp), A_part) : distribute(Asp, A_part)
        Db = distribute(b, Db_part)

        @testset "$(nameof(solver))" for solver in (Dagger.gmres, Dagger.bicgstab)
            x, stats = solver(DA, Db; atol = 1e-12, rtol = 1e-10, itmax = 500)
            @test stats.solved
            @test collect(x) ≈ xref rtol = 1e-6
        end
    end

    @testset "complex SPD (Hermitian) operator" begin
        # Real SPD tridiagonal is Hermitian as a complex matrix.
        Asp = SparseArrays.spdiagm(
            -1 => fill(ComplexF64(-1), n - 1),
             0 => fill(ComplexF64(SPD_DIAG), n),
             1 => fill(ComplexF64(-1), n - 1),
        )
        b = rand(ComplexF64, n)
        xref = Matrix(Asp) \ b
        DA = distribute(Asp, A_part)
        Db = distribute(b, Db_part)
        x, stats = Dagger.cg(DA, Db; atol = 1e-12, rtol = 1e-10, itmax = 500)
        @test stats.solved
        @test collect(x) ≈ xref rtol = 1e-6
    end

    @testset "Jacobi preconditioner" begin
        Asp = laplacian_1d(Float64, n)
        b = rand(n)
        xref = Matrix(Asp) \ b

        @testset "build + apply ($(backend))" for backend in (:dense, :sparse)
            DA = backend === :dense ? distribute(Matrix(Asp), A_part) : distribute(Asp, A_part)
            Db = distribute(b, Db_part)

            P = Dagger.JacobiPreconditioner(DA)
            @test collect(P.dinv) ≈ fill(1 / SPD_DIAG, n)   # 1/diag

            # Apply: y = M⁻¹ x = dinv .* x.
            y = similar(Db)
            mul!(y, P, Db)
            @test collect(y) ≈ (1 / SPD_DIAG) .* b

            x, stats = Dagger.cg(DA, Db; M = P, atol = 1e-12, rtol = 1e-10, itmax = 500)
            @test stats.solved
            @test collect(x) ≈ xref rtol = 1e-6
        end

        # Non-square block grid must be rejected with a helpful error.
        DA_ragged = distribute(Matrix(Asp), Blocks(k, k ÷ 2))
        @test_throws ArgumentError Dagger.JacobiPreconditioner(DA_ragged)
    end

    @testset "block-Jacobi preconditioner" begin
        Asp = laplacian_1d(Float64, n)
        Adense = Matrix(Asp)
        b = rand(n)
        xref = Adense \ b

        # Reference: apply the exact block-diagonal inverse.
        yref = similar(b)
        for s in 1:k:n
            r = s:min(s + k - 1, n)
            yref[r] = Adense[r, r] \ b[r]
        end

        @testset "build + apply ($(backend))" for backend in (:dense, :sparse)
            DA = backend === :dense ? distribute(Adense, A_part) : distribute(Asp, A_part)
            Db = distribute(b, Db_part)

            P = Dagger.BlockJacobiPreconditioner(DA)
            y = similar(Db)
            mul!(y, P, Db)
            @test collect(y) ≈ yref

            x, stats = Dagger.cg(DA, Db; M = P, atol = 1e-12, rtol = 1e-10, itmax = 500)
            @test stats.solved
            @test collect(x) ≈ xref rtol = 1e-6
        end

        # A single tile makes block-Jacobi an *exact* solve, so PCG converges
        # essentially immediately.
        DA1 = distribute(Adense, Blocks(n, n))
        Db1 = distribute(b, Blocks(n))
        P1 = Dagger.BlockJacobiPreconditioner(DA1)
        x1, s1 = Dagger.cg(DA1, Db1; M = P1, atol = 1e-12, rtol = 1e-10, itmax = 500)
        @test s1.solved
        @test s1.niter <= 2
        @test collect(x1) ≈ xref rtol = 1e-8

        @test_throws ArgumentError Dagger.BlockJacobiPreconditioner(distribute(Adense, Blocks(k, k ÷ 2)))
    end
end
