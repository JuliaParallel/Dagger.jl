# Distributed iterative eigen (LOBPCG) tests.
#
# `eigen` / `eigvals` on a `DMatrix` compute a few extreme pairs — not dense
# geev. Reference values come from `eigen(Matrix(A))`. Residual is the true
# `‖Ax-λx‖` via distributed `mul!`, not only the Ritz residual.
#
#     julia test/runtests.jl --test array/linalg/eigen

# Classic 1-D Dirichlet Laplacian: 2 on the diagonal, -1 on the off-diagonals.
# Smallest eigenpair is known in closed form: λ_k = 2 - 2cos(kπ/(n+1)).
laplacian_1d_eigen(T, n) = SparseArrays.spdiagm(
    -1 => fill(-one(T), n - 1),
     0 => fill(T(2), n),
     1 => fill(-one(T), n - 1),
)

function eigen_true_res(DA, λ, x::Dagger.DVector)
    r = similar(x)
    mul!(r, DA, x)
    axpy!(-λ, x, r)
    return LinearAlgebra.norm2(r)
end

# First column as a DVector without `F.vectors[:, 1]` (that getindex is a
# nested DArray — AGENTS.md lesson 33).
function eigen_first_vec(F)
    v = collect(F.vectors)[:, 1]
    mb = F.vectors.partitioning.blocksize[1]
    return distribute(v, Blocks(mb))
end

@testset "Iterative eigen (LOBPCG)" begin
    n = 32
    k = 8
    A_part = Blocks(k, k)
    Asp = laplacian_1d_eigen(Float64, n)
    Ah = Matrix(Asp)
    href = eigen(Ah)
    λmin = href.values[1]
    λmax = href.values[end]
    # Closed form for the 1-D Dirichlet Laplacian.
    @test λmin ≈ 2 - 2 * cos(π / (n + 1))

    @testset "smallest pair ($(backend))" for backend in (:sparse, :dense)
        DA = backend === :sparse ? distribute(Asp, A_part) : distribute(Ah, A_part)
        F = eigen(DA)
        @test F isa LinearAlgebra.Eigen
        @test F.values isa Vector
        @test F.vectors isa Dagger.DMatrix
        @test length(F.values) == 1
        @test size(F.vectors) == (n, 1)
        @test F.values[1] ≈ λmin rtol = 1e-6 atol = 1e-8

        x = eigen_first_vec(F)
        # True residual of the distributed pair; also matches the host residual.
        @test eigen_true_res(DA, F.values[1], x) < 1e-6
        @test norm(Ah * collect(x) - F.values[1] * collect(x)) < 1e-6

        @test eigvals(DA)[1] ≈ λmin rtol = 1e-6 atol = 1e-8
        @test eigvals(DA; nev=1, which=:SR)[1] ≈ F.values[1] rtol = 1e-5
    end

    @testset "few pairs and largest" begin
        DA = distribute(Asp, A_part)
        F2 = eigen(DA; nev=2, which=:SR)
        @test length(F2.values) == 2
        @test F2.values[1] ≈ href.values[1] rtol = 1e-5
        @test F2.values[2] ≈ href.values[2] rtol = 1e-5
        @test F2.values[1] <= F2.values[2]
        V2 = collect(F2.vectors)
        for j in 1:2
            @test norm(Ah * V2[:, j] - F2.values[j] * V2[:, j]) < 1e-5
        end

        Fl = eigen(DA; nev=1, which=:LR)
        @test Fl.values[1] ≈ λmax rtol = 1e-5
        @test eigen_true_res(DA, Fl.values[1], eigen_first_vec(Fl)) < 1e-5
    end

    @testset "Hermitian wrapper and Jacobi P" begin
        DA = distribute(Asp, A_part)
        Fh = eigen(Hermitian(DA); nev=1, which=:SR)
        @test Fh.values[1] ≈ λmin rtol = 1e-6
        @test eigen_true_res(DA, Fh.values[1], eigen_first_vec(Fh)) < 1e-6

        P = Dagger.JacobiPreconditioner(DA)
        Fp = eigen(DA; nev=1, which=:SR, P=P)
        @test Fp.values[1] ≈ λmin rtol = 1e-6
        @test eigen_true_res(DA, Fp.values[1], eigen_first_vec(Fp)) < 1e-6
    end

    @testset "matrix-free Projected" begin
        # Constant mode is *not* a kernel of the Dirichlet Laplacian, so this
        # only checks that a non-`AbstractMatrix` operator with `mul!` over
        # `DVector`s reaches the same LOBPCG path.
        DA = distribute(Asp, A_part)
        N = distribute(randn(n), Blocks(k))
        PA = Dagger.Projected(DA, N)
        F = eigen(PA; nev=1, which=:SR)
        x = eigen_first_vec(F)
        @test eigen_true_res(PA, F.values[1], x) < 1e-5
    end
end
