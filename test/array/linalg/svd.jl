using LinearAlgebra
# ──────────────────────────────────────────────────────────────────────
# Helper: validate a thin SVD factorization F of the dense matrix A_col
# ──────────────────────────────────────────────────────────────────────
function check_svd(A_col, F; tol=200.0)
    T = eltype(A_col)
    eps_val = eps(real(T))
    m, n = size(A_col)
    k = min(m, n)

    U  = collect(F.U)
    S  = F.S
    Vt = collect(F.Vt)

    @test size(U) == (m, k)
    @test length(S) == k
    @test size(Vt) == (k, n)

    # Reconstruction residual
    recon = U * Diagonal(S) * Vt
    res_f = opnorm(A_col - recon, 1) / (opnorm(A_col, 1) * max(m, n) * eps_val)
    @test res_f < tol

    # Orthonormality of singular vectors
    res_u = opnorm(U' * U - I, 1) / (k * eps_val)
    res_v = opnorm(Vt * Vt' - I, 1) / (k * eps_val)
    @test res_u < tol
    @test res_v < tol

    # Singular values: nonnegative, descending, and matching LAPACK
    @test all(>=(0), S)
    @test issorted(S; rev=true)
    @test S ≈ svdvals(A_col)
end

# ======================================================================
# 1. svd(DA) — varying shapes, block sizes, element types
# ======================================================================
@testset "Tile SVD: $T" for T in (Float32, Float64, ComplexF32, ComplexF64)
    @testset "Square" begin
        @testset "blocks=$bs" for bs in [(32, 32), (16, 32), (16, 16)]
            A = rand(T, 128, 128)
            DA = distribute(A, Blocks(bs...))
            check_svd(collect(DA), svd(DA))
            # svd must not modify its input
            @test collect(DA) ≈ A
        end
    end

    @testset "Tall" begin
        @testset "blocks=$bs" for bs in [(32, 32), (16, 32)]
            A = rand(T, 128, 64)
            DA = distribute(A, Blocks(bs...))
            check_svd(collect(DA), svd(DA))
        end
    end

    @testset "Wide" begin
        @testset "blocks=$bs" for bs in [(32, 32), (32, 16)]
            A = rand(T, 64, 128)
            DA = distribute(A, Blocks(bs...))
            check_svd(collect(DA), svd(DA))
        end
    end

    @testset "Irregular tiling" begin
        A = rand(T, 100, 60)
        DA = distribute(A, Blocks(24, 16))
        check_svd(collect(DA), svd(DA))
    end

    @testset "Single block-column" begin
        A = rand(T, 48, 40)
        DA = distribute(A, Blocks(48, 64))  # one column tile
        check_svd(collect(DA), svd(DA))
    end
end

# ======================================================================
# 2. In-place svd! (exercises the in-place path and input destruction)
# ======================================================================
@testset "In-place svd!: $T" for T in (Float64, ComplexF64)
    A = rand(T, 96, 96)
    DA = distribute(A, Blocks(32, 32))
    F = svd!(DA)
    check_svd(A, F)
end

# ======================================================================
# 3. svdvals / svdvals! agree with LAPACK and with svd
# ======================================================================
@testset "svdvals: $T" for T in (Float64, ComplexF64)
    @testset "shape=$sz" for sz in [(128, 128), (128, 48), (48, 128)]
        A = rand(T, sz...)
        DA = distribute(A, Blocks(32, 32))

        S = svdvals(DA)
        @test S ≈ svdvals(A)
        @test collect(DA) ≈ A            # svdvals must not modify input

        # svdvals agrees with the singular values from a full svd
        @test S ≈ svd(DA).S

        # in-place variant
        DA2 = distribute(A, Blocks(32, 32))
        @test svdvals!(DA2) ≈ svdvals(A)
    end
end

# ======================================================================
# 4. Known spectrum: reconstruct a matrix with prescribed singular values
# ======================================================================
@testset "Prescribed spectrum: $T" for T in (Float64, ComplexF64)
    n = 64
    svals = collect(range(10.0, 1.0; length=n))
    Q1, _ = qr(rand(T, n, n))
    Q2, _ = qr(rand(T, n, n))
    A = Matrix(Q1) * Diagonal(T.(svals)) * Matrix(Q2)'
    DA = distribute(A, Blocks(16, 16))
    S = svdvals(DA)
    @test S ≈ svals
end

# ======================================================================
# 5. full=true is rejected
# ======================================================================
@testset "full=true unsupported" begin
    DA = distribute(rand(Float64, 32, 32), Blocks(16, 16))
    @test_throws ArgumentError svd(DA; full=true)
end

# ======================================================================
# 6. Solve via SVD: \, ldiv!, inv
# ======================================================================
@testset "SVD solve: $T" for T in (Float64, ComplexF64)
    tol = T <: Complex ? 1e-8 : 1e-10

    @testset "shape=$sz" for sz in [(64, 64), (96, 48), (48, 96)]
        m, n = sz
        A = rand(T, m, n)
        DA = distribute(A, Blocks(16, 16))
        F = svd(DA)
        Fd = svd(A)

        # Vector RHS
        b = rand(T, m)
        Db = distribute(b, Blocks(16))
        x = F \ Db
        xd = Fd \ b
        @test size(x) == (n,)
        @test collect(x) ≈ xd rtol=tol

        X = zeros(Blocks(16), T, n)
        ldiv!(X, F, Db)
        @test collect(X) ≈ xd rtol=tol

        # Matrix RHS
        B = rand(T, m, 3)
        DB = distribute(B, Blocks(16, 3))
        X = F \ DB
        Xd = Fd \ B
        @test size(X) == (n, 3)
        @test collect(X) ≈ Xd rtol=tol

        X2 = zeros(Blocks(16, 3), T, n, 3)
        ldiv!(X2, F, DB)
        @test collect(X2) ≈ Xd rtol=tol
    end

    @testset "inv (square)" begin
        A = rand(T, 64, 64)
        DA = distribute(A, Blocks(16, 16))
        P = inv(svd(DA))
        @test collect(P) * A ≈ I rtol=tol
    end
end
