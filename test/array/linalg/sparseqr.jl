# Sparse QR must gather-then-SPQR, not tiled dense Compact-WY (that densifies).
# Residual is ‖Ax−b‖ / ‖A x_ref − b‖ against host `qr(A) \ b`, not only a
# factorization type check.
#
#     julia test/runtests.jl --test array/linalg/sparseqr

function relres_ls(A, x, b)
    return norm(A * x - b)
end

function fullrank_tall(T, m, n)
    A = sprand(T, m, n, 0.25)
    @inbounds for i in 1:n
        A[i, i] += T(4)
    end
    return A
end

@testset "sparse QR" begin
    @testset "overdetermined least squares" begin
        m, n, k = 40, 16, 8
        Ah = fullrank_tall(Float64, m, n)
        b = rand(m)
        xref = qr(Ah) \ b
        DA = distribute(Ah, Blocks(k, k))
        Db = distribute(b, Blocks(k))

        F = qr(DA)
        @test F isa Dagger.DaggerSparseQR
        @test !(F isa LinearAlgebra.QRCompactWY)
        @test size(F) == (m, n)

        x = F \ Db
        @test x isa Dagger.DVector
        @test length(x) == n
        @test collect(x) ≈ xref
        @test relres_ls(Ah, collect(x), b) ≈ relres_ls(Ah, xref, b) atol=1e-10

        y = similar(x)
        LinearAlgebra.ldiv!(y, F, Db)
        @test collect(y) ≈ xref

        F! = qr!(DA)
        @test F! isa Dagger.DaggerSparseQR
        @test collect(F! \ Db) ≈ xref

        # Dense tiled keywords are not SPQR knobs.
        @test_throws ArgumentError qr!(copy(DA); ib=8)
    end

    @testset "square sparse QR \\ b" begin
        n, k = 32, 8
        Ah = SparseArrays.spdiagm(
            -1 => fill(-1.0, n - 1),
             0 => fill(4.0, n),
             1 => fill(-1.0, n - 1),
        )
        b = rand(n)
        xref = qr(Ah) \ b
        DA = distribute(Ah, Blocks(k, k))
        Db = distribute(b, Blocks(k))
        x = qr(DA) \ Db
        @test collect(x) ≈ xref
        @test relres_ls(Ah, collect(x), b) < 1e-10
    end

    @testset "dense qr is still Compact-WY" begin
        n, k = 16, 8
        Ah = rand(n, n)
        DA = distribute(Ah, Blocks(k, k))
        F = qr(DA)
        @test F isa LinearAlgebra.QRCompactWY
        Q, R = F
        @test collect(Q * R) ≈ Ah
    end

    @testset "multi-RHS least squares" begin
        m, n, k, p = 24, 8, 8, 3
        Ah = fullrank_tall(Float64, m, n)
        Bh = rand(m, p)
        Xref = qr(Ah) \ Bh
        DA = distribute(Ah, Blocks(k, k))
        DB = distribute(Bh, Blocks(k, p))
        X = qr(DA) \ DB
        @test collect(X) ≈ Xref
        @test norm(Ah * collect(X) - Bh) ≈ norm(Ah * Xref - Bh) atol=1e-10
    end
end
