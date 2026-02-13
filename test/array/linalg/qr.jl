import Dagger: geqrf!, porgqr!, pormqr!, cageqrf!

# ──────────────────────────────────────────────────────────────────────
# Helper: run residual checks on a QR factorization result
# ──────────────────────────────────────────────────────────────────────
function check_qr_residuals(A_col, DQ, DR; tol=2.0)
    T = eltype(A_col)
    eps_val = eps(real(T))

    R   = collect(DR)
    QR  = collect(DQ * DR)
    CDI = collect(DQ' * DQ)

    res1 = opnorm(A_col - QR, 1) / (opnorm(A_col, 1) * max(size(A_col)...) * eps_val)
    res2 = opnorm(I - CDI, 1) / (size(A_col, 2) * eps_val)

    @test res1 < tol   # Factorization residual
    @test res2 < tol   # Orthogonality residual
    @test triu(R) ≈ R  # R is upper triangular
end

# ======================================================================
# 1. Basic qr(DA) — varying shapes and block sizes (p=1, ib=1)
# ======================================================================
@testset "Tile QR:  $T" for T in (Float32, Float64, ComplexF32, ComplexF64)
    @testset "Square matrices" begin
        @testset "blocks=$bs" for bs in [(32,32), (16,32), (32,16)]
            A = rand(T, 128, 128)
            DA = distribute(A, Blocks(bs...))
            DQ, DR = qr(DA)
            check_qr_residuals(collect(DA), DQ, DR)
        end
    end

    @testset "Tall matrices" begin
        @testset "blocks=$bs" for bs in [(32,32), (16,32), (32,16)]
            A = rand(T, 128, 64)
            DA = distribute(A, Blocks(bs...))
            DQ, DR = qr(DA)
            check_qr_residuals(collect(DA), DQ, DR)
        end
    end

    @testset "Wide matrices" begin
        @testset "blocks=$bs" for bs in [(32,32), (16,32), (32,16)]
            A = rand(T, 64, 128)
            DA = distribute(A, Blocks(bs...))
            DQ, DR = qr(DA)
            check_qr_residuals(collect(DA), DQ, DR)
        end
    end
end

# ======================================================================
# 2. In-place qr! (ensures qr! path is exercised directly)
# ======================================================================
@testset "In-place qr!: $T" for T in (Float32, Float64, ComplexF32, ComplexF64)
    A = rand(T, 128, 128)
    DA = distribute(A, Blocks(32,32))
    DA_copy = copy(DA)
    F = qr!(DA_copy)

    # F is a QRCompactWY; extract Q and R
    DQ_mat = DMatrix(F.Q)
    DR = DArray{T}(DA_copy.partitioning, undef, size(DA_copy))
    # Extract upper triangle from the factored DA_copy into DR
    R_local = triu(collect(DA_copy))
    DR = distribute(R_local, DA_copy.partitioning)

    QR  = collect(DQ_mat) * R_local
    eps_val = eps(real(T))
    res = opnorm(A - QR, 1) / (opnorm(A, 1) * max(size(A)...) * eps_val)
    @test res < 2.0
end

# ======================================================================
# 3. CAQR with p > 1 (communication-avoiding QR)
# ======================================================================
@testset "CAQR p=$p: $T" for T in (Float64, ComplexF64),
                               p in (2, 4)
    # mt must be divisible by p; 128/32 = 4 tiles → p ∈ {2, 4} work
    A = rand(T, 128, 64)
    DA = distribute(A, Blocks(32, 32))
    DA_copy = copy(DA)
    F = qr!(DA_copy; p=p)
    DQ, DR = F.Q, triu(collect(DA_copy))

    DQ_mat = collect(DMatrix(DQ))
    QR = DQ_mat * DR
    eps_val = eps(real(T))
    res1 = opnorm(A - QR, 1) / (opnorm(A, 1) * max(size(A)...) * eps_val)
    CDI  = DQ_mat' * DQ_mat
    res2 = opnorm(I - CDI, 1) / (size(A, 2) * eps_val)

    @test res1 < 2.0
    @test res2 < 2.0
    @test triu(DR) ≈ DR
end

# ======================================================================
# 4. pormqr! — left and right multiply by Q and Q'
# ======================================================================
@testset "pormqr! side=$side trans=$trans: $T" for T in (Float64, ComplexF64),
                                                    side in ('L', 'R'),
                                                    trans in ('N', T <: Complex ? 'C' : 'T')
    A = rand(T, 128, 128)
    DA = distribute(A, Blocks(32,32))
    F = qr(DA)
    Q_dense = collect(F.Q)

    if side == 'L'
        # Q * C  or  Q' * C
        C = rand(T, 128, 64)
        DC = distribute(C, Blocks(32, 32))
        pormqr!(side, trans, F.Q.factors, F.Q.T, DC)
        Qt = trans == 'N' ? Q_dense : Q_dense'
        expected = Qt * C
    else
        # C * Q  or  C * Q'
        C = rand(T, 64, 128)
        DC = distribute(C, Blocks(32, 32))
        pormqr!(side, trans, F.Q.factors, F.Q.T, DC)
        Qt = trans == 'N' ? Q_dense : Q_dense'
        expected = C * Qt
    end

    eps_val = eps(real(T))
    res = opnorm(collect(DC) - expected, 1) / (opnorm(expected, 1) * max(size(C)...) * eps_val)
    @test res < 2.0
end

# ======================================================================
# 5. lmul! / rmul! dispatch  (Q*DA, Q'*DA, DA*Q, DA*Q')
# ======================================================================
@testset "lmul!/rmul!: $T" for T in (Float64, ComplexF64)
    A = rand(T, 128, 128)
    DA = distribute(A, Blocks(32,32))
    F = qr(DA)
    Q_dense = collect(F.Q)
    trans = T <: Complex ? 'C' : 'T'

    # lmul! Q * B  (DMatrix target)
    @testset "lmul! Q*DMatrix" begin
        B = rand(T, 128, 64)
        DB = distribute(B, Blocks(32, 32))
        lmul!(F.Q, DB)
        @test collect(DB) ≈ Q_dense * B
    end

    # lmul! Q' * B  (DMatrix target)
    @testset "lmul! Q'*DMatrix" begin
        B = rand(T, 128, 64)
        DB = distribute(B, Blocks(32, 32))
        lmul!(F.Q', DB)
        @test collect(DB) ≈ Q_dense' * B
    end

    # rmul! B * Q  (DMatrix target)
    @testset "rmul! DMatrix*Q" begin
        B = rand(T, 64, 128)
        DB = distribute(B, Blocks(32, 32))
        rmul!(DB, F.Q)
        @test collect(DB) ≈ B * Q_dense
    end

    # rmul! B * Q'  (DMatrix target)
    @testset "rmul! DMatrix*Q'" begin
        B = rand(T, 64, 128)
        DB = distribute(B, Blocks(32, 32))
        rmul!(DB, F.Q')
        @test collect(DB) ≈ B * Q_dense'
    end

    # lmul! Q * dense  (AbstractMatrix target)
    @testset "lmul! Q*dense" begin
        B = rand(T, 128, 64)
        B_copy = copy(B)
        lmul!(F.Q, B_copy)
        @test B_copy ≈ Q_dense * B
    end

    # lmul! Q' * dense  (AbstractMatrix target)
    @testset "lmul! Q'*dense" begin
        B = rand(T, 128, 64)
        B_copy = copy(B)
        lmul!(F.Q', B_copy)
        @test B_copy ≈ Q_dense' * B
    end

    # rmul! dense * Q  (AbstractMatrix target)
    @testset "rmul! dense*Q" begin
        B = rand(T, 64, 128)
        B_copy = copy(B)
        rmul!(B_copy, F.Q)
        @test B_copy ≈ B * Q_dense
    end

    # rmul! dense * Q'  (AbstractMatrix target)
    @testset "rmul! dense*Q'" begin
        B = rand(T, 64, 128)
        B_copy = copy(B)
        rmul!(B_copy, F.Q')
        @test B_copy ≈ B * Q_dense'
    end
end

# ======================================================================
# 6. Scalar multiplication:  Q*s, s*Q, Q'*s, s*Q'
# ======================================================================
@testset "Scalar * Q: $T" for T in (Float64, ComplexF64)
    A = rand(T, 64, 64)
    DA = distribute(A, Blocks(32,32))
    F = qr(DA)
    Q_dense = collect(F.Q)
    s = T(3.0)

    @test collect(F.Q * s) ≈ Q_dense * s
    @test collect(s * F.Q) ≈ Q_dense * s
    @test collect(F.Q' * s) ≈ Q_dense' * s
    @test collect(s * F.Q') ≈ Q_dense' * s
end

# ======================================================================
# 7. DMatrix(Q) and DMatrix(Q') explicit construction
# ======================================================================
@testset "DMatrix(Q) / DMatrix(Q'): $T" for T in (Float64, ComplexF64)
    A = rand(T, 128, 64)
    DA = distribute(A, Blocks(32,32))
    F = qr(DA)

    DQ = DMatrix(F.Q)
    DQa = DMatrix(F.Q')

    Q_dense  = collect(DQ)
    Qa_dense = collect(DQa)
    eps_val  = eps(real(T))

    # Q should be orthogonal
    CDI = Q_dense' * Q_dense
    res = opnorm(I - CDI, 1) / (size(A, 2) * eps_val)
    @test res < 2.0

    # Q' should equal adjoint of Q
    @test Qa_dense ≈ Q_dense'
end

# ======================================================================
# 8. collect(Q) and collect(Q')
# ======================================================================
@testset "collect(Q) / collect(Q'): $T" for T in (Float64, ComplexF64)
    A = rand(T, 64, 64)
    DA = distribute(A, Blocks(32,32))
    F = qr(DA)

    Q_col  = collect(F.Q)
    Qa_col = collect(F.Q')

    @test Q_col' ≈ Qa_col
    @test size(Q_col) == size(A)
end

# ======================================================================
# 9. geqrf! / porgqr! low-level API
# ======================================================================
@testset "geqrf! + porgqr!: $T" for T in (Float64, ComplexF64)
    A = rand(T, 128, 64)
    DA = distribute(A, Blocks(32,32))
    DA_copy = copy(DA)

    nb = DA_copy.partitioning.blocksize[2]
    ib = 1
    lm, ln = Dagger.meas_ws(DA_copy, ib)
    Tm = DArray{T}(Blocks(ib, nb), undef, (lm, ln))

    geqrf!(DA_copy, Tm)

    # Build Q explicitly via porgqr!
    DQ = DMatrix(DA_copy.partitioning, I*one(T), (size(A, 1), size(A, 1)))
    porgqr!('N', DA_copy, Tm, DQ)

    Q_dense = collect(DQ)
    R = triu(collect(DA_copy))
    eps_val = eps(real(T))

    QR = Q_dense * R
    res1 = opnorm(A - QR, 1) / (opnorm(A, 1) * max(size(A)...) * eps_val)
    CDI = Q_dense' * Q_dense
    res2 = opnorm(I - CDI, 1) / (size(A, 1) * eps_val)

    @test res1 < 2.0
    @test res2 < 2.0
end

# ======================================================================
# 10. static/traversal keyword options
# ======================================================================
@testset "Scheduler options (static=$s, traversal=$t): $T" for T in (Float64,),
        s in (true,),
        t in (:inorder,)
    A = rand(T, 128, 64)
    DA = distribute(A, Blocks(32,32))
    DA_copy = copy(DA)

    nb = DA_copy.partitioning.blocksize[2]
    ib = 1
    lm, ln = Dagger.meas_ws(DA_copy, ib)
    Tm = DArray{T}(Blocks(ib, nb), undef, (lm, ln))

    geqrf!(DA_copy, Tm; static=s, traversal=t)

    DQ = DMatrix(DA_copy.partitioning, I*one(T), (size(A, 1), size(A, 1)))
    porgqr!('N', DA_copy, Tm, DQ; static=s, traversal=t)

    Q_dense = collect(DQ)
    R = triu(collect(DA_copy))
    eps_val = eps(real(T))

    QR = Q_dense * R
    res = opnorm(A - QR, 1) / (opnorm(A, 1) * max(size(A)...) * eps_val)
    @test res < 2.0
end

# ======================================================================
# 11. CAQR pormqr! — left and right multiply with p > 1
# ======================================================================
@testset "CAQR pormqr! p=$p side=$side trans=$trans: $T" for T in (Float64, ComplexF64),
        p in (2, 4),
        side in ('L', 'R'),
        trans in ('N', T <: Complex ? 'C' : 'T')
    # Factor with CAQR
    A = rand(T, 128, 128)
    DA = distribute(A, Blocks(32, 32))
    DA_copy = copy(DA)
    F = qr!(DA_copy; p=p)
    Q_dense = collect(DMatrix(F.Q))

    if side == 'L'
        C = rand(T, 128, 64)
        DC = distribute(C, Blocks(32, 32))
        pormqr!(side, trans, F.Q.factors, F.Q.T, DC; p=p)
        Qt = trans == 'N' ? Q_dense : Q_dense'
        expected = Qt * C
    else
        C = rand(T, 64, 128)
        DC = distribute(C, Blocks(32, 32))
        pormqr!(side, trans, F.Q.factors, F.Q.T, DC; p=p)
        Qt = trans == 'N' ? Q_dense : Q_dense'
        expected = C * Qt
    end

    eps_val = eps(real(T))
    res = opnorm(collect(DC) - expected, 1) / (opnorm(expected, 1) * max(size(C)...) * eps_val)
    @test res < 2.0
end

# ======================================================================
# 12. CAQR lmul!/rmul! with p > 1
# ======================================================================
@testset "CAQR lmul!/rmul! p=$p: $T" for T in (Float64, ComplexF64),
        p in (2, 4)
    A = rand(T, 128, 128)
    DA = distribute(A, Blocks(32, 32))
    DA_copy = copy(DA)
    F = qr!(DA_copy; p=p)
    Q_dense = collect(DMatrix(F.Q))

    @testset "lmul! Q*DMatrix" begin
        B = rand(T, 128, 64)
        DB = distribute(B, Blocks(32, 32))
        lmul!(F.Q, DB)
        @test collect(DB) ≈ Q_dense * B
    end

    @testset "lmul! Q'*DMatrix" begin
        B = rand(T, 128, 64)
        DB = distribute(B, Blocks(32, 32))
        lmul!(F.Q', DB)
        @test collect(DB) ≈ Q_dense' * B
    end

    @testset "rmul! DMatrix*Q" begin
        B = rand(T, 64, 128)
        DB = distribute(B, Blocks(32, 32))
        rmul!(DB, F.Q)
        @test collect(DB) ≈ B * Q_dense
    end

    @testset "rmul! DMatrix*Q'" begin
        B = rand(T, 64, 128)
        DB = distribute(B, Blocks(32, 32))
        rmul!(DB, F.Q')
        @test collect(DB) ≈ B * Q_dense'
    end
end

# ======================================================================
# 13. CAQR with Float32/ComplexF32
# ======================================================================
@testset "CAQR single-precision p=$p: $T" for T in (Float32, ComplexF32),
        p in (2, 4)
    A = rand(T, 128, 64)
    DA = distribute(A, Blocks(32, 32))
    DA_copy = copy(DA)
    F = qr!(DA_copy; p=p)
    DQ, DR = F.Q, triu(collect(DA_copy))

    DQ_mat = collect(DMatrix(DQ))
    QR = DQ_mat * DR
    eps_val = eps(real(T))
    res1 = opnorm(A - QR, 1) / (opnorm(A, 1) * max(size(A)...) * eps_val)
    CDI  = DQ_mat' * DQ_mat
    res2 = opnorm(I - CDI, 1) / (size(A, 2) * eps_val)

    @test res1 < 2.0
    @test res2 < 2.0
    @test triu(DR) ≈ DR
end

# ======================================================================
# 14. CAQR with square and wide matrices
# ======================================================================
@testset "CAQR shapes p=$p: $T" for T in (Float64,), p in (2, 4)
    @testset "Square 128×128" begin
        A = rand(T, 128, 128)
        DA = distribute(A, Blocks(32, 32))
        DA_copy = copy(DA)
        F = qr!(DA_copy; p=p)
        DQ_mat = collect(DMatrix(F.Q))
        R = triu(collect(DA_copy))
        eps_val = eps(T)
        res1 = opnorm(A - DQ_mat * R, 1) / (opnorm(A, 1) * 128 * eps_val)
        res2 = opnorm(I - DQ_mat' * DQ_mat, 1) / (128 * eps_val)
        @test res1 < 2.0
        @test res2 < 2.0
    end
end

# ======================================================================
# 15. CAQR extreme p = mt (one tile per domain)
# ======================================================================
@testset "CAQR p=mt: $T" for T in (Float64, ComplexF64)
    # 128/32 = 4 tiles → p = 4 = mt for a 128×32 tall matrix
    A = rand(T, 128, 32)
    DA = distribute(A, Blocks(32, 32))
    DA_copy = copy(DA)
    p = size(DA.chunks, 1)  # p = mt
    F = qr!(DA_copy; p=p)
    DQ_mat = collect(DMatrix(F.Q))
    R = triu(collect(DA_copy))
    eps_val = eps(real(T))
    res1 = opnorm(A - DQ_mat * R, 1) / (opnorm(A, 1) * max(size(A)...) * eps_val)
    res2 = opnorm(I - DQ_mat' * DQ_mat, 1) / (size(A, 2) * eps_val)
    @test res1 < 2.0
    @test res2 < 2.0
end

# ======================================================================
# 16. CAQR porgqr! direct call with p > 1 (both trans='N' and trans='T'/'C')
# ======================================================================
@testset "CAQR porgqr! direct p=$p trans=$trans: $T" for T in (Float64, ComplexF64),
        p in (2, 4),
        trans in ('N', T <: Complex ? 'C' : 'T')
    A = rand(T, 128, 64)
    DA = distribute(A, Blocks(32, 32))
    DA_copy = copy(DA)
    F = qr!(DA_copy; p=p)

    DQ = DMatrix(F.Q.factors.partitioning, I*one(T), (size(A, 1), size(A, 1)))
    porgqr!(trans, F.Q.factors, F.Q.T, DQ; p=p)

    Q_dense = collect(DQ)
    eps_val = eps(real(T))

    if trans == 'N'
        R = triu(collect(DA_copy))
        QR = Q_dense * R
        res1 = opnorm(A - QR, 1) / (opnorm(A, 1) * max(size(A)...) * eps_val)
        @test res1 < 2.0
    end

    # Orthogonality: Q'*Q ≈ I regardless of trans
    # For trans != 'N', Q_dense is already Q', so Q_dense * Q_dense' ≈ I
    if trans == 'N'
        CDI = Q_dense' * Q_dense
    else
        CDI = Q_dense * Q_dense'
    end
    res2 = opnorm(I - CDI, 1) / (size(A, 2) * eps_val)
    @test res2 < 2.0
end

# ======================================================================
# 17. cageqrf! direct call
# ======================================================================
@testset "cageqrf! direct p=$p: $T" for T in (Float64, ComplexF64),
        p in (2, 4)
    A = rand(T, 128, 64)
    DA = distribute(A, Blocks(32, 32))
    DA_copy = copy(DA)

    nb = DA_copy.partitioning.blocksize[2]
    ib = 1
    lm, ln = Dagger.meas_ws(DA_copy, ib)
    Tm = DArray{T}(Blocks(ib, nb), undef, (lm, 2 * ln))  # 2× for tree T

    cageqrf!(DA_copy, Tm; p=p)

    # Build Q
    DQ = DMatrix(DA_copy.partitioning, I*one(T), (size(A, 1), size(A, 1)))
    porgqr!('N', DA_copy, Tm, DQ; p=p)

    Q_dense = collect(DQ)
    R = triu(collect(DA_copy))
    eps_val = eps(real(T))

    QR = Q_dense * R
    res1 = opnorm(A - QR, 1) / (opnorm(A, 1) * max(size(A)...) * eps_val)
    CDI = Q_dense' * Q_dense
    res2 = opnorm(I - CDI, 1) / (size(A, 1) * eps_val)

    @test res1 < 2.0
    @test res2 < 2.0
end

# ======================================================================
# 18. cageqrf! error on irregular tiling with p > 1
# ======================================================================
@testset "cageqrf! rejects irregular tiling" begin
    A = rand(Float64, 100, 60)
    DA = distribute(A, Blocks(30, 20))  # non-square tiles
    DA_copy = copy(DA)

    nb = 20
    ib = 1
    lm = ib * cld(100, 30)
    ln = nb * cld(60, 20)
    Tm = DArray{Float64}(Blocks(ib, nb), undef, (lm, 2 * ln))

    @test_throws ArgumentError cageqrf!(DA_copy, Tm; p=2)
end

# ======================================================================
# 19. Edge case — single tile
# ======================================================================
@testset "Single tile QR: $T" for T in (Float64, ComplexF64)
    A = rand(T, 32, 32)
    DA = distribute(A, Blocks(32, 32))
    DQ, DR = qr(DA)
    check_qr_residuals(A, DQ, DR)
end

# ======================================================================
# 20. Edge case — matrix smaller than block size
# ======================================================================
@testset "Smaller than block QR: $T" for T in (Float64, ComplexF64)
    A = rand(T, 16, 16)
    DA = distribute(A, Blocks(32, 32))
    DQ, DR = qr(DA)
    check_qr_residuals(A, DQ, DR)
end

# ======================================================================
# 21. QR least-squares solve: A x ≈ b
# ======================================================================
@testset "QR least-squares: $T" for T in (Float64, ComplexF64)
    m, n = 128, 64
    A = rand(T, m, n)
    x_true = rand(T, n)
    b = A * x_true

    DA = distribute(A, Blocks(32, 32))
    F = qr(DA)

    # Solve via Q'b then back-substitution
    Q_dense = collect(F.Q)
    R = collect(F.R)
    Qtb = Q_dense' * b
    x_solved = R[1:n, 1:n] \ Qtb[1:n]

    @test x_solved ≈ x_true rtol=1e-8
end

# ======================================================================
# 22. CAQR least-squares solve with p > 1
# ======================================================================
@testset "CAQR least-squares p=$p: $T" for T in (Float64,), p in (2, 4)
    m, n = 128, 64
    A = rand(T, m, n)
    x_true = rand(T, n)
    b = A * x_true

    DA = distribute(A, Blocks(32, 32))
    DA_copy = copy(DA)
    F = qr!(DA_copy; p=p)

    Q_dense = collect(DMatrix(F.Q))
    R = triu(collect(DA_copy))
    Qtb = Q_dense' * b
    x_solved = R[1:n, 1:n] \ Qtb[1:n]

    @test x_solved ≈ x_true rtol=1e-8
end
