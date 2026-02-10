@testset "$T with $pivot" for T in (Float32, Float64, ComplexF32, ComplexF64), pivot in (NoPivot(), RowMaximum())
    A = rand(T, 128, 128)
    B = copy(A)
    DA = view(A, Blocks(64, 64))

    # Out-of-place
    lu_DA = lu(DA, pivot)
    @test lu_DA isa LU{T,DMatrix{T},DVector{Int}}

    # Verify the factorization property: P*A = L*U
    # Convert to regular arrays for comparison
    L_DA = Array(lu_DA.L)
    U_DA = Array(lu_DA.U)
    P_DA = Array(lu_DA.P)
    if !(T in (Float32, ComplexF32) && pivot == NoPivot()) # FIXME: NoPivot is unstable for FP32
        tol_fact = T in (Float32, ComplexF32) ? 1e-4 : 1e-12
        @test P_DA * B ≈ L_DA * U_DA rtol=tol_fact
    end
    @test istriu(U_DA)
    @test istril(L_DA)
    @test isapprox(P_DA' * P_DA, I)
    @test all(x->x == 0 || x == 1, P_DA)
    @test all(x->abs(x) == 1, det(P_DA))

    # Verify the factorization can solve linear systems correctly
    b = rand(T, 128)
    x_direct = B \ b
    x_lu = lu_DA \ b
    tol = T in (Float32, ComplexF32) ? 1e-3 : 1e-6
    @test Array(x_lu) ≈ x_direct rtol=tol

    # Check that lu did not modify A or DA
    @test A ≈ DA ≈ B

    # In-place
    A_copy = copy(A)
    DA_copy = view(A_copy, Blocks(64, 64))
    lu_DA = lu!(DA_copy, pivot)
    @test lu_DA isa LU{T,DMatrix{T},DVector{Int}}

    # Verify the factorization property: P*A = L*U
    L_DA = Array(lu_DA.L)
    U_DA = Array(lu_DA.U)
    P_DA = Array(lu_DA.P)
    if !(T in (Float32, ComplexF32) && pivot == NoPivot()) # FIXME: NoPivot is unstable for FP32
        tol_fact = T in (Float32, ComplexF32) ? 1e-4 : 1e-12
        @test P_DA * B ≈ L_DA * U_DA rtol=tol_fact
    end
    @test istriu(U_DA)
    @test istril(L_DA)
    @test isapprox(P_DA' * P_DA, I)
    @test all(x->x == 0 || x == 1, P_DA)
    @test all(x->abs(x) == 1, det(P_DA))

    # Verify the factorization can solve linear systems correctly
    b = rand(T, 128)
    x_direct = B \ b
    x_lu = lu_DA \ b
    tol = T in (Float32, ComplexF32) ? 1e-3 : 1e-6
    @test Array(x_lu) ≈ x_direct rtol=tol

    # Check that changes propagated to A
    @test !(B ≈ A_copy)

    # Non-square block sizes
    A_nonsq = rand(T, 128, 128)
    DA_nonsq = view(A_nonsq, Blocks(64, 32))
    lu_nonsq = lu(DA_nonsq, pivot)
    @test lu_nonsq isa LU{T,DMatrix{T},DVector{Int}}
    # Verify factorization property for non-square blocks
    L_nonsq = Array(lu_nonsq.L)
    U_nonsq = Array(lu_nonsq.U)
    P_nonsq = Array(lu_nonsq.P)
    if !(T in (Float32, ComplexF32) && pivot == NoPivot())
        @test P_nonsq * A_nonsq ≈ L_nonsq * U_nonsq
    end
    @test istriu(U_nonsq)
    @test istril(L_nonsq)
    @test isapprox(P_nonsq' * P_nonsq, I)
    @test all(x->x == 0 || x == 1, P_nonsq)
    @test all(x->abs(x) == 1, det(P_nonsq))

    @test lu!(rand(Blocks(64, 32), T, 128, 128), pivot) isa LU{T,DMatrix{T},DVector{Int}}

    # Singular Values
    @test_throws LinearAlgebra.SingularException lu(ones(Blocks(64,64), T, 128, 128)) # FIXME: NoPivot needs to handle info
end