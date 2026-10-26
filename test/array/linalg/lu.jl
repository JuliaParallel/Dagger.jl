# Tolerance for the P*A ≈ L*U reconstruction check. NoPivot LU is numerically
# unstable (its error scales with the element growth factor, which can spike for
# random matrices), so it needs a much looser tolerance than pivoted LU. The
# value is chosen with headroom over the default `≈` tolerance (~sqrt(eps)),
# which the non-square NoPivot cases below already rely on.
function factorization_rtol(T, pivot)
    if T in (Float32, ComplexF32)
        return 1e-2
    elseif pivot == NoPivot()
        return 1e-7
    else
        return 1e-12
    end
end

function test_lu_pivot_blocks(::Type{T}, part, pivot) where T
    A = Matrix{T}(I, 13, 13) + rand(T, 13, 13) / 20
    F = lu(distribute(A, part), pivot)
    @test F.ipiv.partitioning == Blocks(part.blocksize[1])
    pivot == NoPivot() && @test collect(F.ipiv) == collect(1:13)
    b = rand(T, 13)
    @test collect(F \ distribute(b, Blocks(2))) ≈ A \ b
    B = rand(T, 13, 2)
    @test collect(F \ distribute(B, Blocks(2, 1))) ≈ A \ B
end

@testset "Pivot blocks follow factor rows" begin
    # Deliberately differ from AutoBlocks, including a partial final tile.
    for T in (Float64, ComplexF64), part in (Blocks(3, 3), Blocks(5, 3)), pivot in (NoPivot(), RowMaximum())
        test_lu_pivot_blocks(T, part, pivot)
    end
end

@testset "$T with $pivot" for T in (Float64, ComplexF64), pivot in (NoPivot(), RowMaximum())
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
        tol_fact = factorization_rtol(T, pivot)
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
    tol = T in (Float32, ComplexF32) ? 1e-2 : 1e-6
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
        tol_fact = factorization_rtol(T, pivot)
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
    tol = T in (Float32, ComplexF32) ? 1e-2 : 1e-6
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
