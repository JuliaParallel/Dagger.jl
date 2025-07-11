@testset "$T with $pivot" for T in (Float32, Float64, ComplexF32, ComplexF64), pivot in (NoPivot(), RowMaximum())
    A = rand(T, 128, 128)
    B = copy(A)
    DA = view(A, Blocks(64, 64))

    # Out-of-place
    lu_A = lu(A, pivot)
    lu_DA = lu(DA, pivot)
    @test lu_DA isa LU{T,DMatrix{T},DVector{Int}}
    if !(T in (Float32, ComplexF32) && pivot == NoPivot()) # FIXME: NoPivot is unstable for FP32
        @test lu_A.L ≈ lu_DA.L
        @test lu_A.U ≈ lu_DA.U
    end
    @test lu_A.P ≈ lu_DA.P
    @test lu_A.p ≈ lu_DA.p
    # Check that lu did not modify A or DA
    @test A ≈ DA ≈ B

    # In-place
    A_copy = copy(A)
    lu_A = lu!(A_copy, pivot)
    lu_DA = lu!(DA, pivot)
    @test lu_DA isa LU{T,DMatrix{T},DVector{Int}}
    if !(T in (Float32, ComplexF32) && pivot == NoPivot()) # FIXME: NoPivot is unstable for FP32
        @test lu_A.L ≈ lu_DA.L
        @test lu_A.U ≈ lu_DA.U
    end
    @test lu_A.P ≈ lu_DA.P
    @test lu_A.p ≈ lu_DA.p
    # Check that changes propagated to A
    @test DA ≈ A
    @test !(B ≈ A)

    # Non-square block sizes
    @test lu(rand(Blocks(64, 32), T, 128, 128), pivot) isa LU{T,DMatrix{T},DVector{Int}}
    @test lu!(rand(Blocks(64, 32), T, 128, 128), pivot) isa LU{T,DMatrix{T},DVector{Int}}

    # Singular Values
    @test_throws LinearAlgebra.SingularException lu(ones(Blocks(64,64), T, 128, 128)) # FIXME: NoPivot needs to handle info
end