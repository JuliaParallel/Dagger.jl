@testset "$T" for T in (Float32, Float64, ComplexF32, ComplexF64)
    D = rand(Blocks(4, 4), T, 32, 32)
    if !(T <: Complex)
        @test !issymmetric(D)
    end
    @test !ishermitian(D)

    A = rand(T, 128, 128)
    A = A * A'
    A[diagind(A)] .+= size(A, 1)
    DA = view(A, Blocks(32, 32))
    if !(T <: Complex)
        @test issymmetric(DA)
    end
    @test ishermitian(DA)

    # Out-of-place
    chol_A = cholesky(A)
    chol_DA = cholesky(DA)
    @test chol_DA isa Cholesky
    @test chol_A.L ≈ chol_DA.L
    @test chol_A.U ≈ chol_DA.U

    # In-place
    A_copy = copy(A)
    chol_A = cholesky!(A_copy)
    chol_DA = cholesky!(DA)
    @test chol_DA isa Cholesky
    @test chol_A.L ≈ chol_DA.L
    @test chol_A.U ≈ chol_DA.U
    # Check that changes propagated to A
    @test UpperTriangular(collect(DA)) ≈ UpperTriangular(collect(A))

    # Non-PosDef matrix
    A = rand(T, 128, 128)
    A = A * A'
    A[diagind(A)] .+= size(A, 1)
    A[1, 1] = -100
    DA = view(A, Blocks(32, 32))
    if !(T <: Complex)
        @test issymmetric(DA)
    end
    @test ishermitian(DA)
    @test_broken cholesky(DA).U == 42 # This should throw PosDefException
    #@test_throws_unwrap PosDefException cholesky(DA).U
end
