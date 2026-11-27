@testset "$T" for T in (Float32, Float64, ComplexF32, ComplexF64)
    D = rand(Blocks(4, 4), T, 32, 32)
    if !(T <: Complex)
        @test !issymmetric(D)
    end
    @test !ishermitian(D)

    A = rand(T, 128, 128)
    A = A * A'
    A[diagind(A)] .+= size(A, 1)
    B = copy(A)
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
    # Check that cholesky did not modify A or DA
    @test A ≈ DA ≈ B

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

@testset "Cholesky on the owner-computes path (opt-in) matches Datadeps" begin
    old = Dagger.OWNER_COMPUTES_LINALG[]
    try
        for T in (Float64, Float32, ComplexF64), (uplo, W) in (('L', LowerTriangular), ('U', UpperTriangular))
            n, nb = 96, 16
            X = rand(T, n, n); A = X * X' + n * I; A = (A + A') / 2
            DA = distribute(copy(A), Blocks(nb, nb))
            Dagger.owner_computes_linalg!(false)
            ref, info0 = LinearAlgebra._chol!(copy(DA), W)
            Dagger.owner_computes_linalg!(true)
            runs0 = Dagger.OWNER_COMPUTES_RUNS[]
            res, info1 = LinearAlgebra._chol!(copy(DA), W)
            @test Dagger.OWNER_COMPUTES_RUNS[] == runs0 + 1          # the layer really ran (the Datadeps fallback gives the same numbers)
            Dagger.owner_computes_linalg!(false)
            F = uplo == 'L' ? tril : triu
            @test info0 == 0 && info1 == 0
            @test norm(F(collect(res.data)) - F(collect(ref.data))) / norm(A) < (T <: Union{Float32,ComplexF32} ? 1e-4 : 1e-10)
        end
        # not positive definite: nonzero info, no exception, no hang
        A = rand(64, 64); A = A + A'; DA = distribute(A, Blocks(16, 16))
        Dagger.owner_computes_linalg!(true)
        _, info = LinearAlgebra._chol!(copy(DA), LowerTriangular)
        @test info > 0
        # unsupported tile shape: falls back to Datadeps
        DA2 = distribute(let X = rand(48, 48); X * X' + 48I end, Blocks(16, 12))
        runs1 = Dagger.OWNER_COMPUTES_RUNS[]
        _, i2 = LinearAlgebra._chol!(copy(DA2), LowerTriangular)
        @test i2 == 0
        @test Dagger.OWNER_COMPUTES_RUNS[] == runs1                   # ... and the unsupported shape did NOT use it
        # tiles that are not square, though the grid is (3 x 3 tiles of 12 x 13): also falls back
        DA3 = distribute(let X = rand(36, 36); X * X' + 36I end, Blocks(12, 13))
        _, i3 = LinearAlgebra._chol!(copy(DA3), LowerTriangular)
        @test i3 == 0
        @test Dagger.OWNER_COMPUTES_RUNS[] == runs1
    finally
        Dagger.owner_computes_linalg!(old)
    end
end
