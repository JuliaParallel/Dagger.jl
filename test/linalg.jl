@testset "Linear Algebra" begin
    @testset "GEMM: $T" for T in (Float32, Float64, ComplexF32, ComplexF64)

        A = rand(T, 128, 128)
        B = rand(T, 128, 128)
        
        DA = view(A, Blocks(32, 32))
        DB = view(B, Blocks(32, 32))

        # Out-of-place gemm
        # No transA, No transB
        DC = DA * DB
        C = A * B
        @test collect(DC)  ≈ C

         # No transA,  transB
        DC = DA * DB'
        C = A * B'
        @test collect(DC)  ≈ C

        # transA,  No transB
        DC = DA' * DB
        C = A' * B
        @test collect(DC)  ≈ C

        # transA,   transB
        DC = DA' * DB'
        C = A' * B'
        @test collect(DC)  ≈ C

         # In-of-place gemm
         # No transA, No transB
        C= zeros(T, 128, 128)
        DC = view(C, Blocks(32, 32))
        mul!(C, A, B)
        mul!(DC, DA, DB)
        @test collect(DC)  ≈ C

        # No transA, transB
        C= zeros(T, 128, 128)
        DC = view(C, Blocks(32, 32))
        mul!(C, A, B')
        mul!(DC, DA, DB')
        @test collect(DC)  ≈ C    

        # transA, No transB
        C= zeros(T, 128, 128)
        DC = view(C, Blocks(32, 32))
        mul!(C, A', B)
        mul!(DC, DA', DB)
        @test collect(DC)  ≈ C 

        # transA, transB
        C= zeros(T, 128, 128)
        DC = view(C, Blocks(32, 32))
        mul!(C, A', B')
        mul!(DC, DA', DB')
        collect(DC)  ≈ C    

        # Out-of-place syrk
         # No trans,  trans
        DC = DA * DA'
        C = A * A'
        @test collect(DC)  ≈ C

        # trans,  No trans
        DC = DA' * DA
        C = A' * A
        @test collect(DC)  ≈ C

        # In-of-place syrk
        # No trans, trans
        C= zeros(T, 128, 128)
        DC = view(C, Blocks(32, 32))
        mul!(C, A, A')
        mul!(DC, DA, DA')
        @test collect(DC)  ≈ C    

        # trans, No trans
        C= zeros(T, 128, 128)
        DC = view(C, Blocks(32, 32))
        mul!(C, A', A)
        mul!(DC, DA', DA)
        @test collect(DC)  ≈ C 

    end

    @testset "Cholesky: $T" for T in (Float32, Float64, ComplexF32, ComplexF64)
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
        @test_throws_unwrap PosDefException cholesky(DA).U
    end
end
