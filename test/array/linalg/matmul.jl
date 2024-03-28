@testset "Matmul" begin
    X = rand(40, 40)
    tol = 1e-12

    X1 = distribute(X, Blocks(10, 20))
    X2 = X1'*X1
    X3 = X1*X1'
    X4 = X1*X1

    @test norm(collect(X2) - (X' * X)) < tol
    @test norm(collect(X3) - (X * X')) < tol
    @test norm(collect(X4) - (X * X)) < tol
    @test chunks(X2) |> size == (2, 2)
    @test chunks(X3) |> size == (4, 4)
    @test chunks(X4) |> size == (4, 2)
    @test map(x->size(x) == (20, 20), domainchunks(X2)) |> all
    @test map(x->size(x) == (10, 10), domainchunks(X3)) |> all
    @test map(x->size(x) == (10, 20), domainchunks(X4)) |> all

    @testset "Powers" begin
        x = rand(Blocks(4,4), 16, 16)
        @test collect(x^1) == collect(x)
        @test collect(x^2) == collect(x*x)
        @test collect(x^3) == collect(x*x*x)
    end

    @testset "GEMM: $T" for T in (Float32, Float64, ComplexF32, ComplexF64)
        A = rand(T, 128, 128)
        B = rand(T, 128, 128)

        DA = view(A, Blocks(32, 32))
        DB = view(B, Blocks(32, 32))

        ## Out-of-place gemm
        # No transA, No transB
        DC = DA * DB
        C = A * B
        @test collect(DC) ≈ C

        # No transA, transB
        DC = DA * DB'
        C = A * B'
        @test collect(DC) ≈ C

        # transA, No transB
        DC = DA' * DB
        C = A' * B
        @test collect(DC) ≈ C

        # transA, transB
        DC = DA' * DB'
        C = A' * B'
        @test collect(DC) ≈ C

        ## In-place gemm
        # No transA, No transB
        C = zeros(T, 128, 128)
        DC = view(C, Blocks(32, 32))
        mul!(C, A, B)
        mul!(DC, DA, DB)
        @test collect(DC) ≈ C

        # No transA, transB
        C = zeros(T, 128, 128)
        DC = view(C, Blocks(32, 32))
        mul!(C, A, B')
        mul!(DC, DA, DB')
        @test collect(DC) ≈ C

        # transA, No transB
        C = zeros(T, 128, 128)
        DC = view(C, Blocks(32, 32))
        mul!(C, A', B)
        mul!(DC, DA', DB)
        @test collect(DC) ≈ C

        # transA, transB
        C = zeros(T, 128, 128)
        DC = view(C, Blocks(32, 32))
        mul!(C, A', B')
        mul!(DC, DA', DB')
        collect(DC) ≈ C

        ## Out-of-place syrk
        # No trans, trans
        DC = DA * DA'
        C = A * A'
        @test collect(DC) ≈ C

        # trans, No trans
        DC = DA' * DA
        C = A' * A
        @test collect(DC) ≈ C

        ## In-place syrk
        # No trans, trans
        C = zeros(T, 128, 128)
        DC = distribute(C, Blocks(32, 32))
        mul!(C, A, A')
        mul!(DC, DA, DA')
        @test collect(DC) ≈ C

        # trans, No trans
        C = zeros(T, 128, 128)
        DC = distribute(C, Blocks(32, 32))
        mul!(C, A', A)
        mul!(DC, DA', DA)
        @test collect(DC) ≈ C
    end
end
