@testset "isapprox" begin
    A = rand(16, 16)

    U1 = UpperTriangular(DArray(A, Blocks(16, 16)))
    U2 = UpperTriangular(DArray(A, Blocks(16, 16)))
    @test isapprox(U1, U2)

    L1 = LowerTriangular(DArray(A, Blocks(16, 16)))
    L2 = LowerTriangular(DArray(A, Blocks(16, 16)))
    @test isapprox(L1, L2)
end

@testset "norm" begin
    A = rand(16, 16)
    DA = DArray(A)
    @test isapprox(norm(A), norm(DA))

    A = rand(16)
    DA = DArray(A)
    @test isapprox(norm(A), norm(DA))

    A = rand(16, 16, 16)
    DA = DArray(A)
    @test isapprox(norm(A), norm(DA))
end
