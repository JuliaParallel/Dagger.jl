@testset "$T" for T in (Float32, Float64, ComplexF32, ComplexF64)
    N = 32
    bs = 16

    A = rand(T, N, N)
    B = rand(T, N, N)
    DA = DArray(A, Blocks(bs, bs))
    DB = DArray(B, Blocks(bs, bs))

    @testset "DMatrix +" begin
        DC = DA + DB
        @test DC isa DArray
        @test collect(DC) == A + B
    end

    @testset "DMatrix -" begin
        DC = DA - DB
        @test DC isa DArray
        @test collect(DC) == A - B
    end

    a = rand(T, N)
    b = rand(T, N)
    Da = DArray(a, Blocks(bs))
    Db = DArray(b, Blocks(bs))

    @testset "DVector +" begin
        Dc = Da + Db
        @test Dc isa DArray
        @test collect(Dc) == a + b
    end

    @testset "DVector -" begin
        Dc = Da - Db
        @test Dc isa DArray
        @test collect(Dc) == a - b
    end
end
