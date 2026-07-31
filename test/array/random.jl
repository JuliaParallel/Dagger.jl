using Distributions, Statistics

@testset "rand!" begin
    X = zeros(Blocks(10,10), Float64, 40, 40)

    Random.rand!(X)
    @test all(0.0 .<= X .<= 1.0)
    @test sum(X) > 0.01

    Random.rand!(Uniform(1, 10), X)
    @test all(1.0 .<= X .<= 10.0)
    @test sum(X) > length(X) + 0.1

    for RNGT in (MersenneTwister, Xoshiro)
        Random.rand!(RNGT(1234), X)
        @test all(0.0 .<= X .<= 1.0)
        @test sum(X) > 0.01

        Random.rand!(RNGT(1234), Uniform(1,10), X)
        @test all(1.0 .<= X .<= 10.0)
        @test sum(X) > length(X) + 0.1

        Random.randn!(X)
        @test sum(X) <= 2*length(X)

        Random.randn!(RNGT(1234), X)
        @test sum(X) <= 2*length(X)
        @test all(X .!= 0.0)
    end
end
