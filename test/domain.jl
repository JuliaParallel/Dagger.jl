import Dagger: indexes, project

@testset "UnitDomain" begin
    @test domain(1) == UnitDomain()
    @test !isempty(UnitDomain())
end

@testset "ArrayDomain" begin
    @test domain(rand(10, 10)) == DenseDomain((1:10, 1:10))

    d1 = domain(Array{Int}(10, 10))
    @test indexes(d1) == (1:10, 1:10)
    @test size(d1) == (10, 10,)
    @test length(d1) == 100

    d2 = DenseDomain(5:10, 5:10)
    d3 = DenseDomain((8:12, 1:8))
    @testset "Domain methods for DenseDomain" begin
        @test intersect(d2,d2) == d2
        @test intersect(d2,d1) == d2
        @test intersect(d1,d2) == d2
        @test intersect(d2,d3) == DenseDomain((8:10, 5:8))

        @test project(DenseDomain((11:25, 21:100)), DenseDomain((15:20, 30:40))) == DenseDomain((5:10,10:20))
        # should this cause a BoundsError? vvv
        @test project(DenseDomain((11:25, 21:35)), DenseDomain((15:20, 30:40))) == DenseDomain((5:10,10:20))

        @test DenseDomain((11:20, 11:20))[DenseDomain((5:8, 5:10))] == DenseDomain((15:18, 15:20))
        @test_throws BoundsError DenseDomain((11:20, 11:20))[DenseDomain((5:8, 5:11))]

        @test alignfirst(DenseDomain(11:25, 21:100)) == DenseDomain((1:15), (1:80))
    end
end


