import ComputeFramework: children, parts, Computed

children(x::Computed) = children(x.result)
parts(x::Computed) = parts(x.result)
ComputeFramework.domain(x::Computed) = domain(x.result)

@testset "Arrays" begin

@testset "rand" begin
    function test_rand(X)
        X1 = compute(X)
        X2 = gather(X1)

        @test isa(X, ComputeFramework.Computation)
        @test isa(X1, ComputeFramework.Computed)
        @test isa(X1.result, ComputeFramework.Cat)
        @test X2 |> size == (100, 100)
        @test all(X2 .>= 0.0)
        @test size(parts(X1)) == (10, 10)
        @test domain(X1).head == DenseDomain(1:100, 1:100)
        @test children(domain(X1)) |> size == (10, 10)
        @test children(domain(X1)) == children(partition(BlockPartition(10, 10), DenseDomain(1:100, 1:100)))
    end
    X = rand(BlockPartition(10, 10), 100, 100)
    test_rand(X)
    Xsp = sprand(BlockPartition(10, 10), 100, 100, 0.1)
    test_rand(Xsp)
end
@testset "sum(ones(...))" begin
    X = ones(BlockPartition(10, 10), 100, 100)
    @test compute(sum(X)) == 10000
end


@testset "distributing an array" begin
    function test_dist(X)
        X1 = Distribute(BlockPartition(10, 20), X)
        @test gather(X1) == X
        Xc = compute(X1)
        @test parts(Xc) |> size == (10, 5)
        @test children(domain(Xc)) |> size == (10, 5)
        @test map(x->size(x) == (10, 20), children(domain(Xc))) |> all
    end
    test_dist(rand(100, 100))
    test_dist(sprand(100, 100, 0.1))
end

@testset "transpose" begin
    function test_transpose(X)
        x, y = size(X)
        X1 = Distribute(BlockPartition(10, 20), X)
        @test gather(X1') == X'
        Xc = compute(X1')
        @test parts(Xc) |> size == (div(y, 20), div(x,10))
        @test children(domain(Xc)) |> size == (div(y, 20), div(x, 10))
        @test map(x->size(x) == (20, 10), children(domain(Xc))) |> all
    end
    test_transpose(rand(100, 100))
    test_transpose(rand(100, 120))
    test_transpose(sprand(100, 100, 0.1))
    test_transpose(sprand(100, 120, 0.1))
end

@testset "matrix-matrix multiply" begin
    function test_mul(X)
        tol = 1e-12
        X1 = Distribute(BlockPartition(10, 20), X)
        @test_throws DimensionMismatch compute(X1*X1)
        X2 = compute(X1'*X1)
        X3 = compute(X1*X1')
        @test norm(gather(X2) - X'X) < tol
        @test norm(gather(X3) - X*X') < tol
        @test parts(X2) |> size == (2, 2)
        @test parts(X3) |> size == (4, 4)
        @test map(x->size(x) == (20, 20), children(domain(X2))) |> all
        @test map(x->size(x) == (10, 10), children(domain(X3))) |> all
    end
    test_mul(rand(40, 40))
end


end
