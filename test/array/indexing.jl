@testset "indexing" begin
    pairs = []
    A = rand(64)
    DA = view(A, Blocks(8))
    push!(pairs, (A, DA))
    A = rand(64, 64)
    DA = view(A, Blocks(8, 8))
    push!(pairs, (A, DA))

    for (A, DA) in pairs
        @test DA[1] == A[1]
        @test first(A) == first(DA)
        @test last(A) == last(DA)
        DA[3] = 42.0
        @test DA[3] == A[3] == 42.0

        if size(A) == 2
            @test DA[2, 4] == A[2, 4]
            DA[2, 4] = 99.0
            @test DA[2, 4] == A[2, 4] == 99.0
        end
    end
end

@testset "Getindex" begin
    function test_getindex(x)
        X = distribute(x, Blocks(3,3))
        @test collect(X[3:8, 2:7]) == x[3:8, 2:7]
        ragged_idx = [1,2,9,7,6,2,4,5]
        @test collect(X[ragged_idx, 2:7]) == x[ragged_idx, 2:7]
        @test collect(X[ragged_idx, reverse(ragged_idx)]) == x[ragged_idx, reverse(ragged_idx)]
        ragged_idx = [1,2,9,7,6,2,4,5]
        @test collect(X[[2,7,10], :]) == x[[2,7,10], :]
        @test collect(X[[], ragged_idx]) == x[[], ragged_idx]
        @test collect(X[[], []]) == x[[], []]

        @testset "dimensionality reduction" begin
            @test vec(collect(X[ragged_idx, 5])) == vec(x[ragged_idx, 5])
            @test vec(collect(X[5, ragged_idx])) == vec(x[5, ragged_idx])
            @test X[5, 5] == x[5,5]
        end
    end

    test_getindex(rand(10,10))
    test_getindex(sprand(10,10,0.5))

    y = rand(10, 10)
    xs = distribute(y, Blocks(2,2))
    for i=1:10, j=1:10
        @test xs[i:j, j:i] == y[i:j, j:i]
    end
end

@testset "setindex" begin
    x=rand(10,10)
    y=copy(x)
    y[3:8, 2:7] .= 1.0
    X = distribute(x, Blocks(3,3))
    @test collect(setindex(X,1.0, 3:8, 2:7)) == y
    @test collect(X) == x
end
