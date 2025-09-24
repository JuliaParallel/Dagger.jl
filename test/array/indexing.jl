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

@testset "getindex" begin
    function test_getindex(x)
        X = distribute(x, Blocks(3,3))
        @test collect(X[3:8, 2:7]) == x[3:8, 2:7]
        ragged_idx = [1,2,9,7,6,2,4,5]
        @test collect(X[ragged_idx, 2:7]) == x[ragged_idx, 2:7]
        @test collect(X[ragged_idx, reverse(ragged_idx)]) == x[ragged_idx, reverse(ragged_idx)]
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

@testset "slicing" begin
    A = zeros(Blocks(5, 3), 10, 10)
    @test A[1:2, 1:2] isa DArray

    # Matrix - Vector
    for idx in 1:10
        A = zeros(Blocks(5, 3), 10, 10)
        b = rand(Blocks(2), 10)
        Dagger.allowscalar(false) do
            A[:, idx] = b
        end
        @test all(collect(A)[:, idx] .== b)
        @test all(collect(A)[:, 1:idx-1] .== 0)
        @test all(collect(A)[:, idx+1:end] .== 0)
    end

    # Matrix - Vector (transposed)
    for idx in 1:10
        A = zeros(Blocks(5, 3), 10, 10)
        b = rand(Blocks(2), 10)
        bT = DArray(b')
        Dagger.allowscalar(false) do
            A[idx, :] = bT
        end
        @test all(collect(A)[idx, :] .== b)
        @test all(collect(A)[1:idx-1, :] .== 0)
        @test all(collect(A)[idx+1:end, :] .== 0)
    end

    # Matrix - Matrix
    for idx in 1:9
        A = zeros(Blocks(5, 3), 10, 10)
        B = rand(Blocks(2, 2), 10, 10)
        Dagger.allowscalar(false) do
            A[idx:(idx+1), idx:(idx+1)] = view(B, idx:(idx+1), idx:(idx+1))
        end
        diff = setdiff(CartesianIndices(A), CartesianIndices((idx:(idx+1), idx:(idx+1))))
        @test all(collect(A)[diff] .== 0)
        @test all(collect(A)[idx:(idx+1), idx:(idx+1)] .== collect(B)[idx:(idx+1), idx:(idx+1)])
    end
end