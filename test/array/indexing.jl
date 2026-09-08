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

        if ndims(A) == 2
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
            col = X[ragged_idx, 5]
            row = X[5, ragged_idx]
            @test col isa DVector
            @test row isa DVector
            @test collect(col) == x[ragged_idx, 5]
            @test collect(row) == x[5, ragged_idx]
            @test X[5, 5] == x[5,5]
            @test collect(X[:, 5]) == x[:, 5]
            @test collect(X[5, :]) == x[5, :]
            @test size(X[:, 5:5]) == (size(x, 1), 1)
            @test collect(X[:, 5:5]) == x[:, 5:5]
        end
    end

    test_getindex(rand(10,10))
    test_getindex(sprand(10,10,0.5))

    let X = distribute(sprand(10, 10, 0.5), Blocks(3, 3))
        S = X[2:8, 3:9]
        @test S isa DMatrix
        @test Dagger.is_sparse_backed(S)
        @test S.partitioning == X.partitioning
        @test collect(S) == collect(X)[2:8, 3:9]
    end

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

@testset "view is a SubArray (writes through)" begin
    A = distribute(reshape(Float64.(1:64), 8, 8), Blocks(4, 4))
    V = view(A, 2:5, 3:6)
    @test V isa SubArray
    @test parent(V) === A
    @test size(V) == (4, 4)
    @test collect(V) == collect(A)[2:5, 3:6]

    Dagger.allowscalar(false) do
        copyto!(V, ones(4, 4))
    end
    href = reshape(Float64.(1:64), 8, 8)
    href[2:5, 3:6] .= 1
    @test collect(A) == href
end

@testset "range getindex keeps tiling" begin
    A = distribute(reshape(Float64.(1:100), 10, 10), Blocks(3, 3))
    Dagger.allowscalar(false) do
        B = A[2:8, 4:9]
        @test B isa DArray
        @test B.partitioning == A.partitioning
        @test collect(B) == collect(A)[2:8, 4:9]

        v = A[3:8]
        @test v isa DVector
        @test collect(v) == collect(A)[3:8]

        @test collect(A[:]) == vec(collect(A))
        @test collect(A[1:2:9, :]) == collect(A)[1:2:9, :]
        @test collect(A[:, 10:-1:1]) == collect(A)[:, 10:-1:1]
    end

    # copyto! of a StepRange view stays an error (intentional contract).
    DB = zeros(Blocks(3, 3), 10, 10)
    @test_throws ArgumentError copyto!(view(DB, 1:2:10, :), view(A, 1:2:10, :))
end

@testset "setindex! from Array and scalar" begin
    A = zeros(Blocks(3, 3), 10, 10)
    Dagger.allowscalar(false) do
        A[3:8, 2:7] = ones(6, 6)
    end
    href = zeros(10, 10)
    href[3:8, 2:7] .= 1
    @test collect(A) == href

    A2 = zeros(Blocks(3, 3), 10, 10)
    Dagger.allowscalar(false) do
        A2[2:4, 5:8] = 7.0
    end
    href2 = zeros(10, 10)
    href2[2:4, 5:8] .= 7
    @test collect(A2) == href2
end

@testset "DVector slicing" begin
    x = rand(20)
    X = distribute(x, Blocks(5))
    @test collect(X[4:12]) == x[4:12]
    @test collect(X[[1, 4, 9, 16]]) == x[[1, 4, 9, 16]]
    @test X[4:12] isa DVector
    @test X[4:12].partitioning == X.partitioning

    Dagger.allowscalar(false) do
        X[2:6] = 3.0
    end
    @test collect(X)[2:6] == fill(3.0, 5)
    @test collect(X)[1] == x[1]
end

@testset "field-split style A[Ω, Ω]" begin
    n = 12
    A = distribute(Float64.(reshape(1:n*n, n, n)), Blocks(4, 4))
    Ω = 3:9
    S = A[Ω, Ω]
    @test S isa DMatrix
    @test size(S) == (length(Ω), length(Ω))
    @test collect(S) == collect(A)[Ω, Ω]
end