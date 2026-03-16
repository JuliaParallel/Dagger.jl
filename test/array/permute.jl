@testset "permutedims" begin
    @testset "2D - square chunks" begin
        x = rand(12, 12)
        X = distribute(x, Blocks(4, 4))

        Y = permutedims(X, (2, 1))
        y = collect(Y)

        @test y == permutedims(x, (2, 1))
        @test size(Y) == (12, 12)
        # Chunk grid should be transposed: was (3,3), stays (3,3) for square
        @test size(Y.chunks) == (3, 3)
        # Each chunk should have been permuted
        @test Y.partitioning == Blocks(4, 4)
    end

    @testset "2D - rectangular chunks" begin
        x = rand(12, 20)
        X = distribute(x, Blocks(4, 5))

        Y = permutedims(X, (2, 1))
        y = collect(Y)

        @test y == permutedims(x, (2, 1))
        @test size(Y) == (20, 12)
        # Original chunk grid is (3, 4); after permute it should be (4, 3)
        @test size(Y.chunks) == (4, 3)
        @test Y.partitioning == Blocks(5, 4)
    end

    @testset "2D - non-divisible chunks" begin
        x = rand(10, 7)
        X = distribute(x, Blocks(3, 4))

        Y = permutedims(X, (2, 1))
        y = collect(Y)

        @test y == permutedims(x, (2, 1))
        @test size(Y) == (7, 10)
    end

    @testset "2D - single chunk per dimension" begin
        x = rand(6, 9)
        X = distribute(x, Blocks(6, 9))

        Y = permutedims(X, (2, 1))
        y = collect(Y)

        @test y == permutedims(x, (2, 1))
        @test size(Y) == (9, 6)
        @test size(Y.chunks) == (1, 1)
    end

    @testset "3D - (2,1,3) permutation" begin
        x = rand(6, 8, 10)
        X = distribute(x, Blocks(2, 4, 5))

        perm = (2, 1, 3)
        Y = permutedims(X, perm)
        y = collect(Y)

        @test y == permutedims(x, perm)
        @test size(Y) == (8, 6, 10)
        # Original chunk grid (3,2,2) -> after perm (2,3,2)
        @test size(Y.chunks) == (2, 3, 2)
        @test Y.partitioning == Blocks(4, 2, 5)
    end

    @testset "3D - (3,2,1) permutation" begin
        x = rand(6, 8, 10)
        X = distribute(x, Blocks(2, 4, 5))

        perm = (3, 2, 1)
        Y = permutedims(X, perm)
        y = collect(Y)

        @test y == permutedims(x, perm)
        @test size(Y) == (10, 8, 6)
        # Original chunk grid (3,2,2) -> after perm (2,2,3)
        @test size(Y.chunks) == (2, 2, 3)
        @test Y.partitioning == Blocks(5, 4, 2)
    end

    @testset "3D - (1,3,2) permutation" begin
        x = rand(6, 8, 10)
        X = distribute(x, Blocks(2, 4, 5))

        perm = (1, 3, 2)
        Y = permutedims(X, perm)
        y = collect(Y)

        @test y == permutedims(x, perm)
        @test size(Y) == (6, 10, 8)
        # Original chunk grid (3,2,2) -> after perm (3,2,2)
        @test size(Y.chunks) == (3, 2, 2)
        @test Y.partitioning == Blocks(2, 5, 4)
    end

    @testset "3D - non-divisible chunks" begin
        x = rand(5, 7, 9)
        X = distribute(x, Blocks(3, 4, 5))

        perm = (3, 1, 2)
        Y = permutedims(X, perm)
        y = collect(Y)

        @test y == permutedims(x, perm)
        @test size(Y) == (9, 5, 7)
    end

    @testset "3D - identity permutation" begin
        x = rand(6, 8, 10)
        X = distribute(x, Blocks(2, 4, 5))

        perm = (1, 2, 3)
        Y = permutedims(X, perm)
        y = collect(Y)

        @test y == x
        @test size(Y) == size(x)
        @test size(Y.chunks) == size(X.chunks)
    end

    @testset "return type" begin
        x = rand(Float32, 8, 12)
        X = distribute(x, Blocks(4, 6))

        Y = permutedims(X, (2, 1))
        @test Y isa DArray{Float32, 2}
        @test collect(Y) == permutedims(x, (2, 1))
    end
end
