Dagger.allowscalar!(false)

@testset "Broadcasting" begin
    @testset "Out-of-place" begin
        A = distribute(ones(10, 10), Blocks(5, 5))
        B = distribute(ones(10, 10), Blocks(5, 5))

        # Binary operation
        C = A .+ B
        @test C isa DArray
        @test collect(C) == fill(2.0, 10, 10)

        # Multiple operations
        D = A .* B .+ 1.0
        @test D isa DArray
        @test collect(D) == fill(2.0, 10, 10)

        # Mixed dimensions
        v = distribute(ones(10), Blocks(5))
        E = A .+ v
        @test E isa DArray
        @test collect(E) == fill(2.0, 10, 10)
    end

    @testset "In-place" begin
        A = distribute(ones(10, 10), Blocks(5, 5))
        B = distribute(ones(10, 10), Blocks(5, 5))

        # Simple in-place update
        A .+= B
        @test collect(A) == fill(2.0, 10, 10)

        # In-place with scalar
        A .*= 2.0
        @test collect(A) == fill(4.0, 10, 10)

        # In-place with mixed dimensions
        v = distribute(ones(10), Blocks(5))
        A .+= v
        @test collect(A) == fill(5.0, 10, 10)

        # In-place assignment to pre-allocated array
        C = distribute(zeros(10, 10), Blocks(5, 5))
        C .= A .+ B
        @test collect(C) == fill(6.0, 10, 10)
    end

    @testset "Complex Broadcast" begin
        A = distribute(rand(10, 10), Blocks(5, 5))
        B = distribute(rand(10, 10), Blocks(5, 5))
        f(x, y) = x^2 + y^2

        C = f.(A, B)
        @test collect(C) ≈ (collect(A).^2 .+ collect(B).^2)

        dest = distribute(zeros(10, 10), Blocks(5, 5))
        dest .= f.(A, B)
        @test collect(dest) ≈ (collect(A).^2 .+ collect(B).^2)
    end
end

Dagger.allowscalar!(true)