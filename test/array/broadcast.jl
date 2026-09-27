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

    @testset "Distributed view operands" begin
        # Nonuniform values expose misplaced slices; offset ranges cross tile
        # boundaries in the second layout. Keep scalar indexing disabled.
        ref = reshape(Float32.(1:64), 8, 8)
        for part in (Blocks(8, 8), Blocks(3, 3))
            A = distribute(ref, part)
            wait(A)
            V = @view A[2:7, 2:7]
            W = @view A[1:6, 2:7]
            expected = ref[2:7, 2:7] .+ ref[1:6, 2:7]

            @views C = A[2:7, 2:7] .+ A[1:6, 2:7]
            @test C isa DArray
            @test collect(C) == expected
            @test collect(2 .* V .+ W .- 1) == 2 .* ref[2:7, 2:7] .+ ref[1:6, 2:7] .- 1
            @test collect(V .+ 1) == ref[2:7, 2:7] .+ 1
            @test collect(V .+ ones(Float32, 6, 6)) == ref[2:7, 2:7] .+ 1

            B = distribute(ref[1:6, 2:7], Blocks(2, 2))
            @test collect(B .+ V) == expected
            dest = distribute(zeros(Float32, 6, 6), Blocks(2, 2))
            dest .= V .+ W
            @test collect(dest) == expected

            nested = @view V[2:5, 2:5]
            @test collect(nested .+ 1) == ref[3:6, 3:6] .+ 1
            column = @view A[2:7, 2]
            @test collect(column .+ 1) == ref[2:7, 2] .+ 1
            @test collect(V .+ column) == ref[2:7, 2:7] .+ ref[2:7, 2]
            row = @view A[2, 2:7]
            @test collect(row .+ 1) == ref[2, 2:7] .+ 1
            scalar_view = @view A[2, 3]
            @test collect(V .+ scalar_view) == ref[2:7, 2:7] .+ ref[2, 3]
            @test Dagger.allowscalar(() -> scalar_view .+ 1) == ref[2, 3] + 1
            @test_throws ArgumentError scalar_view .+ 1
            @test collect(A) == ref
        end
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