@testset "isapprox" begin
    A = rand(16, 16)

    U1 = UpperTriangular(DArray(A, Blocks(16, 16)))
    U2 = UpperTriangular(DArray(A, Blocks(16, 16)))
    @test isapprox(U1, U2)

    L1 = LowerTriangular(DArray(A, Blocks(16, 16)))
    L2 = LowerTriangular(DArray(A, Blocks(16, 16)))
    @test isapprox(L1, L2)
end

@testset "norm" begin
    A = rand(16, 16)
    DA = DArray(A)
    @test isapprox(norm(A), norm(DA))

    A = rand(16)
    DA = DArray(A)
    @test isapprox(norm(A), norm(DA))

    A = rand(16, 16, 16)
    DA = DArray(A)
    @test isapprox(norm(A), norm(DA))
end

@testset "BLAS-1 vector ops" begin
    @testset "T=$T part=$(part.blocksize)" for T in (Float64, ComplexF64), part in (Blocks(16), Blocks(4))
        n = 16
        x = rand(T, n)
        y = rand(T, n)
        a = T(2)
        b = T(3)
        Dx = distribute(x, part)

        # dot (conjugates first arg for complex)
        @test dot(Dx, distribute(y, part)) ≈ dot(x, y)

        # axpy!: y += a*x
        Dy = distribute(copy(y), part)
        axpy!(a, Dx, Dy)
        @test collect(Dy) ≈ a .* x .+ y

        # axpby!: y = a*x + b*y
        Dy = distribute(copy(y), part)
        axpby!(a, Dx, b, Dy)
        @test collect(Dy) ≈ a .* x .+ b .* y

        # rmul!/lmul!: scale in place
        Dz = distribute(copy(x), part)
        rmul!(Dz, a)
        @test collect(Dz) ≈ a .* x
        Dz = distribute(copy(x), part)
        lmul!(a, Dz)
        @test collect(Dz) ≈ a .* x
    end

    # Operands with *different* partitionings must still work (aligned internally
    # via maybe_copy_buffered), and the in-place result keeps its own layout.
    @testset "mismatched partitionings T=$T" for T in (Float64, ComplexF64)
        n = 16
        x = rand(T, n)
        y = rand(T, n)
        a, b = T(2), T(3)
        Dx = distribute(x, Blocks(4))
        Dy = distribute(y, Blocks(8))

        @test dot(Dx, Dy) ≈ dot(x, y)

        Dy2 = distribute(copy(y), Blocks(8))
        axpy!(a, Dx, Dy2)
        @test collect(Dy2) ≈ a .* x .+ y
        @test Dy2.partitioning == Blocks(8)   # output layout preserved

        Dy3 = distribute(copy(y), Blocks(8))
        axpby!(a, Dx, b, Dy3)
        @test collect(Dy3) ≈ a .* x .+ b .* y
    end
end

@testset "diag/tr" begin
    @testset "$T $(sz) part=$(part.blocksize)" for T in (Float64, ComplexF64),
            (sz, part) in (((16, 16), Blocks(4, 4)),    # diagonal follows the partition grid
                           ((16, 16), Blocks(16, 16)),  # single partition
                           ((16, 16), Blocks(5, 3)),    # diagonal cuts through partitions
                           ((12, 20), Blocks(4, 5)),    # wide
                           ((20, 12), Blocks(7, 4)))    # tall
        A = rand(T, sz...)
        DA = distribute(A, part)
        # A `k` outside of the matrix gives an empty diagonal, as for a `Matrix`
        @testset "k=$k" for k in (-sz[1]-2):(sz[2]+2)
            Dd = diag(DA, k)
            @test Dd isa DVector{T}
            @test collect(Dd) == diag(A, k)
        end
        @test collect(diag(DA)) == diag(A)
        @testset "k::$(typeof(k))" for k in (UInt(1), Int8(-1), Int8(-128), big(2), true, typemax(Int))
            @test collect(diag(DA, k)) == diag(A, k)
        end
        if sz[1] == sz[2]
            @test tr(DA) ≈ tr(A)
        else
            @test_throws DimensionMismatch tr(DA)
        end
    end
end

@testset "tril/triu" begin
    # Built straight from the definition (`tril` keeps the entries with
    # `j - i <= k`, `triu` those with `j - i >= k`) rather than from `Base`, so
    # that the reference stays correct for the extreme `k` below, where `Base`'s
    # own `tril!`/`triu!` overflow computing `m + k` and `j - k - 1`.
    band_ref(keep, A, k) = [keep(j - i, k) ? A[i, j] : zero(eltype(A))
                            for i in axes(A, 1), j in axes(A, 2)]

    @testset "$T $(sz) part=$(part.blocksize)" for T in (Float64, ComplexF64),
            (sz, part) in (((16, 16), Blocks(4, 4)),    # diagonal follows the partition grid
                           ((16, 16), Blocks(16, 16)),  # single partition
                           ((16, 16), Blocks(5, 3)),    # diagonal cuts through partitions
                           ((12, 20), Blocks(4, 5)),    # wide
                           ((20, 12), Blocks(7, 4)))    # tall
        A = rand(T, sz...)
        DA = distribute(A, part)

        # A `k` past either end of the band keeps all of the matrix or none of
        # it, as for a `Matrix`
        @testset "k=$k" for k in (-sz[1]-2):(sz[2]+2)
            @test collect(tril(DA, k)) == tril(A, k)
            @test collect(triu(DA, k)) == triu(A, k)
        end
        @test tril(DA) isa DMatrix{T}
        @test collect(tril(DA)) == tril(A)
        @test collect(triu(DA)) == triu(A)

        # Out-of-place leaves `A` alone and keeps its partitioning
        @test collect(DA) == A
        @test tril(DA, 1).partitioning == DA.partitioning

        @testset "k::$(typeof(k))" for k in (UInt(1), Int8(-1), Int8(-128), big(2),
                                             true, typemax(Int), typemin(Int),
                                             typemin(Int) + 1, typemax(UInt))
            @test collect(tril(DA, k)) == band_ref(<=, A, k)
            @test collect(triu(DA, k)) == band_ref(>=, A, k)
        end

        # In place, returning the very same `DMatrix`
        DB = distribute(A, part)
        @test tril!(DB, 1) === DB
        @test collect(DB) == tril(A, 1)
        DC = distribute(A, part)
        @test triu!(DC, -1) === DC
        @test collect(DC) == triu(A, -1)

        # No partition is touched one element at a time
        Dagger.allowscalar(false) do
            @test collect(tril(DA, 2)) == tril(A, 2)
            @test collect(triu(DA, -2)) == triu(A, -2)
        end
    end
end
