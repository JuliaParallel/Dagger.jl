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

    # Local ThreadProc tiles take the no-Datadeps loop. Rebuild chunks on
    # OSProc so the guard is false and the Datadeps path still matches BLAS.
    @testset "local vs Datadeps path" begin
        n = 16
        x = rand(n)
        y = rand(n)
        a = 2.0
        Dx = fetch(distribute(x, Blocks(4)))
        Dy = fetch(distribute(copy(y), Blocks(4)))
        @test Dagger._blas1_tiles_local(Dx.chunks, Dy.chunks)
        axpy!(a, Dx, Dy)
        @test collect(Dy) ≈ a .* x .+ y
        @test dot(Dx, fetch(distribute(y, Blocks(4)))) ≈ dot(x, y)
        @test norm(Dx) ≈ norm(x)
        fill!(Dy, 0)
        @test all(iszero, collect(Dy))
        copyto!(Dy, Dx)
        @test collect(Dy) ≈ x

        function _with_osproc_chunks(A::DArray)
            A = fetch(A)
            new_chunks = map(c -> Dagger.tochunk(fetch(c), Dagger.OSProc()), A.chunks)
            return DArray(eltype(A), A.domain, A.subdomains, new_chunks, A.partitioning, A.concat)
        end
        Dx_os = _with_osproc_chunks(distribute(x, Blocks(4)))
        Dy_os = _with_osproc_chunks(distribute(copy(y), Blocks(4)))
        @test !Dagger._blas1_tiles_local(Dx_os.chunks, Dy_os.chunks)
        axpy!(a, Dx_os, Dy_os)
        @test collect(Dy_os) ≈ a .* x .+ y
        @test dot(Dx_os, _with_osproc_chunks(distribute(y, Blocks(4)))) ≈ dot(x, y)
        @test norm(Dx_os) ≈ norm(x)
    end
end
