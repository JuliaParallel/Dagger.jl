struct LUNonCPUProc <: Dagger.Processor end

@testset "LU" begin
    Random.seed!(1234)
    A = randn(100, 100)
    orig_A = copy(A)
    DA = DArray(A, Blocks(25, 25))

    F = lu!(DA, RowMaximum(); check=false)
    lu!(A, RowMaximum())

    @test norm(collect(A) - collect(DA)) / norm(collect(A)) < 1e-12
    DAc = collect(DA)
    p = LinearAlgebra.ipiv2perm(collect(F.ipiv), size(DAc, 1))
    LtU = UnitLowerTriangular(DAc) * UpperTriangular(DAc)
    @test norm(LtU - orig_A[p, :]) / norm(orig_A) < 1e-12
end

@testset "MPI native LU panels" begin
    @test !Dagger.supports_lapack_panel(LUNonCPUProc())
    @test !@inferred(Dagger.supports_lapack_panel(MPIExt.MPIProcessor(LUNonCPUProc(), comm, rank)))
    @test all(Dagger.supports_lapack_panel, Dagger.compatible_processors(Dagger.get_compute_scope()))
    for proc in mpi_procs()
        @test @inferred(Dagger.supports_lapack_panel(proc))
    end
    @test_throws LinearAlgebra.SingularException lu(DArray(ones(32, 32), Blocks(16, 16)))
    singular = lu(DArray(ones(32, 32), Blocks(16, 16)); check=false)
    @test singular.info > 0
    @test !issuccess(singular)
    for T in (Float64, ComplexF64), (m, n, b) in ((64, 64, 16), (61, 61, 16), (64, 32, 16), (32, 64, 16))
        A = rand(MersenneTwister(757), T, m, n)
        DA = DArray(A, Blocks(b, b))
        F = lu(DA, RowMaximum())
        factors = collect(F.factors)
        pivots = collect(F.ipiv)
        p = LinearAlgebra.ipiv2perm(pivots, m)
        k = min(m, n)
        L = tril(factors[:, 1:k], -1) + Matrix{T}(I, m, k)
        U = triu(factors[1:k, :])
        @test L * U ≈ A[p, :] rtol=1e-12
        @test collect(DA) == A
        if m == n
            rhs = rand(MersenneTwister(758), T, m)
            x = collect(F \ DArray(rhs, Blocks(b)))
            @test A*x ≈ rhs rtol=1e-11
        end
    end
end
