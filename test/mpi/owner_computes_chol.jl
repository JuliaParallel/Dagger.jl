# DArray Cholesky on the owner-computes layer over MPI (included by cpu.jl, which has set up MPI acceleration and `comm`):
# same result as the sequential factorization, both triangles, a non-PD input, an unsupported shape.
@testset "Owner-computes Cholesky of a DArray (MPI)" begin
    rank = MPI.Comm_rank(comm)
    n, nb = 96, 16
    old = Dagger.OWNER_COMPUTES_LINALG[]
    try
        Random.seed!(42)
        for T in (Float64, ComplexF64), (uplo, W) in (('L', LowerTriangular), ('U', UpperTriangular))
            X = rand(T, n, n); A = X * X' + n * I; A = (A + A') / 2
            DA = zeros(Blocks(nb, nb), T, n, n)
            copyto!(DA, A)
            Dagger.owner_computes_linalg!(true)
            runs0 = Dagger.OWNER_COMPUTES_RUNS[]
            res, info = LinearAlgebra._chol!(DA, W)
            @test Dagger.OWNER_COMPUTES_RUNS[] == runs0 + 1          # the layer really ran on every rank
            Dagger.owner_computes_linalg!(false)
            R = collect(res.data)
            ref = cholesky(Hermitian(A, uplo == 'L' ? :L : :U))
            F = uplo == 'L' ? tril : triu
            @test info == 0
            @test norm(F(R) - F(uplo == 'L' ? ref.L : ref.U)) / norm(A) < 1e-10
        end

        # not positive definite: every rank reports the same nonzero info and nobody hangs
        A = rand(64, 64); A = A + A'
        DA = zeros(Blocks(16, 16), Float64, 64, 64); copyto!(DA, A)
        Dagger.owner_computes_linalg!(true)
        _, info = LinearAlgebra._chol!(DA, LowerTriangular)
        Dagger.owner_computes_linalg!(false)
        @test info > 0
        infos = MPI.Allgather(Int(info), comm)
        @test all(==(infos[1]), infos)

        # unsupported shape (rectangular tiles) falls back to Datadeps and still works
        X = rand(48, 48); A = X * X' + 48I; A = (A + A') / 2
        DA = zeros(Blocks(16, 12), Float64, 48, 48); copyto!(DA, A)
        Dagger.owner_computes_linalg!(true)
        runs1 = Dagger.OWNER_COMPUTES_RUNS[]
        _, i2 = LinearAlgebra._chol!(DA, LowerTriangular)
        @test Dagger.OWNER_COMPUTES_RUNS[] == runs1                   # unsupported shape: not on the owner-computes layer
        Dagger.owner_computes_linalg!(false)
        @test i2 == 0
    finally
        Dagger.owner_computes_linalg!(old)
    end
end
