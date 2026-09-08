# Dense `schur(::DMatrix)` gathers tiles and runs LAPACK. Sparse-backed
# operators must throw (would densify). This does not replace LOBPCG `eigen`.
#
#     julia test/runtests.jl --test array/linalg/schur

@testset "dense Schur" begin
    n, k = 8, 4
    Ah = rand(n, n)
    DA = distribute(Ah, Blocks(k, k))
    F = schur(DA)
    Fh = schur(Ah)
    @test F isa LinearAlgebra.Schur
    @test F.values ≈ Fh.values
    @test Ah ≈ F.Z * F.T * F.Z' rtol=1e-8

    F! = schur!(copy(DA))
    @test F! isa LinearAlgebra.Schur
    @test F!.values ≈ Fh.values
end

@testset "sparse-backed schur refuses to densify" begin
    n, k = 8, 4
    Asp = sprand(n, n, 0.4) + n * I
    DA = distribute(Asp, Blocks(k, k))
    @test Dagger.is_sparse_backed(DA)
    @test_throws ArgumentError schur(DA)
    @test_throws ArgumentError schur!(DA)
end
