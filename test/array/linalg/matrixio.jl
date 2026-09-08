# Matrix Market / DelimitedFiles hooks on DArray. Sparse write gathers CSC
# (not collect/densify). Dense writedlm collects. No Dagger-only format.
#
#     julia test/runtests.jl --test array/linalg/matrixio

using MatrixMarket
using DelimitedFiles

@testset "MatrixMarket sparse DMatrix" begin
    n, k = 12, 4
    Ah = sprand(n, n, 0.25) + I
    DA = distribute(Ah, Blocks(k, k))
    @test Dagger.is_sparse_backed(DA)

    path = tempname() * ".mtx"
    try
        MatrixMarket.mmwrite(path, DA)
        DA2 = MatrixMarket.mmread(path, Blocks(k, k))
        @test DA2 isa Dagger.DMatrix
        @test Dagger.is_sparse_backed(DA2)
        @test Matrix(sparse(DA2)) ≈ Matrix(Ah)
    finally
        isfile(path) && rm(path)
    end
end

@testset "MatrixMarket refuses dense DMatrix" begin
    DA = distribute(rand(8, 8), Blocks(4, 4))
    path = tempname() * ".mtx"
    try
        @test_throws ArgumentError MatrixMarket.mmwrite(path, DA)
    finally
        isfile(path) && rm(path)
    end
end

@testset "DelimitedFiles dense DArray" begin
    n, k = 8, 4
    Ah = rand(n, n)
    DA = distribute(Ah, Blocks(k, k))
    path = tempname()
    try
        writedlm(path, DA)
        @test readdlm(path) ≈ Ah
        DA2 = readdlm(path, Blocks(k, k))
        @test DA2 isa Dagger.DMatrix
        @test collect(DA2) ≈ Ah
    finally
        isfile(path) && rm(path)
    end
end

@testset "DelimitedFiles refuses sparse DMatrix" begin
    DA = distribute(sprand(8, 8, 0.3) + I, Blocks(4, 4))
    path = tempname()
    try
        @test_throws ArgumentError writedlm(path, DA)
    finally
        isfile(path) && rm(path)
    end
end
