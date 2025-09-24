function test_copyto(sz, partA, partB, T)
    # Full arrays
    A = rand(T, sz...)
    DA = distribute(A, partA)
    DB = zeros(partB, T, sz...)
    copyto!(DB, DA)
    @test collect(DB) == collect(DA) == A

    # Contiguous SubArrays
    A = rand(T, sz...)
    DA = distribute(A, partA)
    DB = zeros(partB, T, sz...)
    copyto!(view(DB, 1:fld1(sz[1], 2), 1:fld1(sz[2], 2)),
            view(DA, 1:fld1(sz[1], 2), 1:fld1(sz[2], 2)))
    @test collect(DB)[1:fld1(sz[1], 2), 1:fld1(sz[2], 2)] == collect(DA)[1:fld1(sz[1], 2), 1:fld1(sz[2], 2)]
    diff = setdiff(CartesianIndices(DB), CartesianIndices((1:fld1(sz[1], 2), 1:fld1(sz[2], 2))))
    @test all(collect(DB)[diff] .== 0)

    # Non-contiguous SubArrays (currently unsupported)
    A = rand(T, sz...)
    DA = distribute(A, partA)
    DB = zeros(partB, T, sz...)
    @test_throws ArgumentError copyto!(view(DB, 1:2:sz[1], 1:2:sz[2]), view(DA, 1:2:sz[1], 1:2:sz[2]))

    # Dimension mismatch
    A = rand(T, sz...)
    DA = distribute(A, partA)
    DB = zeros(partB, T, sz...)
    @test_throws DimensionMismatch copyto!(DB, view(DA, 1:2:sz[1], 1:2:sz[2]))
end

@testset "T=$T" for T in (Int8, Int32, Int64, Float64, ComplexF64)
    test_copyto((128, 128), Blocks(32, 32), Blocks(32, 32), T)
end
sizes_to_test = [
    (3, 7),
    (32, 16),
    (32, 32),
    (57, 83),
    (128, 128)
]
parts_to_test = map(sz->Blocks(sz...), sizes_to_test)
@testset "Partitioning=$partB<-$partA ($sz)" for partB in parts_to_test, partA in parts_to_test, sz in sizes_to_test
    test_copyto(sz, partA, partB, Float64)
end
