function test_copyto(sz, partA, partB, T)
    A = rand(T, sz...)
    DA = distribute(A, partA)
    DB = zeros(partB, T, sz...)
    copyto!(DB, DA)
    @test collect(DB) == collect(DA) == A
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
