# Graph partitioning of sparse operators (`repartition` / `distribute`
# `partitioner=` / `perm=`). Geometric `Blocks` is unchanged; METIS supplies a
# vertex permutation so tiles have fewer off-diagonal nonzeros.
#
#     julia test/runtests.jl --test array/linalg/partition

using Metis

module NotAPartitioner end

# 2D 5-point Laplacian on an N×N grid (same stencil as sparsedirect.jl).
function lap2d(T, N)
    n = N * N
    Is = Int[]; Js = Int[]; Vs = T[]
    idx(i, j) = (j - 1) * N + i
    for j in 1:N, i in 1:N
        k = idx(i, j)
        push!(Is, k); push!(Js, k); push!(Vs, T(4))
        if i > 1; push!(Is, k); push!(Js, idx(i - 1, j)); push!(Vs, -one(T)); end
        if i < N; push!(Is, k); push!(Js, idx(i + 1, j)); push!(Vs, -one(T)); end
        if j > 1; push!(Is, k); push!(Js, idx(i, j - 1)); push!(Vs, -one(T)); end
        if j < N; push!(Is, k); push!(Js, idx(i, j + 1)); push!(Vs, -one(T)); end
    end
    return SparseArrays.sparse(Is, Js, Vs, n, n)
end

function offdiag_tile_nnz(A::Dagger.DMatrix)
    off = 0
    for I in CartesianIndices(A.chunks)
        i, j = Tuple(I)
        i == j && continue
        tile = fetch(A.chunks[I])
        S = SparseArrays.sparse(Dagger._tile_matrix(tile))
        off += SparseArrays.nnz(S)
    end
    return off
end

@testset "Graph partition of sparse operators" begin
    N = 16
    Alocal = lap2d(Float64, N)
    n = size(Alocal, 1)
    k = 64
    part = Blocks(k, k)
    nparts = cld(n, k)
    rng = MersenneTwister(42)
    pscr = randperm(rng, n)
    Ascr = Alocal[pscr, pscr]

    @testset "geometric repartition is unchanged" begin
        DA = distribute(Ascr, Blocks(128, 128))
        A2 = Dagger.repartition(DA, part)
        @test A2.partitioning == part
        @test Dagger.is_sparse_backed(A2)
        @test SparseArrays.sparse(A2) ≈ Ascr
        @test Dagger.repartition(A2, part) === A2
    end

    @testset "partition_graph + perm" begin
        parts = Dagger.partition_graph(Metis, Ascr, nparts)
        @test length(parts) == n
        @test extrema(parts)[1] >= 1
        @test extrema(parts)[2] <= nparts
        perm = Dagger.partition_perm(parts)
        @test sort(perm) == 1:n
        @test Dagger.partition_graph(Metis.partition, Ascr, nparts) isa Vector{Int}

        stripe(A, np) = [mod1(i, np) for i in 1:size(A, 1)]
        @test Dagger.partition_graph(stripe, Ascr, nparts) == stripe(Ascr, nparts)

        @test_throws ArgumentError Dagger.partition_graph(NotAPartitioner, Ascr, nparts)
        @test_throws ArgumentError Dagger.partition_graph(Metis, Matrix(Ascr), nparts)
    end

    @testset "repartition/distribute with perm=" begin
        DA = distribute(Ascr, Blocks(128, 128))
        parts = Dagger.partition_graph(Metis, DA, nparts)
        perm = Dagger.partition_perm(parts)
        A2 = Dagger.repartition(DA, part; perm)
        @test A2.partitioning == part
        @test Dagger.is_sparse_backed(A2)
        @test SparseArrays.sparse(A2) ≈ Ascr[perm, perm]

        b = rand(n)
        Db = distribute(b, Blocks(128))
        b2 = Dagger.repartition(Db, Blocks(k); perm)
        @test collect(b2) ≈ b[perm]
        @test collect(A2 * b2) ≈ Ascr[perm, perm] * b[perm]

        A3 = distribute(Ascr, part; perm)
        @test Dagger.is_sparse_backed(A3)
        @test SparseArrays.sparse(A3) ≈ Ascr[perm, perm]
        @test distribute(b, Blocks(k); perm) == b[perm]
    end

    @testset "repartition/distribute with partitioner=Metis" begin
        DA = distribute(Ascr, Blocks(128, 128))
        A_geom = Dagger.repartition(DA, part)
        A_metis = Dagger.repartition(DA, part; partitioner=Metis)
        @test A_metis.partitioning == part
        @test Dagger.is_sparse_backed(A_metis)
        @test SparseArrays.nnz(SparseArrays.sparse(A_metis)) == SparseArrays.nnz(Ascr)
        @test size(A_metis) == size(DA)

        A_fn = Dagger.repartition(DA, part; partitioner=Metis.partition)
        @test Dagger.is_sparse_backed(A_fn)
        @test SparseArrays.nnz(SparseArrays.sparse(A_fn)) == SparseArrays.nnz(Ascr)

        A_dist = distribute(Ascr, part; partitioner=Metis)
        @test Dagger.is_sparse_backed(A_dist)
        @test A_dist.partitioning == part

        # Random numbering destroys geometric locality; METIS should recover it.
        @test offdiag_tile_nnz(A_metis) < offdiag_tile_nnz(A_geom)

        @test_throws ArgumentError Dagger.repartition(DA, part; partitioner=Metis, perm=1:n)
        @test_throws ArgumentError Dagger.repartition(distribute(rand(n), Blocks(k)),
                                                      Blocks(k); partitioner=Metis)
        @test_throws ArgumentError Dagger.repartition(distribute(Matrix(Ascr), part),
                                                      part; partitioner=Metis)
    end

    @testset "tiny / nparts=1 is a no-op permutation" begin
        A3 = sprand(3, 3, 0.8); A3 += A3'
        parts = Dagger.partition_graph(Metis, A3, 2)
        @test parts == ones(Int, 3)
        perm = Dagger.partition_perm(parts)
        @test perm == 1:3
        DA = distribute(A3, Blocks(2, 2))
        @test SparseArrays.sparse(Dagger.repartition(DA, Blocks(2, 2); partitioner=Metis)) ≈ A3
    end

    @testset "Schur nested-dissection hook still works" begin
        nd = Dagger._nested_dissection_partition(Alocal, 4)
        @test sort(nd.perm) == 1:n
        @test length(nd.interiors) >= 1
        @test length(vcat(nd.interiors..., nd.separator)) == n
        # Interiors and separator are disjoint and cover 1:n.
        seen = falses(n)
        for I in nd.interiors
            for i in I
                @test !seen[i]
                seen[i] = true
            end
        end
        for i in nd.separator
            @test !seen[i]
            seen[i] = true
        end
        @test all(seen)
    end
end
