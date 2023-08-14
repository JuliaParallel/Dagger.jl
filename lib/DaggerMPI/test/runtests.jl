using LinearAlgebra, SparseArrays, Random, SharedArrays, Test, MPI, DaggerMPI, Dagger
import Dagger: chunks, DArray, domainchunks, treereduce_nd
import Distributed: myid, procs

MPI.Init()

@testset "DArray MPI constructor" begin
    x = rand(MPIBlocks(nothing,nothing), 10, 10)
    @test x isa Dagger.DArray{eltype(x), 2, MPIBlocks{2}, typeof(cat)}
    @test collect(x) == DArray(eltype(x), x.domain, x.subdomains[1], x.chunks[1], x.partitioning, x.concat) |> collect
end

@testset "rand" begin
    function test_rand(X1)
        X2 = collect(X1)
        @test isa(X1, Dagger.DArray)
        @test X2 |> size == (100, 100)
        @test all(X2 .>= 0.0)
        @test size(chunks(X1)) == (1, 1)
        @test domain(X1) == ArrayDomain(1:100, 1:100)
        @test domainchunks(X1) |> size == (1, 1)
        @test collect(X1) == collect(X1)
    end
    X = rand(MPIBlocks(nothing, nothing), 100, 100)
    test_rand(X)
    R = rand(MPIBlocks(nothing), 20)
    r = collect(R)
    @test r[1:10] != r[11:20]
end

@testset "local sum" begin
    X = ones(MPIBlocks(nothing, nothing), 100, 100)
    @test sum(X, acrossranks=false) == 10000 / MPI.Comm_size(MPI.COMM_WORLD)
    Y = zeros(MPIBlocks(nothing, nothing), 100, 100)
    @test sum(Y, acrossranks=false) == 0
end

@testset "sum across ranks" begin
    X = ones(MPIBlocks(nothing, nothing), 100, 100)
    @test sum(X) == 10000
    Y = zeros(MPIBlocks(nothing, nothing), 100, 100)
    @test sum(Y) == 0
end

@testset "rooted sum across ranks" begin
    X = ones(MPIBlocks(nothing, nothing), 100, 100)
    isroot = MPI.Comm_rank(MPI.COMM_WORLD) == 0
    if isroot
        @test sum(X, root=0) == 10000
    else
        @test sum(X, root=0) == nothing
    end

    Y = zeros(MPIBlocks(nothing, nothing), 100, 100)
    if isroot
        @test sum(Y, root=0) == 0
    else
        @test sum(Y, root=0) == nothing
    end
end

@testset "local prod" begin
    x = ones(100, 100)
    x = 2 .* x
    x = x ./ 10
    X = distribute(x, MPIBlocks(nothing, nothing))
    @test prod(X, acrossranks=false) == 0.2 ^ (10000 / MPI.Comm_size(MPI.COMM_WORLD))
    Y = zeros(MPIBlocks(nothing, nothing), 100, 100)
    @test prod(Y, acrossranks=false) == 0
end

@testset "prod across ranks" begin
    x = randn(100, 100)
    X = distribute(x, MPIBlocks(nothing, nothing))
    @test prod(X) == prod(x)
    Y = zeros(MPIBlocks(nothing, nothing), 100, 100)
    @test prod(Y) == 0
end

@testset "rooted prod across ranks" begin
    x = randn(100, 100)
    X = distribute(x, MPIBlocks(nothing, nothing))
    isroot = MPI.Comm_rank(MPI.COMM_WORLD) == 0

    if isroot
        @test prod(X, root=0) == prod(x)
    else
        @test prod(X, root=0) == nothing
    end

    Y = zeros(MPIBlocks(nothing, nothing), 100, 100)
    if isroot
        @test prod(Y, root=0) == 0
    else
        @test prod(Y, root=0) == nothing
    end
end

@testset "distributing an array" begin
    function test_dist(X)
        X1 = distribute(X, MPIBlocks(nothing, nothing))
        Xc = fetch(X1)
        @test Xc isa DArray{eltype(X), ndims(X), MPIBlocks{2}, typeof(cat)}
        @test chunks(Xc) |> size == (1, 1)
        @test domainchunks(Xc) |> size == (1, 1)
    end
    x = [1 2; 3 4]
    test_dist(rand(100, 100))
end

@testset "reducedim across ranks" begin
    X = randn(MPIBlocks(nothing, nothing), 100, 100)
    @test reduce(+, collect(X), dims= 1) ≈ collect(reduce(+, X, dims=1))
    @test reduce(+, collect(X), dims= 2) ≈ collect(reduce(+, X, dims=2))
    
    @test sum(collect(X), dims= 1) ≈ collect(sum(X, dims=1))
    @test sum(collect(X), dims= 2) ≈ collect(sum(X, dims=2))
end

@testset "rooted reducedim across ranks" begin
    X = randn(MPIBlocks(nothing, nothing), 100, 100)
    isroot = MPI.Comm_rank(MPI.COMM_WORLD) == 0
    if isroot
        @test collect(reduce(+, X, dims=1, root=0), acrossranks=false) ≈ collect(reduce(+, X, dims=1), acrossranks=false)
        @test collect(reduce(+, X, dims=2, root=0), acrossranks=false) ≈ collect(reduce(+, X, dims=2), acrossranks=false)
    else
        @test reduce(+, X, dims=1, root=0) == nothing
        reduce(+, X, dims=1)
        @test reduce(+, X, dims= 2, root=0) == nothing
        reduce(+, X, dims=2)
    end
    
    if isroot
        @test collect(sum(X, dims=1, root=0), acrossranks=false) ≈ collect(sum(X, dims=1), acrossranks=false)
        @test collect(sum(X, dims=2, root=0), acrossranks=false) ≈ collect(sum(X, dims=2), acrossranks=false)
    else
        @test sum(X, dims=1, root=0) == nothing
        sum(X, dims=1)
        @test sum(X, dims=2, root=0) == nothing
        sum(X, dims=2)
    end
end

@testset "local reducedim" begin
    X = rand(MPIBlocks(nothing, nothing), 100, 100)
    @test reduce(+, collect(X, acrossranks=false), dims=1) == collect(reduce(+, X, dims=1, acrossranks=false), acrossranks=false)
    @test reduce(+, collect(X, acrossranks=false), dims=2) == collect(reduce(+, X, dims=2, acrossranks=false), acrossranks=false)

    X = rand(MPIBlocks(nothing, nothing), 100, 100)
    @test sum(collect(X, acrossranks=false), dims=1) == collect(sum(X, dims=1, acrossranks=false), acrossranks=false)
    @test sum(collect(X, acrossranks=false), dims=2) == collect(sum(X, dims=2, acrossranks=false), acrossranks=false)
end

MPI.Finalize()
