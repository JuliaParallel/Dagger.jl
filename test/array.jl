using LinearAlgebra, SparseArrays, Random, SharedArrays
import Dagger: chunks, DArray, domainchunks, treereduce_nd

@testset "treereduce_nd" begin
    xs = rand(1:10, 8,8,8)
    concats = [(x...)->cat(x..., dims=n) for n in 1:3]
    @test treereduce_nd(concats, xs) == xs
    @test treereduce_nd(reverse(concats), xs) == permutedims(xs, [3,2,1])
end

@testset "DArray constructor" begin
    x = compute(rand(Blocks(2,2), 3,3))
    @test collect(x) == DArray{Float64, 2}(x.domain, x.subdomains, x.chunks) |> collect
end

@testset "rand" begin
    function test_rand(X)
        X1 = compute(X)
        X2 = collect(X1)

        @test isa(X, Dagger.ArrayOp)
        @test isa(X1, Dagger.DArray)
        @test X2 |> size == (100, 100)
        @test all(X2 .>= 0.0)
        @test size(chunks(X1)) == (10, 10)
        @test domain(X1) == ArrayDomain(1:100, 1:100)
        @test domainchunks(X1) |> size == (10, 10)
        @test domainchunks(X1) == partition(Blocks(10, 10), ArrayDomain(1:100, 1:100))
        @test collect(X1) == collect(X1)
    end
    X = rand(Blocks(10, 10), 100, 100)
    test_rand(X)
    Xsp = sprand(Blocks(10, 10), 100, 100, 0.1)
    test_rand(Xsp)
    R = rand(Blocks(10), 20)
    r = collect(R)
    @test r[1:10] != r[11:20]
end
@testset "sum(ones(...))" begin
    X = ones(Blocks(10, 10), 100, 100)
    @test sum(X) == 10000
end

@testset "distributing an array" begin
    function test_dist(X)
        X1 = Distribute(Blocks(10, 20), X)
        @test X1 == X
        Xc = compute(X1)
        @test chunks(Xc) |> size == (10, 5)
        @test domainchunks(Xc) |> size == (10, 5)
        @test map(x->size(x) == (10, 20), domainchunks(Xc)) |> all
    end
    x = [1 2; 3 4]
    @test Distribute(Blocks(1,1), x) == x
    #test_dist(rand(100, 100))
    #test_dist(sprand(100, 100, 0.1))

    x = distribute(rand(10), 2)
    @test collect(distribute(x, 3)) == collect(x)
end

@testset "transpose" begin
    function test_transpose(X)
        x, y = size(X)
        X1 = Distribute(Blocks(10, 20), X)
        @test X1' == X'
        Xc = compute(X1')
        @test chunks(Xc) |> size == (div(y, 20), div(x,10))
        @test domainchunks(Xc) |> size == (div(y, 20), div(x, 10))
        @test map(x->size(x) == (20, 10), domainchunks(Xc)) |> all
    end
    test_transpose(rand(100, 100))
    test_transpose(rand(100, 120))
    test_transpose(sprand(100, 100, 0.1))
    test_transpose(sprand(100, 120, 0.1))
end

@testset "matrix-matrix multiply" begin
    function test_mul(X)
        tol = 1e-12
        X1 = Distribute(Blocks(10, 20), X)
        @test_throws DimensionMismatch compute(X1*X1)
        X2 = compute(X1'*X1)
        X3 = compute(X1*X1')
        @test norm(collect(X2) - X'X) < tol
        @test norm(collect(X3) - X*X') < tol
        @test chunks(X2) |> size == (2, 2)
        @test chunks(X3) |> size == (4, 4)
        @test map(x->size(x) == (20, 20), domainchunks(X2)) |> all
        @test map(x->size(x) == (10, 10), domainchunks(X3)) |> all
    end
    test_mul(rand(40, 40))

    x = rand(10,10)
    X = Distribute(Blocks(3,3), x)
    y = rand(10)
    @test norm(collect(X*y) - x*y) < 1e-13
end

@testset "concat" begin
    m = rand(75,75)
    x = Distribute(Blocks(10,20), m)
    y = Distribute(Blocks(10,10), m)
    @test hcat(m,m) == collect(hcat(x,x))
    @test vcat(m,m) == collect(vcat(x,x))
    @test hcat(m,m) == collect(hcat(x,y))
    @test_throws DimensionMismatch compute(vcat(x,y))
end

@testset "scale" begin
    x = rand(10,10)
    X = Distribute(Blocks(3,3), x)
    y = rand(10)

    @test Diagonal(y)*x == collect(Diagonal(y)*X)
end

@testset "Getindex" begin
    function test_getindex(x)
        X = Distribute(Blocks(3,3), x)
        @test collect(X[3:8, 2:7]) == x[3:8, 2:7]
        ragged_idx = [1,2,9,7,6,2,4,5]
        @test collect(X[ragged_idx, 2:7]) == x[ragged_idx, 2:7]
        @test collect(X[ragged_idx, reverse(ragged_idx)]) == x[ragged_idx, reverse(ragged_idx)]
        ragged_idx = [1,2,9,7,6,2,4,5]
        @test collect(X[[2,7,10], :]) == x[[2,7,10], :]
        @test collect(X[[], ragged_idx]) == x[[], ragged_idx]
        @test collect(X[[], []]) == x[[], []]

        @testset "dimensionality reduction" begin
        # THESE NEED FIXING!!
            @test vec(collect(X[ragged_idx, 5])) == vec(x[ragged_idx, 5])
            @test vec(collect(X[5, ragged_idx])) == vec(x[5, ragged_idx])
            @test collect(X[5, 5]) == x[5,5]
        end
    end

    test_getindex(rand(10,10))
    test_getindex(sprand(10,10,0.5))

    y = rand(10, 10)
    xs = distribute(y, Blocks(2,2))
    for i=1:10, j=1:10
        @test compute(xs[i:j, j:i]) == y[i:j, j:i]
    end
end


@testset "cleanup" begin
    X = Distribute(Blocks(10,10), rand(10,10))
    @test collect(sin.(X)) == collect(sin.(X))
end


@testset "reducedim" begin
    x = rand(1:10, 10, 5)
    X = Distribute(Blocks(3,3), x)
    @test reduce(+, x, dims=1) == collect(reduce(+, X, dims=1))
    @test reduce(+, x, dims=2) == collect(reduce(+, X, dims=2))

    x = rand(1:10, 10, 5)
    X = Distribute(Blocks(10, 10), x)
    @test sum(x, dims=1) == collect(sum(X, dims=1))
    @test sum(x, dims=2) == collect(sum(X, dims=2))
end

@testset "setindex" begin
    x=rand(10,10)
    y=copy(x)
    y[3:8, 2:7] .= 1.0
    X = Distribute(Blocks(3,3), x)
    @test collect(setindex(X,1.0, 3:8, 2:7)) == y
    @test collect(X) == x
end

@testset "sort" begin
    x = shuffle(1:10)
    X = distribute(x, 4)
    @test collect(sort(X)) == sort(x)

    X = distribute(x, 10)
    @test collect(sort(X)) == sort(x)
    @test collect(sort(X, rev=true)) == sort(x, rev=true)

    X = distribute(x, 1)
    @test collect(sort(X)) == sort(x)
    @test collect(sort(X, rev=true)) == sort(x, rev=true)

    x = [("A",1), ("A",2), ("B",1)]
    y = distribute(x, 3)
    @test collect(sort(y)) == x

    x = ones(10)
    y = compute(Distribute(Blocks(3), x))
    #@test map(x->length(collect(x)), compute(sort(y)).chunks) == [3,3,3,1]
end

using MemPool

@testset "affinity" begin
    x = Dagger.tochunk([1:10;])
    aff = Dagger.affinity(x)
    @test length(aff) == 1
    @test aff[1][1] == Dagger.OSProc(myid())
    @test aff[1][2] == sizeof(Int)*10
    @test Dagger.tochunk(x) === x
    f = MemPool.FileRef("/tmp/d", aff[1][2])
    aff = Dagger.affinity(f)
    #@test length(aff) == 3
    @test (aff[1][1]).pid in procs()
    @test aff[1][2] == sizeof(Int)*10
end

@testset "show_plan" begin
    @test !isempty(Dagger.show_plan(Dagger.Thunk(()->10)))
end

#=
@testset "darray distributed refcount" begin
    D2 = remotecall_fetch(2, compute(Distribute(Blocks(10, 20), rand(40,40)))) do D
        D2 = D
    end
    @test size(collect(D2)) == (40,40)
    GC.gc()
    @test size(collect(D2)) == (40,40)
end
=#

@testset "sharedarray" begin
    A = SharedArray{Int}((1024,))
    B = SharedArray{Int}((1024,))
    C = Dagger.merge_sorted(Base.Order.Forward, A, B)
    @test length(C) === length(A) + length(B)
    @test typeof(C) === (Dagger.use_shared_array[] ? SharedArray{Int,1} : Array{Int,1})
end
