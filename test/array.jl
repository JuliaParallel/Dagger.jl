using LinearAlgebra, SparseArrays, Random, SharedArrays
import Dagger: chunks, DArray, domainchunks, treereduce_nd
import Distributed: myid, procs
import Statistics: mean, var, std
import OnlineStats

@testset "treereduce_nd" begin
    xs = rand(1:10, 8,8,8)
    concats = [(x...)->cat(x..., dims=n) for n in 1:3]
    @test treereduce_nd(concats, xs) == xs
    @test treereduce_nd(reverse(concats), xs) == permutedims(xs, [3,2,1])
end

@testset "DArray constructor" begin
    x = rand(Blocks(2,2), 3,3)
    @test x isa DArray{Float64, 2}
    @test collect(x) == DArray{Float64, 2}(x.domain, x.subdomains, x.chunks, x.partitioning, x.concat) |> collect
end

@testset "rand" begin
    function test_rand(X1)
        X2 = collect(X1)

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

@testset "view" begin
    A = rand(64, 64)
    DA = view(A, Blocks(8, 8))
    @test collect(DA) == A
    @test size(DA) == (64, 64)
    A_v = fetch(first(DA.chunks))
    @test A_v isa SubArray
    @test A_v == A[1:8, 1:8]
end

@testset "map" begin
    X1 = ones(Blocks(10, 10), 100, 100)
    X2 = map(x->x+1, X1)
    @test typeof(X1) === typeof(X2)
    @test collect(X1) .+ 1 == collect(X2)
end

@testset "copy/similar" begin
    X1 = ones(Blocks(10, 10), 100, 100)
    X2 = copy(X1)
    X3 = similar(X1)
    @test typeof(X1) === typeof(X2) === typeof(X3)
    @test collect(X1) == collect(X2)
    @test collect(X1) != collect(X3)
end

@testset "DiffEq support" begin
    X = ones(Blocks(10), 100)
    X0 = zero(X)
    @test typeof(X) === typeof(X0)
    @test all(collect(X0) .== 0)
    @testset for T in (Int8, Int, Float32, Float64)
        DT = DArray{Base.promote_op(/, Float64, T), 1, Blocks{1}, typeof(cat)}
        @test Base.promote_op(/, typeof(X), T) === DT
        y = T(2)
        Xd = X / y
        @test typeof(Xd) === DT
        @test collect(Xd) == collect(X) ./ y
    end
end

function test_mapreduce(f, init_func; no_init=true, zero_init=zero,
                        types=(Int32, Int64, Float32, Float64),
                        cmp=isapprox)
    @testset "$T" for T in types
        X = init_func(Blocks(10, 10), T, 100, 100)
        inits = ()
        if no_init
            inits = (inits..., nothing)
        end
        if zero_init !== nothing
            inits = (inits..., zero_init(T))
        end
        @testset "dims=$dims" for dims in (Colon(), 1, 2, (1,), (2,))
            @testset "init=$init" for init in inits
                if init === nothing
                    if dims == Colon()
                        @test cmp(f(X; dims), f(collect(X); dims))
                    else
                        @test cmp(collect(f(X; dims)), f(collect(X); dims))
                    end
                else
                    if dims == Colon()
                        @test cmp(f(X; dims, init), f(collect(X); dims, init))
                    else
                        @test cmp(collect(f(X; dims, init)), f(collect(X); dims, init))
                    end
                end
            end
        end
    end
end

# Base
@testset "reduce" test_mapreduce((X; dims, init=Base._InitialValue())->reduce(+, X; dims, init), ones)
@testset "mapreduce" test_mapreduce((X; dims, init=Base._InitialValue())->mapreduce(x->x+1, +, X; dims, init), ones)
@testset "sum" test_mapreduce(sum, ones)
@testset "prod" test_mapreduce(prod, rand)
@testset "minimum" test_mapreduce(minimum, rand)
@testset "maximum" test_mapreduce(maximum, rand)
@testset "extrema" test_mapreduce(extrema, rand; cmp=Base.:(==), zero_init=T->(zero(T), zero(T)))

# Statistics
@testset "mean" test_mapreduce(mean, rand; zero_init=nothing, types=(Float32, Float64))
@testset "var" test_mapreduce(var, rand; zero_init=nothing, types=(Float32, Float64))
@testset "std" test_mapreduce(std, rand; zero_init=nothing, types=(Float32, Float64))

@testset "broadcast" begin
    X1 = rand(Blocks(10), 100)
    X2 = X1 .* 3.4
    @test typeof(X1) === typeof(X2)
    @test collect(X1) .* 3.4 == collect(X2)
    X3 = X1 .+ X1
    @test typeof(X1) === typeof(X3)
    @test collect(X1) .* 2 == collect(X3)
end

@testset "distributing an array" begin
    function test_dist(X)
        X1 = Distribute(Blocks(10, 20), X)
        Xc = fetch(X1)
        @test Xc isa DArray{eltype(X),ndims(X)}
        @test Xc == X
        @test chunks(Xc) |> size == (10, 5)
        @test domainchunks(Xc) |> size == (10, 5)
        @test map(x->size(x) == (10, 20), domainchunks(Xc)) |> all
    end
    x = [1 2; 3 4]
    @test Distribute(Blocks(1,1), x) == x
    test_dist(rand(100, 100))
    test_dist(sprand(100, 100, 0.1))

    x = distribute(rand(10), 2)
    @test collect(distribute(x, 3)) == collect(x)
end

@testset "transpose" begin
    function test_transpose(X)
        x, y = size(X)
        X1 = Distribute(Blocks(10, 20), X)
        @test X1' == X'
        Xc = fetch(X1')
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
        @test_throws DimensionMismatch X1*X1
        X2 = X1'*X1
        X3 = X1*X1'
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

@testset "matrix powers" begin
    x = rand(Blocks(4,4), 16, 16)
    @test collect(x^1) == collect(x)
    @test collect(x^2) == collect(x*x)
    @test collect(x^3) == collect(x*x*x)
end

@testset "concat" begin
    m = rand(75,75)
    x = Distribute(Blocks(10,20), m)
    y = Distribute(Blocks(10,10), m)
    @test hcat(m,m) == collect(hcat(x,x)) == collect(hcat(x,y))
    @test vcat(m,m) == collect(vcat(x,x))
    @test_throws DimensionMismatch vcat(x,y)
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
            @test vec(collect(X[ragged_idx, 5])) == vec(x[ragged_idx, 5])
            @test vec(collect(X[5, ragged_idx])) == vec(x[5, ragged_idx])
            @test X[5, 5] == x[5,5]
        end
    end

    test_getindex(rand(10,10))
    test_getindex(sprand(10,10,0.5))

    y = rand(10, 10)
    xs = distribute(y, Blocks(2,2))
    for i=1:10, j=1:10
        @test xs[i:j, j:i] == y[i:j, j:i]
    end
end


@testset "cleanup" begin
    X = Distribute(Blocks(10,10), rand(10,10))
    @test collect(sin.(X)) == collect(sin.(X))
end


@testset "reducedim" begin
    x = rand(1:10, 10, 5)
    X = distribute(x, Blocks(3,3))
    @test reduce(+, x, dims=1) == collect(reduce(+, X, dims=1))
    @test reduce(+, x, dims=2) == collect(reduce(+, X, dims=2))

    x = rand(1:10, 10, 5)
    X = distribute(x, Blocks(10, 10))
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
    y = Distribute(Blocks(3), x)
    @test_broken map(x->length(collect(x)), sort(y).chunks) == [3,3,3,1]
end

using MemPool

@testset "affinity" begin
    x = Dagger.tochunk([1:10;])
    aff = Dagger.affinity(x)
    @test aff[1] == Dagger.OSProc(myid())
    @test aff[2] == sizeof(Int)*10
    @test Dagger.tochunk(x) === x
    f = MemPool.FileRef("/tmp/d", aff[2])
    aff = Dagger.affinity(f)
    #@test length(aff) == 3
    @test (aff[1]).pid in procs()
    @test aff[2] == sizeof(Int)*10
end

#=@testset "show_plan" begin
    @test !isempty(Dagger.show_plan(Dagger.spawn(()->10)))
end=#

#= FIXME: Unreliable, may segfault/bus error
@testset "sharedarray" begin
    A = SharedArray{Int}((1024,))
    B = SharedArray{Int}((1024,))
    C = Dagger.merge_sorted(Base.Order.Forward, A, B)
    @test length(C) === length(A) + length(B)
    @test typeof(C) === (Dagger.use_shared_array[] ? SharedArray{Int,1} : Array{Int,1})
end
=#
