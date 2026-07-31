@testset "treereduce_nd" begin
    xs = rand(1:10, 8,8,8)
    concats = [(x...)->cat(x..., dims=n) for n in 1:3]
    @test treereduce_nd(concats, xs) == xs
    @test treereduce_nd(reverse(concats), xs) == permutedims(xs, [3,2,1])
end

@testset "map" begin
    X1 = ones(Blocks(10, 10), 100, 100)
    X2 = map(x->x+1, X1)
    @test typeof(X1) === typeof(X2)
    @test collect(X1) .+ 1 == collect(X2)
end

@testset "map!" begin
    X = zeros(Blocks(10,10), Int, 40, 40)

    map!(_ -> 1, X)
    @test sum(X) == length(X)

    map!(x -> 2x+1, X, Matrix(X))
    @test sum(X) == 3*length(X)
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

@testset "broadcast" begin
    X1 = rand(Blocks(10), 100)
    X2 = X1 .* 3.4
    @test typeof(X1) === typeof(X2)
    @test collect(X1) .* 3.4 == collect(X2)
    X3 = X1 .+ X1
    @test typeof(X1) === typeof(X3)
    @test collect(X1) .* 2 == collect(X3)
end

@testset "transpose" begin
    function test_transpose(X)
        x, y = size(X)
        X1 = distribute(X, Blocks(10, 20))
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

@testset "concat" begin
    m = rand(75,75)
    x = distribute(m, Blocks(10,20))
    y = distribute(m, Blocks(10,10))
    @test hcat(m,m) == collect(hcat(x,x)) == collect(hcat(x,y))
    @test vcat(m,m) == collect(vcat(x,x))
    @test_throws DimensionMismatch vcat(x,y)
end

@testset "scale" begin
    x = rand(10,10)
    X = distribute(x, Blocks(3,3))
    y = rand(10)

    @test Diagonal(y)*x == collect(Diagonal(y)*X)
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
    y = distribute(x, Blocks(3))
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
