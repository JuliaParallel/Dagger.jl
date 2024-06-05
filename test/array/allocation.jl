@testset "DVector/DMatrix/DArray constructor" begin
    for T in [Float32, Float64, Int32, Int64]
        F = fill(one(T))
        V = rand(T, 64)
        M = rand(T, 64, 64)
        A = rand(T, 64, 64, 64)

        # DArray ctor (empty)
        DF = DArray(F, Blocks(()))
        @test DF isa DArray{T,0}
        @test collect(DF) == F
        @test size(DF) == size(F)

        # DVector ctor
        DV = DVector(V, Blocks(8))
        @test DV isa DVector{T}
        @test collect(DV) == V
        @test size(DV) == size(V)

        # DMatrix ctor
        DM = DMatrix(M, Blocks(8, 8))
        @test DM isa DMatrix{T}
        @test collect(DM) == M
        @test size(DM) == size(M)

        # DArray ctor
        DA = DArray(A, Blocks(8, 8, 8))
        @test DA isa DArray{T,3}
        @test collect(DA) == A
        @test size(DA) == size(A)
    end
end

@testset "random" begin
    for T in [Float32, Float64, Int32, Int64]
        for dims in [(),
                     (100,),
                     (100, 100),
                     (100, 100, 100)]
            dist = Blocks(ntuple(i->10, length(dims))...)

            # rand
            X = rand(dist, T, dims...)
            @test X isa DArray{T,length(dims)}
            @test size(X) == dims
            AX = collect(X)
            @test AX isa Array{T,length(dims)}
            @test AX == collect(X)
            @test AX != collect(rand(dist, T, dims...))
            if T <: AbstractFloat
                # FIXME: Not ideal, but I guess sometimes we can get 0?
                @test sum(.!(AX .> 0)) < 10
            end

            if T in [Float32, Float64]
                # randn
                Xn = randn(dist, T, dims...)
                @test Xn isa DArray{T,length(dims)}
                @test size(Xn) == dims
                AXn = collect(Xn)
                @test AXn isa Array{T,length(dims)}
                @test AXn == collect(Xn)
                @test AXn != collect(randn(dist, T, dims...))
            end

            if 1 <= length(dims) <= 2
                # sprand
                Xsp = sprand(dist, T, dims..., 0.1)
                @test Xsp isa DArray{T,length(dims)}
                @test size(Xsp) == dims
                AXsp = collect(Xsp)
                AT = length(dims) == 2 ? SparseMatrixCSC : SparseVector
                @test AXsp isa AT{T}
                @test AXsp == collect(Xsp)
                @test AXsp != collect(sprand(dist, T, dims..., 0.1))
                @test !allunique(AXsp)
                @test !all(AXsp .> 0)
            end
        end
    end
end

@testset "ones/zeros" begin
    for T in [Float32, Float64, Int32, Int64]
        for (fn, value) in [(ones, one(T)), (zeros, zero(T))]
            for dims in [(),
                         (100,),
                         (100, 100),
                         (100, 100, 100)]
                dist = Blocks(ntuple(i->10, length(dims))...)
                DA = fn(dist, T, dims...)
                @test DA isa DArray{T,length(dims)}
                A = collect(DA)
                @test all(A .== value)
                @test eltype(DA) == eltype(A) == T
                @test size(DA) == size(A) == dims
            end
        end
    end
end

@testset "distribute" begin
    function test_dist(X)
        X1 = distribute(X, Blocks(10, 20))
        Xc = fetch(X1)
        @test Xc isa DArray{eltype(X),ndims(X)}
        @test Xc == X
        @test chunks(Xc) |> size == (10, 5)
        @test domainchunks(Xc) |> size == (10, 5)
        @test map(x->size(x) == (10, 20), domainchunks(Xc)) |> all
    end
    x = [1 2; 3 4]
    @test distribute(x, Blocks(1,1)) == x
    test_dist(rand(100, 100))
    test_dist(sprand(100, 100, 0.1))

    x = distribute(rand(10), 2)
    @test collect(distribute(x, 3)) == collect(x)
end

@testset "AutoBlocks" begin
    function test_auto_blocks(DA, dims)
        np = Dagger.num_processors()
        part = DA.partitioning
        @test part isa Blocks
        part_size = part.blocksize
        for i in 1:(length(dims)-1)
            @test part_size[i] == 100
        end
        if length(dims) > 0
            @test part_size[end] == cld(100, np)
        else
            @test part_size == ()
        end
        @test size(DA) == ntuple(i->100, length(dims))
    end

    for dims in [(),
                 (100,),
                 (100, 100),
                 (100, 100, 100)]
        fn = if length(dims) == 1
            DVector
        elseif length(dims) == 2
            DMatrix
        else
            DArray
        end
        if length(dims) > 0
            DA = fn(rand(dims...), AutoBlocks())
        else
            DA = fn(fill(rand()), AutoBlocks())
        end
        test_auto_blocks(DA, dims)

        if length(dims) > 0
            DA = distribute(rand(dims...), AutoBlocks())
        else
            DA = distribute(fill(rand()), AutoBlocks())
        end
        test_auto_blocks(DA, dims)

        for fn in [rand, randn, sprand, ones, zeros]
            if fn === sprand
                if length(dims) > 2 || length(dims) == 0
                    continue
                end
                DA = fn(AutoBlocks(), dims..., 0.1)
            else
                DA = fn(AutoBlocks(), dims...)
            end
            test_auto_blocks(DA, dims)
        end
    end
end

@testset "Constructor variants" begin
    for fn in [ones, zeros, rand, randn, sprand]
        for dims in [(),
                     (100,),
                     (100, 100),
                     (100, 100, 100)]
            for dist in [Blocks(ntuple(i->10, length(dims))...),
                         AutoBlocks()]
                if fn === sprand
                    if length(dims) > 2
                        continue
                    end
                    @test fn(dist, dims..., 0.1) isa DArray{Float64,length(dims)}
                    @test fn(dist, dims, 0.1) isa DArray{Float64,length(dims)}
                    @test fn(dist, Float32, dims..., 0.1) isa DArray{Float32,length(dims)}
                    @test fn(dist, Float32, dims, 0.1) isa DArray{Float32,length(dims)}
                else
                    @test fn(dist, dims...) isa DArray{Float64,length(dims)}
                    @test fn(dist, dims) isa DArray{Float64,length(dims)}
                    @test fn(dist, Float32, dims...) isa DArray{Float32,length(dims)}
                    @test fn(dist, Float32, dims) isa DArray{Float32,length(dims)}
                end
            end
        end
    end
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

@testset "copy/similar" begin
    X1 = ones(Blocks(10, 10), 100, 100)
    X2 = copy(X1)
    X3 = similar(X1)
    @test typeof(X1) === typeof(X2) === typeof(X3)
    @test collect(X1) == collect(X2)
    @test collect(X1) != collect(X3)
end
