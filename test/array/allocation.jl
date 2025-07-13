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

function chunk_processors(Ad::DArray)
    [Dagger.processor(fetch(Ad.chunks[idx]; raw=true)) for idx in CartesianIndices(size(domainchunks(Ad)))]
end
function tile_processors(proc_grid::AbstractArray{<:Dagger.Processor,N}, block_grid::Tuple{Vararg{Int,N}}) where N
    reps       = Int.(ceil.(block_grid ./ size(proc_grid)))
    tiled      = repeat(proc_grid, reps...)
    idx_slices = [1:block_grid[d] for d in 1:length(block_grid)]
    return tiled[idx_slices...]
end
function get_default_blockgrid(data, numprocs)
    ndims_data = ndims(data)
    size_data  = size(data)
    ntuple(i->i == ndims_data ? cld( size_data[ndims_data], cld(size_data[ndims_data], numprocs) ) : 1, ndims_data)
end
function wait_array(A)
    # Use this to ensure we don't create increasing numbers of background tasks over time
    for chunk in A.chunks
        wait(chunk)
    end
    return A
end

availprocs = collect(Dagger.all_processors())
sort!(availprocs, by = x -> (x.owner, x.tid))
numprocs = length(availprocs)

A = rand(41, 35, 12)
v = rand(23)
M = rand(76,118)

t_blocks_a = (4,3,2)
d_blocks_a = Blocks(t_blocks_a)
blocks_a   = cld.(size(A), t_blocks_a)

n_blocks_v = 3
t_blocks_v = (n_blocks_v,)
d_blocks_v = Blocks(t_blocks_v)
blocks_v   = cld.(size(v), t_blocks_v)
blocks_nv  = blocks_v[1]

t_blocks_m = (2,3)
d_blocks_m = Blocks(t_blocks_m)
blocks_m   = cld.(size(M), t_blocks_m)
@testset "Constructor with assignment" begin
    @testset "Arbitrary Assignment (:arbitrary)" begin
        assignment = :arbitrary

        @testset "Auto Blocks" begin
            @test wait_array(distribute(A, assignment)) isa DArray  && wait_array(distribute(A, AutoBlocks(), assignment)) isa DArray
            @test wait_array(distribute(v, assignment)) isa DVector && wait_array(distribute(v, AutoBlocks(), assignment)) isa DVector
            @test wait_array(distribute(M, assignment)) isa DMatrix && wait_array(distribute(M, AutoBlocks(), assignment)) isa DMatrix

            @test wait_array(DArray( A,    assignment)) isa DArray  && wait_array(DArray(    A, AutoBlocks(), assignment)) isa DArray
            @test wait_array(DVector(v,    assignment)) isa DVector && wait_array(DVector(   v, AutoBlocks(), assignment)) isa DVector
            @test wait_array(DMatrix(M,    assignment)) isa DMatrix && wait_array(DMatrix(   M, AutoBlocks(), assignment)) isa DMatrix

            @test wait_array(rand(  AutoBlocks(), size(A)...     ; assignment)) isa DArray  && wait_array(rand(  AutoBlocks(), size(A);      assignment)) isa DArray
            @test wait_array(rand(  AutoBlocks(), size(v)...     ; assignment)) isa DVector && wait_array(rand(  AutoBlocks(), size(v);      assignment)) isa DVector
            @test wait_array(rand(  AutoBlocks(), size(M)...     ; assignment)) isa DMatrix && wait_array(rand(  AutoBlocks(), size(M);      assignment)) isa DMatrix

            @test wait_array(randn( AutoBlocks(), size(A)...     ; assignment)) isa DArray  && wait_array(randn( AutoBlocks(), size(A);      assignment)) isa DArray
            @test wait_array(randn( AutoBlocks(), size(v)...     ; assignment)) isa DVector && wait_array(randn( AutoBlocks(), size(v);      assignment)) isa DVector
            @test wait_array(randn( AutoBlocks(), size(M)...     ; assignment)) isa DMatrix && wait_array(randn( AutoBlocks(), size(M);      assignment)) isa DMatrix

            @test wait_array(sprand(AutoBlocks(), size(v)..., 0.5; assignment)) isa DVector && wait_array(sprand(AutoBlocks(), size(v), 0.5; assignment)) isa DVector
            @test wait_array(sprand(AutoBlocks(), size(M)..., 0.5; assignment)) isa DMatrix && wait_array(sprand(AutoBlocks(), size(M), 0.5; assignment)) isa DMatrix

            @test wait_array(ones(  AutoBlocks(), size(A)...     ; assignment)) isa DArray  && wait_array(ones(  AutoBlocks(), size(A);      assignment)) isa DArray
            @test wait_array(ones(  AutoBlocks(), size(v)...     ; assignment)) isa DVector && wait_array(ones(  AutoBlocks(), size(v);      assignment)) isa DVector
            @test wait_array(ones(  AutoBlocks(), size(M)...     ; assignment)) isa DMatrix && wait_array(ones(  AutoBlocks(), size(M);      assignment)) isa DMatrix

            @test wait_array(zeros( AutoBlocks(), size(A)...     ; assignment)) isa DArray  && wait_array(zeros( AutoBlocks(), size(A);      assignment)) isa DArray
            @test wait_array(zeros( AutoBlocks(), size(v)...     ; assignment)) isa DVector && wait_array(zeros( AutoBlocks(), size(v);      assignment)) isa DVector
            @test wait_array(zeros( AutoBlocks(), size(M)...     ; assignment)) isa DMatrix && wait_array(zeros( AutoBlocks(), size(M);      assignment)) isa DMatrix
        end

        @testset "Explicit Blocks" begin
            @test wait_array(distribute(A, d_blocks_a, assignment)) isa DArray  && wait_array(distribute(A, blocks_a, assignment)) isa DArray
            @test wait_array(distribute(v, d_blocks_v, assignment)) isa DVector && wait_array(distribute(v, blocks_v,  assignment)) isa DVector
            @test wait_array(distribute(v, n_blocks_v, assignment)) isa DVector
            @test wait_array(distribute(M, d_blocks_m, assignment)) isa DMatrix && wait_array(distribute(M, blocks_m, assignment)) isa DMatrix

            @test wait_array(DArray( A, d_blocks_a, assignment)) isa DArray
            @test wait_array(DVector(v, d_blocks_v, assignment)) isa DVector
            @test wait_array(DMatrix(M, d_blocks_m, assignment)) isa DMatrix

            @test wait_array(rand(  d_blocks_a, size(A)...     ; assignment)) isa DArray  && wait_array(rand(  d_blocks_a, size(A);      assignment)) isa DArray
            @test wait_array(rand(  d_blocks_v, size(v)...     ; assignment)) isa DVector && wait_array(rand(  d_blocks_v, size(v);      assignment)) isa DVector
            @test wait_array(rand(  d_blocks_m, size(M)...     ; assignment)) isa DMatrix && wait_array(rand(  d_blocks_m, size(M);      assignment)) isa DMatrix

            @test wait_array(randn( d_blocks_a, size(A)...     ; assignment)) isa DArray  && wait_array(randn( d_blocks_a, size(A);      assignment)) isa DArray
            @test wait_array(randn( d_blocks_v, size(v)...     ; assignment)) isa DVector && wait_array(randn( d_blocks_v, size(v);      assignment)) isa DVector
            @test wait_array(randn( d_blocks_m, size(M)...     ; assignment)) isa DMatrix && wait_array(randn( d_blocks_m, size(M);      assignment)) isa DMatrix

            @test wait_array(sprand(d_blocks_v, size(v)..., 0.5; assignment)) isa DVector && wait_array(sprand(d_blocks_v, size(v), 0.5; assignment)) isa DVector
            @test wait_array(sprand(d_blocks_m, size(M)..., 0.5; assignment)) isa DMatrix && wait_array(sprand(d_blocks_m, size(M), 0.5; assignment)) isa DMatrix

            @test wait_array(ones(  d_blocks_a, size(A)...     ; assignment)) isa DArray  && wait_array(ones(  d_blocks_a, size(A);      assignment)) isa DArray
            @test wait_array(ones(  d_blocks_v, size(v)...     ; assignment)) isa DVector && wait_array(ones(  d_blocks_v, size(v);      assignment)) isa DVector
            @test wait_array(ones(  d_blocks_m, size(M)...     ; assignment)) isa DMatrix && wait_array(ones(  d_blocks_m, size(M);      assignment)) isa DMatrix

            @test wait_array(zeros( d_blocks_a, size(A)...     ; assignment)) isa DArray  && wait_array(zeros( d_blocks_a, size(A);      assignment)) isa DArray
            @test wait_array(zeros( d_blocks_v, size(v)...     ; assignment)) isa DVector && wait_array(zeros( d_blocks_v, size(v);      assignment)) isa DVector
            @test wait_array(zeros( d_blocks_m, size(M)...     ; assignment)) isa DMatrix && wait_array(zeros( d_blocks_m, size(M);      assignment)) isa DMatrix
        end
    end
end

function get_blockrow_procgrid(data, numprocs, blocksize)
    ndims_data = ndims(data)
    p = ntuple(i -> i == 1 ? blocksize[1] : 1, ndims_data)
    rows_per_proc, extra = divrem(blocksize[1], numprocs)
    counts = [rows_per_proc + (i <= extra ? 1 : 0) for i in 1:numprocs]
    procgrid = reshape(vcat(fill.(availprocs, counts)...), p)
    return procgrid
end
function get_blockcol_procgrid(data, numprocs, blocksize)
    ndims_data = ndims(data)
    p = ntuple(i -> i == ndims_data ? blocksize[end] : 1, ndims_data)
    cols_per_proc, extra = divrem(blocksize[end], numprocs)
    counts = [cols_per_proc + (i <= extra ? 1 : 0) for i in 1:numprocs]
    procgrid = reshape(vcat(fill.(availprocs, counts)...), p)
    return procgrid
end
function get_cyclicrow_procgrid(data, numprocs, blocksize)
    ndims_data = ndims(data)
    p = ntuple(i -> i == 1 ? numprocs : 1, ndims_data)
    procgrid = reshape(availprocs, p)
    return procgrid
end
function get_cycliccol_procgrid(data, numprocs, blocksize)
    ndims_data = ndims(data)
    p = ntuple(i -> i == ndims_data ? numprocs : 1, ndims_data)
    procgrid = reshape(availprocs, p)
    return procgrid
end
function test_assignment_strategy(assignment::Symbol, get_assignment_procgrid)
    @testset "Block Row Assignment (:$assignment)" begin
        @testset "Auto Blocks" begin
            dist_A_def_auto = wait_array(distribute(A,               assignment))
            dist_A_auto_def = wait_array(distribute(A, AutoBlocks(), assignment))
            dist_v_def_auto = wait_array(distribute(v,               assignment))
            dist_v_auto_def = wait_array(distribute(v, AutoBlocks(), assignment))
            dist_M_def_auto = wait_array(distribute(M,               assignment))
            dist_M_auto_def = wait_array(distribute(M, AutoBlocks(), assignment))

            darr_A_def_auto = wait_array(DArray(    A,               assignment))
            darr_A_auto_def = wait_array(DArray(    A, AutoBlocks(), assignment))
            dvec_v_def_auto = wait_array(DVector(   v,               assignment))
            dvec_v_auto_def = wait_array(DVector(   v, AutoBlocks(), assignment))
            dmat_M_def_auto = wait_array(DMatrix(   M,               assignment))
            dmat_M_auto_def = wait_array(DMatrix(   M, AutoBlocks(), assignment))

            @test chunk_processors(dist_A_def_auto) == chunk_processors(dist_A_auto_def) == chunk_processors(darr_A_def_auto) == chunk_processors(darr_A_auto_def) == tile_processors(get_assignment_procgrid(A, numprocs, get_default_blockgrid(A, numprocs)), get_default_blockgrid(A, numprocs))
            @test chunk_processors(dist_v_def_auto) == chunk_processors(dist_v_auto_def) == chunk_processors(dvec_v_def_auto) == chunk_processors(dvec_v_auto_def) == tile_processors(get_assignment_procgrid(v, numprocs, get_default_blockgrid(v, numprocs)), get_default_blockgrid(v, numprocs))
            @test chunk_processors(dist_M_def_auto) == chunk_processors(dist_M_auto_def) == chunk_processors(dmat_M_def_auto) == chunk_processors(dmat_M_auto_def) == tile_processors(get_assignment_procgrid(M, numprocs, get_default_blockgrid(M, numprocs)), get_default_blockgrid(M, numprocs))
        end

        @testset "Functions with AutoBlocks" begin
            rand_A_auto   =        wait_array(rand(  AutoBlocks(), size(A)...; assignment))
            rand_v_auto   =        wait_array(rand(  AutoBlocks(), size(v)...; assignment))
            rand_M_auto   =        wait_array(rand(  AutoBlocks(), size(M)...; assignment))

            randn_A_auto  =        wait_array(randn( AutoBlocks(), size(A)...; assignment))
            randn_v_auto  =        wait_array(randn( AutoBlocks(), size(v)...; assignment))
            randn_M_auto  =        wait_array(randn( AutoBlocks(), size(M)...; assignment))

            sprand_v_auto = wait_array(sprand(AutoBlocks(), size(v)..., 0.5; assignment))
            sprand_M_auto = wait_array(sprand(AutoBlocks(), size(M)..., 0.5; assignment))

            ones_A_auto   =        wait_array(ones(  AutoBlocks(), size(A)...; assignment))
            ones_v_auto   =        wait_array(ones(  AutoBlocks(), size(v)...; assignment))
            ones_M_auto   =        wait_array(ones(  AutoBlocks(), size(M)...; assignment))

            zeros_A_auto  =        wait_array(zeros( AutoBlocks(), size(A)...; assignment))
            zeros_v_auto  =        wait_array(zeros( AutoBlocks(), size(v)...; assignment))
            zeros_M_auto  =        wait_array(zeros( AutoBlocks(), size(M)...; assignment))

            @test chunk_processors(rand_A_auto)   == chunk_processors(randn_A_auto) ==                                    chunk_processors(ones_A_auto) == chunk_processors(zeros_A_auto) ==  tile_processors(get_assignment_procgrid(A, numprocs, get_default_blockgrid(A, numprocs)), get_default_blockgrid(A, numprocs))
            @test chunk_processors(rand_v_auto)   == chunk_processors(randn_v_auto) == chunk_processors(sprand_v_auto) == chunk_processors(ones_v_auto) == chunk_processors(zeros_v_auto) ==  tile_processors(get_assignment_procgrid(v, numprocs, get_default_blockgrid(v, numprocs)), get_default_blockgrid(v, numprocs))
            @test chunk_processors(rand_M_auto)   == chunk_processors(randn_M_auto) == chunk_processors(sprand_M_auto) == chunk_processors(ones_M_auto) == chunk_processors(zeros_M_auto) ==  tile_processors(get_assignment_procgrid(M, numprocs, get_default_blockgrid(M, numprocs)), get_default_blockgrid(M, numprocs))
        end

        @testset "Explicit Blocks" begin
            dist_A_exp_def     = wait_array(distribute(A, d_blocks_a, assignment))
            dist_A_blocks_exp  = wait_array(distribute(A, blocks_a,   assignment))
            dist_v_exp_def     = wait_array(distribute(v, d_blocks_v, assignment))
            dist_v_blocks_exp  = wait_array(distribute(v, blocks_v,   assignment))
            dist_v_nblocks_exp = wait_array(distribute(v, blocks_nv,  assignment))
            dist_M_exp_def     = wait_array(distribute(M, d_blocks_m, assignment))
            dist_M_blocks_exp  = wait_array(distribute(M, blocks_m,   assignment))

            darr_A_exp_def     = wait_array(DArray(    A, d_blocks_a, assignment))
            dvec_v_exp_def     = wait_array(DVector(   v, d_blocks_v, assignment))
            dmat_M_exp_def     = wait_array(DMatrix(   M, d_blocks_m, assignment))

            @test chunk_processors(dist_A_exp_def)      == chunk_processors(dist_A_blocks_exp) == chunk_processors(darr_A_exp_def) == tile_processors(get_assignment_procgrid(A, numprocs, blocks_a), blocks_a)
            @test chunk_processors(dist_v_exp_def)      == chunk_processors(dist_v_blocks_exp) == chunk_processors(dvec_v_exp_def) == tile_processors(get_assignment_procgrid(v, numprocs, blocks_v), blocks_v)
            @test chunk_processors(dist_v_nblocks_exp)                                                                             == tile_processors(get_assignment_procgrid(v, numprocs, blocks_v), blocks_v)
            @test chunk_processors(dist_M_exp_def)      == chunk_processors(dist_M_blocks_exp) == chunk_processors(dmat_M_exp_def) == tile_processors(get_assignment_procgrid(M, numprocs, blocks_m), blocks_m)
        end

        @testset "Functions with Explicit Blocks" begin
            rand_A_exp   =        wait_array(rand(  d_blocks_a, size(A)...; assignment))
            rand_v_exp   =        wait_array(rand(  d_blocks_v, size(v)...; assignment))
            rand_M_exp   =        wait_array(rand(  d_blocks_m, size(M)...; assignment))

            randn_A_exp   =        wait_array(randn( d_blocks_a, size(A)...; assignment))
            randn_v_exp   =        wait_array(randn( d_blocks_v, size(v)...; assignment))
            randn_M_exp   =        wait_array(randn( d_blocks_m, size(M)...; assignment))

            sprand_v_exp  = wait_array(sprand(d_blocks_v, size(v)..., 0.5; assignment))
            sprand_M_exp  = wait_array(sprand(d_blocks_m, size(M)..., 0.5; assignment))

            ones_A_exp    =        wait_array(ones(  d_blocks_a, size(A)...; assignment))
            ones_v_exp    =        wait_array(ones(  d_blocks_v, size(v)...; assignment))
            ones_M_exp    =        wait_array(ones(  d_blocks_m, size(M)...; assignment))

            zeros_A_exp   =        wait_array(zeros( d_blocks_a, size(A)...; assignment))
            zeros_v_exp   =        wait_array(zeros( d_blocks_v, size(v)...; assignment))
            zeros_M_exp   =        wait_array(zeros( d_blocks_m, size(M)...; assignment))

            @test chunk_processors(rand_A_exp) == chunk_processors(randn_A_exp) ==                                   chunk_processors(ones_A_exp) == chunk_processors(zeros_A_exp) == tile_processors(get_assignment_procgrid(A, numprocs, blocks_a), blocks_a)
            @test chunk_processors(rand_v_exp) == chunk_processors(randn_v_exp) == chunk_processors(sprand_v_exp) == chunk_processors(ones_v_exp) == chunk_processors(zeros_v_exp) == tile_processors(get_assignment_procgrid(v, numprocs, blocks_v), blocks_v)
            @test chunk_processors(rand_M_exp) == chunk_processors(randn_M_exp) == chunk_processors(sprand_M_exp) == chunk_processors(ones_M_exp) == chunk_processors(zeros_M_exp) == tile_processors(get_assignment_procgrid(M, numprocs, blocks_m), blocks_m)
        end
    end
end

test_assignment_strategy(:blockrow,  get_blockrow_procgrid)
test_assignment_strategy(:blockcol,  get_blockcol_procgrid)
test_assignment_strategy(:cyclicrow, get_cyclicrow_procgrid)
test_assignment_strategy(:cycliccol, get_cycliccol_procgrid)

@testset "OSProc ID Array Assignment (AbstractArray{<:Int, N})" begin
    function get_random_threadprocs(proc_ids)
        [ThreadProc(proc, 1) for proc in proc_ids]
    end

    rand_osproc_ids_A = rand(procs(), 3, 2, 2)
    rand_osproc_ids_v = rand(procs(), 11)
    rand_osproc_ids_M = rand(procs(), 2, 5)

    @testset "Auto Blocks" begin
        dist_A_rand_osproc_auto = wait_array(distribute(A,               rand_osproc_ids_A))
        dist_A_auto_rand_osproc = wait_array(distribute(A, AutoBlocks(), rand_osproc_ids_A))
        dist_v_rand_osproc_auto = wait_array(distribute(v,               rand_osproc_ids_v))
        dist_v_auto_rand_osproc = wait_array(distribute(v, AutoBlocks(), rand_osproc_ids_v))
        dist_M_rand_osproc_auto = wait_array(distribute(M,               rand_osproc_ids_M))
        dist_M_auto_rand_osproc = wait_array(distribute(M, AutoBlocks(), rand_osproc_ids_M))

        darr_A_rand_osproc_auto = wait_array(DArray(    A,               rand_osproc_ids_A))
        darr_A_auto_rand_osproc = wait_array(DArray(    A, AutoBlocks(), rand_osproc_ids_A))
        dvec_v_rand_osproc_auto = wait_array(DVector(   v,               rand_osproc_ids_v))
        dvec_v_auto_rand_osproc = wait_array(DVector(   v, AutoBlocks(), rand_osproc_ids_v))
        dmat_M_rand_osproc_auto = wait_array(DMatrix(   M,               rand_osproc_ids_M))
        dmat_M_auto_rand_osproc = wait_array(DMatrix(   M, AutoBlocks(), rand_osproc_ids_M))

        @test chunk_processors(dist_A_rand_osproc_auto) == chunk_processors(dist_A_auto_rand_osproc) == chunk_processors(darr_A_rand_osproc_auto) == chunk_processors(darr_A_auto_rand_osproc) == tile_processors(get_random_threadprocs(rand_osproc_ids_A), get_default_blockgrid(A, numprocs))
        @test                                              chunk_processors(dist_v_auto_rand_osproc) == chunk_processors(dvec_v_rand_osproc_auto) == chunk_processors(dvec_v_auto_rand_osproc) == tile_processors(get_random_threadprocs(rand_osproc_ids_v), get_default_blockgrid(v, numprocs))
        @test chunk_processors(dist_v_rand_osproc_auto) == tile_processors(get_random_threadprocs(rand_osproc_ids_v), get_default_blockgrid(v, numprocs))
        @test chunk_processors(dist_M_rand_osproc_auto) == chunk_processors(dist_M_auto_rand_osproc)  == chunk_processors(dmat_M_auto_rand_osproc) == tile_processors(get_random_threadprocs(rand_osproc_ids_M), get_default_blockgrid(M, numprocs))
        @test chunk_processors(dmat_M_rand_osproc_auto) == tile_processors(get_random_threadprocs(rand_osproc_ids_M), get_default_blockgrid(M, numprocs))
    end

    @testset "Functions with AutoBlocks" begin
        rand_A_auto    =        wait_array(rand(  AutoBlocks(), size(A)...; assignment=rand_osproc_ids_A))
        rand_v_auto    =        wait_array(rand(  AutoBlocks(), size(v)...; assignment=rand_osproc_ids_v))
        rand_M_auto    =        wait_array(rand(  AutoBlocks(), size(M)...; assignment=rand_osproc_ids_M))

        randn_A_auto   =        wait_array(randn( AutoBlocks(), size(A)...; assignment=rand_osproc_ids_A))
        randn_v_auto   =        wait_array(randn( AutoBlocks(), size(v)...; assignment=rand_osproc_ids_v))
        randn_M_auto   =        wait_array(randn( AutoBlocks(), size(M)...; assignment=rand_osproc_ids_M))

        sprand_v_auto  = wait_array(sprand(AutoBlocks(), size(v)..., 0.5; assignment=rand_osproc_ids_v))
        sprand_M_auto  = wait_array(sprand(AutoBlocks(), size(M)..., 0.5; assignment=rand_osproc_ids_M))

        ones_A_auto    =        wait_array(ones(  AutoBlocks(), size(A)...; assignment=rand_osproc_ids_A))
        ones_v_auto    =        wait_array(ones(  AutoBlocks(), size(v)...; assignment=rand_osproc_ids_v))
        ones_M_auto    =        wait_array(ones(  AutoBlocks(), size(M)...; assignment=rand_osproc_ids_M))

        zeros_A_auto   =        wait_array(zeros( AutoBlocks(), size(A)...; assignment=rand_osproc_ids_A))
        zeros_v_auto   =        wait_array(zeros( AutoBlocks(), size(v)...; assignment=rand_osproc_ids_v))
        zeros_M_auto   =        wait_array(zeros( AutoBlocks(), size(M)...; assignment=rand_osproc_ids_M))


        @test chunk_processors(rand_A_auto)   == chunk_processors(randn_A_auto) ==                                    chunk_processors(ones_A_auto) == chunk_processors(zeros_A_auto) == tile_processors(get_random_threadprocs(rand_osproc_ids_A), get_default_blockgrid(rand_A_auto, numprocs))
        @test chunk_processors(rand_v_auto)   == chunk_processors(randn_v_auto) == chunk_processors(sprand_v_auto) == chunk_processors(ones_v_auto) == chunk_processors(zeros_v_auto) == tile_processors(get_random_threadprocs(rand_osproc_ids_v), get_default_blockgrid(rand_v_auto, numprocs))
        @test chunk_processors(rand_M_auto)   == chunk_processors(randn_M_auto) == chunk_processors(sprand_M_auto) == chunk_processors(ones_M_auto) == chunk_processors(zeros_M_auto) == tile_processors(get_random_threadprocs(rand_osproc_ids_M), get_default_blockgrid(rand_M_auto, numprocs))
    end

    @testset "Explicit Blocks" begin
        dist_A_exp_rand_osproc     = wait_array(distribute(A, d_blocks_a, rand_osproc_ids_A))
        dist_A_blocks_rand_osproc  = wait_array(distribute(A, blocks_a,   rand_osproc_ids_A))
        dist_v_exp_rand_osproc     = wait_array(distribute(v, d_blocks_v, rand_osproc_ids_v))
        dist_v_blocks_rand_osproc  = wait_array(distribute(v, blocks_v,   rand_osproc_ids_v))
        dist_v_nblocks_rand_osproc = wait_array(distribute(v, blocks_nv,  rand_osproc_ids_v))
        dist_M_exp_rand_osproc     = wait_array(distribute(M, d_blocks_m, rand_osproc_ids_M))
        dist_M_blocks_rand_osproc  = wait_array(distribute(M, blocks_m,   rand_osproc_ids_M))

        darr_A_exp_rand_osproc     = wait_array(DArray(    A, d_blocks_a, rand_osproc_ids_A))
        dvec_v_exp_rand_osproc     = wait_array(DVector(   v, d_blocks_v, rand_osproc_ids_v))
        dmat_M_exp_rand_osproc     = wait_array(DMatrix(   M, d_blocks_m, rand_osproc_ids_M))

        @test chunk_processors(dist_A_exp_rand_osproc) == chunk_processors(dist_A_blocks_rand_osproc) == chunk_processors(darr_A_exp_rand_osproc) == tile_processors(get_random_threadprocs(rand_osproc_ids_A), blocks_a)
        @test chunk_processors(dist_v_exp_rand_osproc) == chunk_processors(dist_v_blocks_rand_osproc) == chunk_processors(dvec_v_exp_rand_osproc) == tile_processors(get_random_threadprocs(rand_osproc_ids_v), blocks_v)
        @test chunk_processors(dist_v_nblocks_rand_osproc)                                                                                        == tile_processors(get_random_threadprocs(rand_osproc_ids_v), blocks_v)
        @test chunk_processors(dist_M_exp_rand_osproc) == chunk_processors(dist_M_blocks_rand_osproc) == chunk_processors(dmat_M_exp_rand_osproc) == tile_processors(get_random_threadprocs(rand_osproc_ids_M), blocks_m)
    end

    @testset "Functions with Explicit Blocks" begin
        rand_A_exp    =        wait_array(rand(  d_blocks_a, size(A)...; assignment=rand_osproc_ids_A))
        rand_v_exp    =        wait_array(rand(  d_blocks_v, size(v)...; assignment=rand_osproc_ids_v))
        rand_M_exp    =        wait_array(rand(  d_blocks_m, size(M)...; assignment=rand_osproc_ids_M))

        randn_A_exp   =        wait_array(randn( d_blocks_a, size(A)...; assignment=rand_osproc_ids_A))
        randn_v_exp   =        wait_array(randn( d_blocks_v, size(v)...; assignment=rand_osproc_ids_v))
        randn_M_exp   =        wait_array(randn( d_blocks_m, size(M)...; assignment=rand_osproc_ids_M))

        sprand_v_exp  = wait_array(sprand(d_blocks_v, size(v)..., 0.5; assignment=rand_osproc_ids_v))
        sprand_M_exp  = wait_array(sprand(d_blocks_m, size(M)..., 0.5; assignment=rand_osproc_ids_M))

        ones_A_exp    =        wait_array(ones(  d_blocks_a, size(A)...; assignment=rand_osproc_ids_A))
        ones_v_exp    =        wait_array(ones(  d_blocks_v, size(v)...; assignment=rand_osproc_ids_v))
        ones_M_exp    =        wait_array(ones(  d_blocks_m, size(M)...; assignment=rand_osproc_ids_M))

        zeros_A_exp   =        wait_array(zeros( d_blocks_a, size(A)...; assignment=rand_osproc_ids_A))
        zeros_v_exp   =        wait_array(zeros( d_blocks_v, size(v)...; assignment=rand_osproc_ids_v))
        zeros_M_exp   =        wait_array(zeros( d_blocks_m, size(M)...; assignment=rand_osproc_ids_M))


        @test chunk_processors(rand_A_exp)   == chunk_processors(randn_A_exp) ==                                   chunk_processors(ones_A_exp) == chunk_processors(zeros_A_exp) == tile_processors(get_random_threadprocs(rand_osproc_ids_A), blocks_a)
        @test chunk_processors(rand_v_exp)   == chunk_processors(randn_v_exp) == chunk_processors(sprand_v_exp) == chunk_processors(ones_v_exp) == chunk_processors(zeros_v_exp) == tile_processors(get_random_threadprocs(rand_osproc_ids_v), blocks_v)
        @test chunk_processors(rand_M_exp)   == chunk_processors(randn_M_exp) == chunk_processors(sprand_M_exp) == chunk_processors(ones_M_exp) == chunk_processors(zeros_M_exp) == tile_processors(get_random_threadprocs(rand_osproc_ids_M), blocks_m)
    end
end

@testset "Explicit Processor Array Assignment (AbstractArray{<:Processor, N})" begin
    rand_procs_A = reshape(availprocs[ rand(procs(),  6) ], 2, 3, 1)
    rand_procs_v = reshape(availprocs[ rand(procs(),  5) ], 5)
    rand_procs_M = reshape(availprocs[ rand(procs(), 14) ], 2, 7)

    @testset "Auto Blocks" begin
        dist_A_rand_procs_auto = wait_array(distribute(A,               rand_procs_A))
        dist_A_auto_rand_procs = wait_array(distribute(A, AutoBlocks(), rand_procs_A))
        dist_v_rand_procs_auto = wait_array(distribute(v,               rand_procs_v))
        dist_v_auto_rand_procs = wait_array(distribute(v, AutoBlocks(), rand_procs_v))
        dist_M_rand_procs_auto = wait_array(distribute(M,               rand_procs_M))
        dist_M_auto_rand_procs = wait_array(distribute(M, AutoBlocks(), rand_procs_M))

        darr_A_rand_procs_auto = wait_array(DArray(    A,               rand_procs_A))
        darr_A_auto_rand_procs = wait_array(DArray(    A, AutoBlocks(), rand_procs_A))
        dvec_v_rand_procs_auto = wait_array(DVector(   v,               rand_procs_v))
        dvec_v_auto_rand_procs = wait_array(DVector(   v, AutoBlocks(), rand_procs_v))
        dmat_M_rand_procs_auto = wait_array(DMatrix(   M,               rand_procs_M))
        dmat_M_auto_rand_procs = wait_array(DMatrix(   M, AutoBlocks(), rand_procs_M))

        @test chunk_processors(dist_A_rand_procs_auto) == chunk_processors(dist_A_auto_rand_procs) == chunk_processors(darr_A_rand_procs_auto) == chunk_processors(darr_A_auto_rand_procs) == tile_processors(rand_procs_A, get_default_blockgrid(A, numprocs))
        @test chunk_processors(dist_v_rand_procs_auto) == chunk_processors(dist_v_auto_rand_procs) == chunk_processors(dvec_v_rand_procs_auto) == chunk_processors(dvec_v_auto_rand_procs) == tile_processors(rand_procs_v, get_default_blockgrid(v, numprocs))
        @test chunk_processors(dist_M_rand_procs_auto) == chunk_processors(dist_M_auto_rand_procs) == chunk_processors(dmat_M_rand_procs_auto) == chunk_processors(dmat_M_auto_rand_procs) == tile_processors(rand_procs_M, get_default_blockgrid(M, numprocs))
    end

    @testset "Functions with AutoBlocks" begin
        rand_A_auto    =        wait_array(rand(  AutoBlocks(), size(A)...; assignment=rand_procs_A))
        rand_v_auto    =        wait_array(rand(  AutoBlocks(), size(v)...; assignment=rand_procs_v))
        rand_M_auto    =        wait_array(rand(  AutoBlocks(), size(M)...; assignment=rand_procs_M))

        randn_A_auto   =        wait_array(randn( AutoBlocks(), size(A)...; assignment=rand_procs_A))
        randn_v_auto   =        wait_array(randn( AutoBlocks(), size(v)...; assignment=rand_procs_v))
        randn_M_auto   =        wait_array(randn( AutoBlocks(), size(M)...; assignment=rand_procs_M))

        sprand_v_auto  = wait_array(sprand(AutoBlocks(), size(v)..., 0.5; assignment=rand_procs_v))
        sprand_M_auto  = wait_array(sprand(AutoBlocks(), size(M)..., 0.5; assignment=rand_procs_M))

        ones_A_auto    =        wait_array(ones(  AutoBlocks(), size(A)...; assignment=rand_procs_A))
        ones_v_auto    =        wait_array(ones(  AutoBlocks(), size(v)...; assignment=rand_procs_v))
        ones_M_auto    =        wait_array(ones(  AutoBlocks(), size(M)...; assignment=rand_procs_M))

        zeros_A_auto   =        wait_array(zeros( AutoBlocks(), size(A)...; assignment=rand_procs_A))
        zeros_v_auto   =        wait_array(zeros( AutoBlocks(), size(v)...; assignment=rand_procs_v))
        zeros_M_auto   =        wait_array(zeros( AutoBlocks(), size(M)...; assignment=rand_procs_M))


        @test chunk_processors(rand_A_auto)   == chunk_processors(randn_A_auto) ==                                    chunk_processors(ones_A_auto) == chunk_processors(zeros_A_auto) == tile_processors(rand_procs_A, get_default_blockgrid(A, numprocs))
        @test chunk_processors(rand_v_auto)   == chunk_processors(randn_v_auto) == chunk_processors(sprand_v_auto) == chunk_processors(ones_v_auto) == chunk_processors(zeros_v_auto) == tile_processors(rand_procs_v, get_default_blockgrid(v, numprocs))
        @test chunk_processors(rand_M_auto)   == chunk_processors(randn_M_auto) == chunk_processors(sprand_M_auto) == chunk_processors(ones_M_auto) == chunk_processors(zeros_M_auto) == tile_processors(rand_procs_M, get_default_blockgrid(M, numprocs))
    end

    @testset "Explicit Blocks" begin
        dist_A_exp_rand_procs     = wait_array(distribute(A, d_blocks_a, rand_procs_A))
        dist_A_blocks_rand_procs  = wait_array(distribute(A, blocks_a,   rand_procs_A))
        dist_v_exp_rand_procs     = wait_array(distribute(v, d_blocks_v, rand_procs_v))
        dist_v_blocks_rand_procs  = wait_array(distribute(v, blocks_v,   rand_procs_v))
        dist_v_nblocks_rand_procs = wait_array(distribute(v, blocks_nv, rand_procs_v))
        dist_M_exp_rand_procs     = wait_array(distribute(M, d_blocks_m, rand_procs_M))
        dist_M_blocks_rand_procs  = wait_array(distribute(M, blocks_m,   rand_procs_M))

        darr_A_exp_rand_procs     = wait_array(DArray(    A, d_blocks_a, rand_procs_A))
        dvec_v_exp_rand_procs     = wait_array(DVector(   v, d_blocks_v, rand_procs_v))
        dmat_M_exp_rand_procs     = wait_array(DMatrix(   M, d_blocks_m, rand_procs_M))

        @test chunk_processors(dist_A_exp_rand_procs)     == chunk_processors(dist_A_blocks_rand_procs)  == chunk_processors(darr_A_exp_rand_procs) == tile_processors(rand_procs_A, blocks_a)
        @test chunk_processors(dist_v_exp_rand_procs)     == chunk_processors(dist_v_blocks_rand_procs)  == chunk_processors(dvec_v_exp_rand_procs) == tile_processors(rand_procs_v, blocks_v)
        @test chunk_processors(dist_v_nblocks_rand_procs)                                                                                           == tile_processors(rand_procs_v, blocks_v)
        @test chunk_processors(dist_M_exp_rand_procs)     == chunk_processors(dist_M_blocks_rand_procs)  == chunk_processors(dmat_M_exp_rand_procs) == tile_processors(rand_procs_M, blocks_m)
    end

    @testset "Functions with Explicit Blocks" begin
        rand_A_exp    =        wait_array(rand(  d_blocks_a, size(A)...; assignment=rand_procs_A))
        rand_v_exp    =        wait_array(rand(  d_blocks_v, size(v)...; assignment=rand_procs_v))
        rand_M_exp    =        wait_array(rand(  d_blocks_m, size(M)...; assignment=rand_procs_M))

        randn_A_exp   =        wait_array(randn( d_blocks_a, size(A)...; assignment=rand_procs_A))
        randn_v_exp   =        wait_array(randn( d_blocks_v, size(v)...; assignment=rand_procs_v))
        randn_M_exp   =        wait_array(randn( d_blocks_m, size(M)...; assignment=rand_procs_M))

        sprand_v_exp  =        wait_array(sprand(d_blocks_v, size(v)..., 0.5; assignment=rand_procs_v))
        sprand_M_exp  =        wait_array(sprand(d_blocks_m, size(M)..., 0.5; assignment=rand_procs_M))

        ones_A_exp    =        wait_array(ones(  d_blocks_a, size(A)...; assignment=rand_procs_A))
        ones_v_exp    =        wait_array(ones(  d_blocks_v, size(v)...; assignment=rand_procs_v))
        ones_M_exp    =        wait_array(ones(  d_blocks_m, size(M)...; assignment=rand_procs_M))

        zeros_A_exp   =        wait_array(zeros( d_blocks_a, size(A)...; assignment=rand_procs_A))
        zeros_v_exp   =        wait_array(zeros( d_blocks_v, size(v)...; assignment=rand_procs_v))
        zeros_M_exp   =        wait_array(zeros( d_blocks_m, size(M)...; assignment=rand_procs_M))

        @test chunk_processors(rand_A_exp)   == chunk_processors(randn_A_exp) ==                                   chunk_processors(ones_A_exp) == chunk_processors(zeros_A_exp) == tile_processors(rand_procs_A, blocks_a)
        @test chunk_processors(rand_v_exp)   == chunk_processors(randn_v_exp) == chunk_processors(sprand_v_exp) == chunk_processors(ones_v_exp) == chunk_processors(zeros_v_exp) == tile_processors(rand_procs_v, blocks_v)
        @test chunk_processors(rand_M_exp)   == chunk_processors(randn_M_exp) == chunk_processors(sprand_M_exp) == chunk_processors(ones_M_exp) == chunk_processors(zeros_M_exp) == tile_processors(rand_procs_M, blocks_m)
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

@testset "Chunk view of DArray" begin
    A = rand(64, 64)
    DA = DArray(A, Blocks(8,8))
    chunk = DA.chunks[1,1]

    @testset "Valid Slices" begin
        @test view(chunk, :, :)     isa ChunkSlice && view(chunk, 1:8, 1:8)   isa ChunkSlice
        @test view(chunk, 1:2:7, :) isa ChunkSlice && view(chunk, :, 2:2:8)   isa ChunkSlice
        @test view(chunk, 1, :)     isa ChunkSlice && view(chunk, :, 1)       isa ChunkSlice
        @test view(chunk, 3:3, 5:5) isa ChunkSlice && view(chunk, 5:7, 1:2:4) isa ChunkSlice
        @test view(chunk, 8, 8)     isa ChunkSlice
        @test view(chunk, 1:0, :)   isa ChunkSlice
    end

    @testset "Dimension Mismatch" begin
        @test_throws DimensionMismatch view(chunk, :)
        @test_throws DimensionMismatch view(chunk, :, :, :)
    end

    @testset "Int Slice Out of Bounds" begin
        @test_throws ArgumentError view(chunk, 0, :)
        @test_throws ArgumentError view(chunk, :, 9)
        @test_throws ArgumentError view(chunk, 9, 1)
    end

    @testset "Range Slice Out of Bounds" begin
        @test_throws ArgumentError view(chunk, 0:5, :)
        @test_throws ArgumentError view(chunk, 1:8, 5:10)
        @test_throws ArgumentError view(chunk, 2:2:10, :)
    end

    @testset "Invalid Slice Types" begin
        @test_throws DimensionMismatch view(chunk, (1:2, :))
        @test_throws ArgumentError view(chunk, :, [1, 2])
    end

end 

@testset "copy/similar" begin
    X1 = ones(Blocks(10, 10), 100, 100)
    X2 = copy(X1)
    X3 = similar(X1)
    @test typeof(X1) === typeof(X2) === typeof(X3)
    @test collect(X1) == collect(X2)
    @test collect(X1) != collect(X3)
end