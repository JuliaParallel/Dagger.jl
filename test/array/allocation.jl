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

@testset "Constructors and Functions with assignment" begin
   
  availprocs = collect(Dagger.all_processors())
  sort!(availprocs, by = x -> (x.owner, x.tid))
  numprocs = length(availprocs)


  function chunk_processors(Ad::DArray)
      [Dagger.processor(Ad.chunks[idx].future.future.v.value[2]) for idx in CartesianIndices(size(Dagger.domainchunks(Ad)))]
  end

  function tile_processors(proc_grid::AbstractArray{<:Dagger.Processor,N}, block_grid::Tuple{Vararg{Int,N}}) where N
      reps       = Int.(ceil.(block_grid ./ size(proc_grid)))
      tiled      = repeat(proc_grid, reps...)
      idx_slices = [1:block_grid[d] for d in 1:length(block_grid)]
      return tiled[idx_slices...]
  end


  A = rand(41, 35, 12)
  v = rand(23)
  M = rand(76,118)

  t_blocks_a = (4,3,2)
  d_blocks_a = Dagger.Blocks(t_blocks_a)
  blocks_a   = cld.(size(A), t_blocks_a)

  n_blocks_v = 3
  t_blocks_v = (n_blocks_v,)
  d_blocks_v = Dagger.Blocks(t_blocks_v)
  blocks_v   = cld.(size(v), t_blocks_v)
  blocks_vv = [blocks_v...]
  blocks_nv  = blocks_v[1]

  t_blocks_m = (2,3)
  d_blocks_m = Dagger.Blocks(t_blocks_m)
  blocks_m   = cld.(size(M), t_blocks_m)

  function get_default_blockgrid(data, numprocs)
    ndims_data = ndims(data)
    size_data  = size(data)
    ntuple(i->i == ndims_data ? cld( size_data[ndims_data], cld(size_data[ndims_data], numprocs) ) : 1, ndims_data)
  end


  @testset "Arbitrary Assignment (:arbitrary)" begin
    assignment = :arbitrary

    @testset "Auto Blocks" begin

      @test distribute(A, assignment) isa DArray  && distribute(A, AutoBlocks(), assignment) isa DArray
      @test distribute(v, assignment) isa DVector && distribute(v, AutoBlocks(), assignment) isa DVector
      @test distribute(M, assignment) isa DMatrix && distribute(M, AutoBlocks(), assignment) isa DMatrix

      @test DArray( A,    assignment) isa DArray  && DArray(    A, AutoBlocks(), assignment) isa DArray
      @test DVector(v,    assignment) isa DVector && DVector(   v, AutoBlocks(), assignment) isa DVector
      @test DMatrix(M,    assignment) isa DMatrix && DMatrix(   M, AutoBlocks(), assignment) isa DMatrix

      @test rand(  AutoBlocks(), size(A)...     ; assignment=assignment) isa DArray  && rand(  AutoBlocks(), size(A);      assignment=assignment) isa DArray
      @test rand(  AutoBlocks(), size(v)...     ; assignment=assignment) isa DVector && rand(  AutoBlocks(), size(v);      assignment=assignment) isa DVector
      @test rand(  AutoBlocks(), size(M)...     ; assignment=assignment) isa DMatrix && rand(  AutoBlocks(), size(M);      assignment=assignment) isa DMatrix

      @test randn( AutoBlocks(), size(A)...     ; assignment=assignment) isa DArray  && randn( AutoBlocks(), size(A);      assignment=assignment) isa DArray
      @test randn( AutoBlocks(), size(v)...     ; assignment=assignment) isa DVector && randn( AutoBlocks(), size(v);      assignment=assignment) isa DVector
      @test randn( AutoBlocks(), size(M)...     ; assignment=assignment) isa DMatrix && randn( AutoBlocks(), size(M);      assignment=assignment) isa DMatrix

      @test Dagger.sprand(AutoBlocks(), size(A)..., 0.5; assignment=assignment) isa DArray  && Dagger.sprand(AutoBlocks(), size(A), 0.5; assignment=assignment) isa DArray
      @test Dagger.sprand(AutoBlocks(), size(v)..., 0.5; assignment=assignment) isa DVector && Dagger.sprand(AutoBlocks(), size(v), 0.5; assignment=assignment) isa DVector
      @test Dagger.sprand(AutoBlocks(), size(M)..., 0.5; assignment=assignment) isa DMatrix && Dagger.sprand(AutoBlocks(), size(M), 0.5; assignment=assignment) isa DMatrix

      @test ones(  AutoBlocks(), size(A)...     ; assignment=assignment) isa DArray  && ones(  AutoBlocks(), size(A);      assignment=assignment) isa DArray
      @test ones(  AutoBlocks(), size(v)...     ; assignment=assignment) isa DVector && ones(  AutoBlocks(), size(v);      assignment=assignment) isa DVector
      @test ones(  AutoBlocks(), size(M)...     ; assignment=assignment) isa DMatrix && ones(  AutoBlocks(), size(M);      assignment=assignment) isa DMatrix

      @test zeros( AutoBlocks(), size(A)...     ; assignment=assignment) isa DArray  && zeros( AutoBlocks(), size(A);      assignment=assignment) isa DArray
      @test zeros( AutoBlocks(), size(v)...     ; assignment=assignment) isa DVector && zeros( AutoBlocks(), size(v);      assignment=assignment) isa DVector
      @test zeros( AutoBlocks(), size(M)...     ; assignment=assignment) isa DMatrix && zeros( AutoBlocks(), size(M);      assignment=assignment) isa DMatrix

    end

    @testset "Explicit Blocks" begin

      @test distribute(A, d_blocks_a, assignment) isa DArray  && distribute(A, blocks_a, assignment) isa DArray
      @test distribute(v, d_blocks_v, assignment) isa DVector && distribute(v, blocks_v,  assignment) isa DVector
      @test distribute(v, n_blocks_v, assignment) isa DVector && distribute(v, blocks_vv, assignment) isa DVector
      @test distribute(M, d_blocks_m, assignment) isa DMatrix && distribute(M, blocks_m, assignment) isa DMatrix

      @test DArray( A, d_blocks_a, assignment) isa DArray
      @test DVector(v, d_blocks_v, assignment) isa DVector
      @test DMatrix(M, d_blocks_m, assignment) isa DMatrix

      @test rand(  d_blocks_a, size(A)...     ; assignment=assignment) isa DArray  && rand(  d_blocks_a, size(A);      assignment=assignment) isa DArray
      @test rand(  d_blocks_v, size(v)...     ; assignment=assignment) isa DVector && rand(  d_blocks_v, size(v);      assignment=assignment) isa DVector
      @test rand(  d_blocks_m, size(M)...     ; assignment=assignment) isa DMatrix && rand(  d_blocks_m, size(M);      assignment=assignment) isa DMatrix

      @test randn( d_blocks_a, size(A)...     ; assignment=assignment) isa DArray  && randn( d_blocks_a, size(A);      assignment=assignment) isa DArray
      @test randn( d_blocks_v, size(v)...     ; assignment=assignment) isa DVector && randn( d_blocks_v, size(v);      assignment=assignment) isa DVector
      @test randn( d_blocks_m, size(M)...     ; assignment=assignment) isa DMatrix && randn( d_blocks_m, size(M);      assignment=assignment) isa DMatrix

      @test Dagger.sprand(d_blocks_a, size(A)..., 0.5; assignment=assignment) isa DArray  && Dagger.sprand(d_blocks_a, size(A), 0.5; assignment=assignment) isa DArray
      @test Dagger.sprand(d_blocks_v, size(v)..., 0.5; assignment=assignment) isa DVector && Dagger.sprand(d_blocks_v, size(v), 0.5; assignment=assignment) isa DVector
      @test Dagger.sprand(d_blocks_m, size(M)..., 0.5; assignment=assignment) isa DMatrix && Dagger.sprand(d_blocks_m, size(M), 0.5; assignment=assignment) isa DMatrix

      @test ones(  d_blocks_a, size(A)...     ; assignment=assignment) isa DArray  && ones(  d_blocks_a, size(A);      assignment=assignment) isa DArray
      @test ones(  d_blocks_v, size(v)...     ; assignment=assignment) isa DVector && ones(  d_blocks_v, size(v);      assignment=assignment) isa DVector
      @test ones(  d_blocks_m, size(M)...     ; assignment=assignment) isa DMatrix && ones(  d_blocks_m, size(M);      assignment=assignment) isa DMatrix

      @test zeros( d_blocks_a, size(A)...     ; assignment=assignment) isa DArray  && zeros( d_blocks_a, size(A);      assignment=assignment) isa DArray
      @test zeros( d_blocks_v, size(v)...     ; assignment=assignment) isa DVector && zeros( d_blocks_v, size(v);      assignment=assignment) isa DVector
      @test zeros( d_blocks_m, size(M)...     ; assignment=assignment) isa DMatrix && zeros( d_blocks_m, size(M);      assignment=assignment) isa DMatrix

    end

  end


  @testset "Structured Assignment (:blockrow, :blockcol, :cyclicrow, :cycliccol)" begin

    function get_default_blockgrid(data, numprocs)
      ndims_data = ndims(data)
      size_data  = size(data)
      ntuple(i->i == ndims_data ? cld( size_data[ndims_data], cld(size_data[ndims_data], numprocs) ) : 1, ndims_data)
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

          dist_A_def_auto = distribute(A,               assignment); fetch(dist_A_def_auto)
          dist_A_auto_def = distribute(A, AutoBlocks(), assignment); fetch(dist_A_auto_def)
          dist_v_def_auto = distribute(v,               assignment); fetch(dist_v_def_auto)
          dist_v_auto_def = distribute(v, AutoBlocks(), assignment); fetch(dist_v_auto_def)
          dist_M_def_auto = distribute(M,               assignment); fetch(dist_M_def_auto)
          dist_M_auto_def = distribute(M, AutoBlocks(), assignment); fetch(dist_M_auto_def)

          darr_A_def_auto = DArray(    A,               assignment); fetch(darr_A_def_auto)
          darr_A_auto_def = DArray(    A, AutoBlocks(), assignment); fetch(darr_A_auto_def)
          dvec_v_def_auto = DVector(   v,               assignment); fetch(dvec_v_def_auto)
          dvec_v_auto_def = DVector(   v, AutoBlocks(), assignment); fetch(dvec_v_auto_def)
          dmat_M_def_auto = DMatrix(   M,               assignment); fetch(dmat_M_def_auto)
          dmat_M_auto_def = DMatrix(   M, AutoBlocks(), assignment); fetch(dmat_M_auto_def)

          @test chunk_processors(dist_A_def_auto) == chunk_processors(dist_A_auto_def) == chunk_processors(darr_A_def_auto) == chunk_processors(darr_A_auto_def) == tile_processors(get_assignment_procgrid(A, numprocs, get_default_blockgrid(A, numprocs)), get_default_blockgrid(A, numprocs))
          @test chunk_processors(dist_v_def_auto) == chunk_processors(dist_v_auto_def) == chunk_processors(dvec_v_def_auto) == chunk_processors(dvec_v_auto_def) == tile_processors(get_assignment_procgrid(v, numprocs, get_default_blockgrid(v, numprocs)), get_default_blockgrid(v, numprocs))
          @test chunk_processors(dist_M_def_auto) == chunk_processors(dist_M_auto_def) == chunk_processors(dmat_M_def_auto) == chunk_processors(dmat_M_auto_def) == tile_processors(get_assignment_procgrid(M, numprocs, get_default_blockgrid(M, numprocs)), get_default_blockgrid(M, numprocs))
          
        end

        @testset "Functions with AutoBlocks" begin

          rand_A_auto   =        rand(  AutoBlocks(), size(A)...;      assignment=assignment); fetch(rand_A_auto)
          rand_v_auto   =        rand(  AutoBlocks(), size(v)...;      assignment=assignment); fetch(rand_v_auto)
          rand_M_auto   =        rand(  AutoBlocks(), size(M)...;      assignment=assignment); fetch(rand_M_auto)

          randn_A_auto  =        randn( AutoBlocks(), size(A)...;      assignment=assignment); fetch(randn_A_auto)
          randn_v_auto  =        randn( AutoBlocks(), size(v)...;      assignment=assignment); fetch(randn_v_auto)
          randn_M_auto  =        randn( AutoBlocks(), size(M)...;      assignment=assignment); fetch(randn_M_auto)

          # sprand_A_auto = Dagger.sprand(AutoBlocks(), size(A)..., 0.5; assignment=assignment); fetch(sprand_A_auto)
          sprand_v_auto = Dagger.sprand(AutoBlocks(), size(v)..., 0.5; assignment=assignment); fetch(sprand_v_auto)
          sprand_M_auto = Dagger.sprand(AutoBlocks(), size(M)..., 0.5; assignment=assignment); fetch(sprand_M_auto)

          ones_A_auto   =        ones(  AutoBlocks(), size(A)...;      assignment=assignment); fetch(ones_A_auto)
          ones_v_auto   =        ones(  AutoBlocks(), size(v)...;      assignment=assignment); fetch(ones_v_auto)
          ones_M_auto   =        ones(  AutoBlocks(), size(M)...;      assignment=assignment); fetch(ones_M_auto)

          zeros_A_auto  =        zeros( AutoBlocks(), size(A)...;      assignment=assignment); fetch(zeros_A_auto)
          zeros_v_auto  =        zeros( AutoBlocks(), size(v)...;      assignment=assignment); fetch(zeros_v_auto)
          zeros_M_auto  =        zeros( AutoBlocks(), size(M)...;      assignment=assignment); fetch(zeros_M_auto)

          @test chunk_processors(rand_A_auto)   == chunk_processors(randn_A_auto) ==                                    chunk_processors(ones_A_auto) == chunk_processors(zeros_A_auto) ==  tile_processors(get_assignment_procgrid(A, numprocs, get_default_blockgrid(A, numprocs)), get_default_blockgrid(A, numprocs))
          # @test chunk_processors(sprand_A_auto) ==  tile_processors(get_assignment_procgrid(A, numprocs, get_default_blockgrid(A, numprocs)), get_default_blockgrid(A, numprocs))
          @test chunk_processors(rand_v_auto)   == chunk_processors(randn_v_auto) == chunk_processors(sprand_v_auto) == chunk_processors(ones_v_auto) == chunk_processors(zeros_v_auto) ==  tile_processors(get_assignment_procgrid(v, numprocs, get_default_blockgrid(v, numprocs)), get_default_blockgrid(v, numprocs))
          @test chunk_processors(rand_M_auto)   == chunk_processors(randn_M_auto) == chunk_processors(sprand_M_auto) == chunk_processors(ones_M_auto) == chunk_processors(zeros_M_auto) ==  tile_processors(get_assignment_procgrid(M, numprocs, get_default_blockgrid(M, numprocs)), get_default_blockgrid(M, numprocs))

        end

        @testset "Explicit Blocks" begin

          dist_A_exp_def     = distribute(A, d_blocks_a, assignment); fetch(dist_A_exp_def)
          dist_A_blocks_exp  = distribute(A, blocks_a,   assignment); fetch(dist_A_blocks_exp)
          dist_v_exp_def     = distribute(v, d_blocks_v, assignment); fetch(dist_v_exp_def)
          dist_v_blocks_exp  = distribute(v, blocks_v,   assignment); fetch(dist_v_blocks_exp)
          dist_v_nblocks_exp = distribute(v, blocks_nv,  assignment); fetch(dist_v_nblocks_exp)
          dist_v_vblocks_exp = distribute(v, blocks_vv,  assignment); fetch(dist_v_vblocks_exp)
          dist_M_exp_def     = distribute(M, d_blocks_m, assignment); fetch(dist_M_exp_def)
          dist_M_blocks_exp  = distribute(M, blocks_m,   assignment); fetch(dist_M_blocks_exp)

          darr_A_exp_def     = DArray(    A, d_blocks_a, assignment); fetch(darr_A_exp_def)
          dvec_v_exp_def     = DVector(   v, d_blocks_v, assignment); fetch(dvec_v_exp_def)
          dmat_M_exp_def     = DMatrix(   M, d_blocks_m, assignment); fetch(dmat_M_exp_def)


          @test chunk_processors(dist_A_exp_def)      == chunk_processors(dist_A_blocks_exp) == chunk_processors(darr_A_exp_def) == tile_processors(get_assignment_procgrid(A, numprocs, blocks_a), blocks_a)
          @test chunk_processors(dist_v_exp_def)      == chunk_processors(dist_v_blocks_exp) == chunk_processors(dvec_v_exp_def) == tile_processors(get_assignment_procgrid(v, numprocs, blocks_v), blocks_v)
          @test chunk_processors(dist_v_nblocks_exp)  == chunk_processors(dist_v_vblocks_exp)                                    == tile_processors(get_assignment_procgrid(v, numprocs, blocks_v), blocks_v)
          @test chunk_processors(dist_M_exp_def)      == chunk_processors(dist_M_blocks_exp) == chunk_processors(dmat_M_exp_def) == tile_processors(get_assignment_procgrid(M, numprocs, blocks_m), blocks_m)
          
        end

        @testset "Functions with Explicit Blocks" begin
          
          rand_A_exp   =        rand(  d_blocks_a, size(A)...;   assignment=assignment); fetch(rand_A_exp)
          rand_v_exp   =        rand(  d_blocks_v, size(v)...;   assignment=assignment); fetch(rand_v_exp)
          rand_M_exp   =        rand(  d_blocks_m, size(M)...;   assignment=assignment); fetch(rand_M_exp)

          rand_A_exp    =        rand(  d_blocks_a, size(A)...; assignment=assignment); fetch(rand_A_exp)
          rand_v_exp    =        rand(  d_blocks_v, size(v)...; assignment=assignment); fetch(rand_v_exp)
          rand_M_exp    =        rand(  d_blocks_m, size(M)...; assignment=assignment); fetch(rand_M_exp)

          randn_A_exp   =        randn( d_blocks_a, size(A)...; assignment=assignment); fetch(randn_A_exp)
          randn_v_exp   =        randn( d_blocks_v, size(v)...; assignment=assignment); fetch(randn_v_exp)
          randn_M_exp   =        randn( d_blocks_m, size(M)...; assignment=assignment); fetch(randn_M_exp)

          # sprand_A_exp  = Dagger.sprand(d_blocks_a, size(A)..., 0.5; assignment=assignment); fetch(sprand_A_exp)
          sprand_v_exp  = Dagger.sprand(d_blocks_v, size(v)..., 0.5; assignment=assignment); fetch(sprand_v_exp)
          sprand_M_exp  = Dagger.sprand(d_blocks_m, size(M)..., 0.5; assignment=assignment); fetch(sprand_M_exp)

          ones_A_exp    =        ones(  d_blocks_a, size(A)...; assignment=assignment); fetch(ones_A_exp)
          ones_v_exp    =        ones(  d_blocks_v, size(v)...; assignment=assignment); fetch(ones_v_exp)
          ones_M_exp    =        ones(  d_blocks_m, size(M)...; assignment=assignment); fetch(ones_M_exp)

          zeros_A_exp   =        zeros( d_blocks_a, size(A)...; assignment=assignment); fetch(zeros_A_exp)
          zeros_v_exp   =        zeros( d_blocks_v, size(v)...; assignment=assignment); fetch(zeros_v_exp)
          zeros_M_exp   =        zeros( d_blocks_m, size(M)...; assignment=assignment); fetch(zeros_M_exp)

          # @test chunk_processors(rand_A_exp) == chunk_processors(randn_A_exp) ==                                   chunk_processors(ones_A_exp) == chunk_processors(zeros_A_exp) == tile_processors(get_assignment_procgrid(A, numprocs, get_default_blockgrid(A, numprocs)), get_default_blockgrid(A, numprocs))
          # @test chunk_processors(sprand_A_exp) == tile_processors(get_assignment_procgrid(A, numprocs, get_default_blockgrid(A, numprocs)), get_default_blockgrid(A, numprocs))
          # @test chunk_processors(rand_v_exp) == chunk_processors(randn_v_exp) == chunk_processors(sprand_v_exp) == chunk_processors(ones_v_exp) == chunk_processors(zeros_v_exp) == tile_processors(get_assignment_procgrid(v, numprocs, get_default_blockgrid(v, numprocs)), get_default_blockgrid(v, numprocs))
          # @test chunk_processors(rand_M_exp) == chunk_processors(randn_M_exp) == chunk_processors(sprand_M_exp) == chunk_processors(ones_M_exp) == chunk_processors(zeros_M_exp) == tile_processors(get_assignment_procgrid(M, numprocs, get_default_blockgrid(M, numprocs)), get_default_blockgrid(M, numprocs))

        end
            
      end
    
    end

  test_assignment_strategy(:blockrow,  get_blockrow_procgrid)
  test_assignment_strategy(:blockcol,  get_blockcol_procgrid)
  test_assignment_strategy(:cyclicrow, get_cyclicrow_procgrid)
  test_assignment_strategy(:cycliccol, get_cycliccol_procgrid)

  end

  @testset "OSProc ID Array Assignment (AbstractArray{<:Int, N})" begin

    function get_random_threadprocs(proc_ids)
      [Dagger.ThreadProc(proc, 1) for proc in proc_ids]
    end

    rand_osproc_ids_A = rand(Dagger.procs(), 3, 2, 2)
    rand_osproc_ids_v = rand(Dagger.procs(), 11)
    rand_osproc_ids_M = rand(Dagger.procs(), 2, 5)

    @testset "Auto Blocks" begin

      dist_A_rand_osproc_auto = distribute(A,               rand_osproc_ids_A); fetch(dist_A_rand_osproc_auto)
      dist_A_auto_rand_osproc = distribute(A, AutoBlocks(), rand_osproc_ids_A); fetch(dist_A_auto_rand_osproc)
      # dist_v_rand_osproc_auto = distribute(v,               rand_osproc_ids_v); fetch(dist_v_rand_osproc_auto)
      dist_v_auto_rand_osproc = distribute(v, AutoBlocks(), rand_osproc_ids_v); fetch(dist_v_auto_rand_osproc)
      dist_M_rand_osproc_auto = distribute(M,               rand_osproc_ids_M); fetch(dist_M_rand_osproc_auto)
      dist_M_auto_rand_osproc = distribute(M, AutoBlocks(), rand_osproc_ids_M); fetch(dist_M_auto_rand_osproc)

      darr_A_rand_osproc_auto = DArray(    A,               rand_osproc_ids_A); fetch(darr_A_rand_osproc_auto)
      darr_A_auto_rand_osproc = DArray(    A, AutoBlocks(), rand_osproc_ids_A); fetch(darr_A_auto_rand_osproc)
      dvec_v_rand_osproc_auto = DVector(   v,               rand_osproc_ids_v); fetch(dvec_v_rand_osproc_auto)
      dvec_v_auto_rand_osproc = DVector(   v, AutoBlocks(), rand_osproc_ids_v); fetch(dvec_v_auto_rand_osproc)
      # dmat_M_rand_osproc_auto = DMatrix(   M,               rand_osproc_ids_M); fetch(dmat_M_rand_osproc_auto) ### rand_osproc_ids_M assigned as Blocks
      dmat_M_auto_rand_osproc = DMatrix(   M, AutoBlocks(), rand_osproc_ids_M); fetch(dmat_M_auto_rand_osproc)

      @test chunk_processors(dist_A_rand_osproc_auto) == chunk_processors(dist_A_auto_rand_osproc) == chunk_processors(darr_A_rand_osproc_auto) == chunk_processors(darr_A_auto_rand_osproc) == tile_processors(get_random_threadprocs(rand_osproc_ids_A), get_default_blockgrid(A, numprocs))
      @test                                              chunk_processors(dist_v_auto_rand_osproc) == chunk_processors(dvec_v_rand_osproc_auto) == chunk_processors(dvec_v_auto_rand_osproc) == tile_processors(get_random_threadprocs(rand_osproc_ids_v), get_default_blockgrid(v, numprocs))
      # @test chunk_processors(dist_v_rand_osproc_auto) == tile_processors(get_random_threadprocs(rand_osproc_ids_v), get_default_blockgrid(v, numprocs)) 
      @test chunk_processors(dist_M_rand_osproc_auto) == chunk_processors(dist_M_auto_rand_osproc)  == chunk_processors(dmat_M_auto_rand_osproc) == tile_processors(get_random_threadprocs(rand_osproc_ids_M), get_default_blockgrid(M, numprocs))
      # @test chunk_processors(dmat_M_rand_osproc_auto) == tile_processors(get_random_threadprocs(rand_osproc_ids_M), get_default_blockgrid(M, numprocs))
    end

    @testset "Functions with AutoBlocks" begin

      rand_A_auto    =        rand(  AutoBlocks(), size(A)...; assignment=rand_osproc_ids_A); fetch(rand_A_auto)
      rand_v_auto    =        rand(  AutoBlocks(), size(v)...; assignment=rand_osproc_ids_v); fetch(rand_v_auto)
      rand_M_auto    =        rand(  AutoBlocks(), size(M)...; assignment=rand_osproc_ids_M); fetch(rand_M_auto)

      randn_A_auto   =        randn( AutoBlocks(), size(A)...; assignment=rand_osproc_ids_A); fetch(randn_A_auto)
      randn_v_auto   =        randn( AutoBlocks(), size(v)...; assignment=rand_osproc_ids_v); fetch(randn_v_auto)
      randn_M_auto   =        randn( AutoBlocks(), size(M)...; assignment=rand_osproc_ids_M); fetch(randn_M_auto)

      # sprand_A_auto  = Dagger.sprand(AutoBlocks(), size(A)..., 0.5; assignment=rand_osproc_ids_A); fetch(sprand_A_auto)
      sprand_v_auto  = Dagger.sprand(AutoBlocks(), size(v)..., 0.5; assignment=rand_osproc_ids_v); fetch(sprand_v_auto)
      sprand_M_auto  = Dagger.sprand(AutoBlocks(), size(M)..., 0.5; assignment=rand_osproc_ids_M); fetch(sprand_M_auto)

      ones_A_auto    =        ones(  AutoBlocks(), size(A)...; assignment=rand_osproc_ids_A); fetch(ones_A_auto)
      ones_v_auto    =        ones(  AutoBlocks(), size(v)...; assignment=rand_osproc_ids_v); fetch(ones_v_auto)
      ones_M_auto    =        ones(  AutoBlocks(), size(M)...; assignment=rand_osproc_ids_M); fetch(ones_M_auto)

      zeros_A_auto   =        zeros( AutoBlocks(), size(A)...; assignment=rand_osproc_ids_A); fetch(zeros_A_auto)
      zeros_v_auto   =        zeros( AutoBlocks(), size(v)...; assignment=rand_osproc_ids_v); fetch(zeros_v_auto)
      zeros_M_auto   =        zeros( AutoBlocks(), size(M)...; assignment=rand_osproc_ids_M); fetch(zeros_M_auto)

 
      @test chunk_processors(rand_A_auto)   == chunk_processors(randn_A_auto) ==                                    chunk_processors(ones_A_auto) == chunk_processors(zeros_A_auto) == tile_processors(get_random_threadprocs(rand_osproc_ids_A), get_default_blockgrid(rand_A_auto, numprocs))
    # @test chunk_processors(sprand_A_auto) == tile_processors(get_assignment_procgrid(A, numprocs, get_default_blockgrid(A, numprocs)), get_default_blockgrid(A, numprocs))
      @test chunk_processors(rand_v_auto)   == chunk_processors(randn_v_auto) == chunk_processors(sprand_v_auto) == chunk_processors(ones_v_auto) == chunk_processors(zeros_v_auto) == tile_processors(get_random_threadprocs(rand_osproc_ids_v), get_default_blockgrid(rand_v_auto, numprocs))
      @test chunk_processors(rand_M_auto)   == chunk_processors(randn_M_auto) == chunk_processors(sprand_M_auto) ==  chunk_processors(ones_M_auto) == chunk_processors(zeros_M_auto) == tile_processors(get_random_threadprocs(rand_osproc_ids_M), get_default_blockgrid(rand_M_auto, numprocs))

    end

    @testset "Explicit Blocks" begin

      dist_A_exp_rand_osproc     = distribute(A, d_blocks_a, rand_osproc_ids_A); fetch(dist_A_exp_rand_osproc)
      dist_A_blocks_rand_osproc  = distribute(A, blocks_a,   rand_osproc_ids_A); fetch(dist_A_blocks_rand_osproc)
      dist_v_exp_rand_osproc     = distribute(v, d_blocks_v, rand_osproc_ids_v); fetch(dist_v_exp_rand_osproc)
      dist_v_blocks_rand_osproc  = distribute(v, blocks_v,   rand_osproc_ids_v); fetch(dist_v_blocks_rand_osproc)
      dist_v_nblocks_rand_osproc = distribute(v, blocks_nv,  rand_osproc_ids_v); fetch(dist_v_nblocks_rand_osproc)
      dist_v_vblocks_rand_osproc = distribute(v, blocks_vv, rand_osproc_ids_v); fetch(dist_v_vblocks_rand_osproc)
      dist_M_exp_rand_osproc     = distribute(M, d_blocks_m, rand_osproc_ids_M); fetch(dist_M_exp_rand_osproc)
      dist_M_blocks_rand_osproc  = distribute(M, blocks_m,   rand_osproc_ids_M); fetch(dist_M_blocks_rand_osproc)

      darr_A_exp_rand_osproc     = DArray(    A, d_blocks_a, rand_osproc_ids_A); fetch(darr_A_exp_rand_osproc)
      dvec_v_exp_rand_osproc     = DVector(   v, d_blocks_v, rand_osproc_ids_v); fetch(dvec_v_exp_rand_osproc)
      dmat_M_exp_rand_osproc     = DMatrix(   M, d_blocks_m, rand_osproc_ids_M); fetch(dmat_M_exp_rand_osproc)

      @test chunk_processors(dist_A_exp_rand_osproc) == chunk_processors(dist_A_blocks_rand_osproc) == chunk_processors(darr_A_exp_rand_osproc) == tile_processors(get_random_threadprocs(rand_osproc_ids_A), blocks_a)
      @test chunk_processors(dist_v_exp_rand_osproc) == chunk_processors(dist_v_blocks_rand_osproc) == chunk_processors(dvec_v_exp_rand_osproc) == tile_processors(get_random_threadprocs(rand_osproc_ids_v), blocks_v)
      @test chunk_processors(dist_v_nblocks_rand_osproc) == chunk_processors(dist_v_vblocks_rand_osproc)                                        == tile_processors(get_random_threadprocs(rand_osproc_ids_v), blocks_v)
      @test chunk_processors(dist_M_exp_rand_osproc) == chunk_processors(dist_M_blocks_rand_osproc) == chunk_processors(dmat_M_exp_rand_osproc) == tile_processors(get_random_threadprocs(rand_osproc_ids_M), blocks_m)

    end

    @testset "Functions with Explicit Blocks" begin

      rand_A_exp    =        rand(  d_blocks_a, size(A)...; assignment=rand_osproc_ids_A); fetch(rand_A_exp)
      rand_v_exp    =        rand(  d_blocks_v, size(v)...; assignment=rand_osproc_ids_v); fetch(rand_v_exp)
      rand_M_exp    =        rand(  d_blocks_m, size(M)...; assignment=rand_osproc_ids_M); fetch(rand_M_exp)

      randn_A_exp   =        randn( d_blocks_a, size(A)...; assignment=rand_osproc_ids_A); fetch(randn_A_exp)
      randn_v_exp   =        randn( d_blocks_v, size(v)...; assignment=rand_osproc_ids_v); fetch(randn_v_exp)
      randn_M_exp   =        randn( d_blocks_m, size(M)...; assignment=rand_osproc_ids_M); fetch(randn_M_exp)

      # sprand_A_exp  = Dagger.sprand(d_blocks_a, size(A)..., 0.5; assignment=rand_osproc_ids_A); fetch(sprand_A_exp)
      sprand_v_exp  = Dagger.sprand(d_blocks_v, size(v)..., 0.5; assignment=rand_osproc_ids_v); fetch(sprand_v_exp)
      sprand_M_exp  = Dagger.sprand(d_blocks_m, size(M)..., 0.5; assignment=rand_osproc_ids_M); fetch(sprand_M_exp)

      ones_A_exp    =        ones(  d_blocks_a, size(A)...; assignment=rand_osproc_ids_A); fetch(ones_A_exp)
      ones_v_exp    =        ones(  d_blocks_v, size(v)...; assignment=rand_osproc_ids_v); fetch(ones_v_exp)
      ones_M_exp    =        ones(  d_blocks_m, size(M)...; assignment=rand_osproc_ids_M); fetch(ones_M_exp)

      zeros_A_exp   =        zeros( d_blocks_a, size(A)...; assignment=rand_osproc_ids_A); fetch(zeros_A_exp)
      zeros_v_exp   =        zeros( d_blocks_v, size(v)...; assignment=rand_osproc_ids_v); fetch(zeros_v_exp)
      zeros_M_exp   =        zeros( d_blocks_m, size(M)...; assignment=rand_osproc_ids_M); fetch(zeros_M_exp)

 
      @test chunk_processors(rand_A_exp)   == chunk_processors(randn_A_exp) ==                                    chunk_processors(ones_A_exp) == chunk_processors(zeros_A_exp) == tile_processors(get_random_threadprocs(rand_osproc_ids_A), blocks_a)
    # @test chunk_processors(sprand_A_exp) == tile_processors(get_assignment_procgrid(A, numprocs, get_default_blockgrid(A, numprocs)), blocks_a)
      @test chunk_processors(rand_v_exp)   == chunk_processors(randn_v_exp) == chunk_processors(sprand_v_exp) == chunk_processors(ones_v_exp) == chunk_processors(zeros_v_exp) == tile_processors(get_random_threadprocs(rand_osproc_ids_v), blocks_v)
      @test chunk_processors(rand_M_exp)   == chunk_processors(randn_M_exp) == chunk_processors(sprand_M_exp) ==  chunk_processors(ones_M_exp) == chunk_processors(zeros_M_exp) == tile_processors(get_random_threadprocs(rand_osproc_ids_M), blocks_m)

    end

  end


  @testset "Explicit Processor Array Assignment (AbstractArray{<:Processor, N})" begin

    rand_procs_A = reshape(availprocs[ rand(Dagger.procs(),  6) ], 2, 3, 1)
    rand_procs_v = reshape(availprocs[ rand(Dagger.procs(),  5) ], 5)
    rand_procs_M = reshape(availprocs[ rand(Dagger.procs(), 14) ], 2, 7)


    @testset "Auto Blocks" begin

      dist_A_rand_procs_auto = distribute(A,               rand_procs_A); fetch(dist_A_rand_procs_auto)
      dist_A_auto_rand_procs = distribute(A, AutoBlocks(), rand_procs_A); fetch(dist_A_auto_rand_procs)
      dist_v_rand_procs_auto = distribute(v,               rand_procs_v); fetch(dist_v_rand_procs_auto)
      dist_v_auto_rand_procs = distribute(v, AutoBlocks(), rand_procs_v); fetch(dist_v_auto_rand_procs)
      dist_M_rand_procs_auto = distribute(M,               rand_procs_M); fetch(dist_M_rand_procs_auto)
      dist_M_auto_rand_procs = distribute(M, AutoBlocks(), rand_procs_M); fetch(dist_M_auto_rand_procs)

      darr_A_rand_procs_auto = DArray(    A,               rand_procs_A); fetch(darr_A_rand_procs_auto)
      darr_A_auto_rand_procs = DArray(    A, AutoBlocks(), rand_procs_A); fetch(darr_A_auto_rand_procs)
      dvec_v_rand_procs_auto = DVector(   v,               rand_procs_v); fetch(dvec_v_rand_procs_auto)
      dvec_v_auto_rand_procs = DVector(   v, AutoBlocks(), rand_procs_v); fetch(dvec_v_auto_rand_procs)
      dmat_M_rand_procs_auto = DMatrix(   M,               rand_procs_M); fetch(dmat_M_rand_procs_auto)
      dmat_M_auto_rand_procs = DMatrix(   M, AutoBlocks(), rand_procs_M); fetch(dmat_M_auto_rand_procs)

      @test chunk_processors(dist_A_rand_procs_auto) == chunk_processors(dist_A_auto_rand_procs) == chunk_processors(darr_A_rand_procs_auto) == chunk_processors(darr_A_auto_rand_procs) == tile_processors(rand_procs_A, get_default_blockgrid(A, numprocs))
      @test chunk_processors(dist_v_rand_procs_auto) == chunk_processors(dist_v_auto_rand_procs) == chunk_processors(dvec_v_rand_procs_auto) == chunk_processors(dvec_v_auto_rand_procs) == tile_processors(rand_procs_v, get_default_blockgrid(v, numprocs))
      @test chunk_processors(dist_M_rand_procs_auto) == chunk_processors(dist_M_auto_rand_procs) == chunk_processors(dmat_M_rand_procs_auto) == chunk_processors(dmat_M_auto_rand_procs) == tile_processors(rand_procs_M, get_default_blockgrid(M, numprocs))

    end

    @testset "Functions with AutoBlocks" begin

      # rand_A_auto    =        rand(  AutoBlocks(), size(A)...; assignment=rand_procs_A); fetch(rand_A_auto)
      # rand_v_auto    =        rand(  AutoBlocks(), size(v)...; assignment=rand_procs_v); fetch(rand_v_auto)
      # rand_M_auto    =        rand(  AutoBlocks(), size(M)...; assignment=rand_procs_M); fetch(rand_M_auto)

      # randn_A_auto   =        randn( AutoBlocks(), size(A)...; assignment=rand_procs_A); fetch(randn_A_auto)
      # randn_v_auto   =        randn( AutoBlocks(), size(v)...; assignment=rand_procs_v); fetch(randn_v_auto)
      # randn_M_auto   =        randn( AutoBlocks(), size(M)...; assignment=rand_procs_M); fetch(randn_M_auto)

      # # sprand_A_auto  = Dagger.sprand(AutoBlocks(), size(A)..., 0.5; assignment=rand_procs_A); fetch(sprand_A_auto)
      # sprand_v_auto  = Dagger.sprand(AutoBlocks(), size(v)..., 0.5; assignment=rand_procs_v); fetch(sprand_v_auto)
      # sprand_M_auto  = Dagger.sprand(AutoBlocks(), size(M)..., 0.5; assignment=rand_procs_M); fetch(sprand_M_auto)

      # ones_A_auto    =        ones(  AutoBlocks(), size(A)...; assignment=rand_procs_A); fetch(ones_A_auto)
      # ones_v_auto    =        ones(  AutoBlocks(), size(v)...; assignment=rand_procs_v); fetch(ones_v_auto)
      # ones_M_auto    =        ones(  AutoBlocks(), size(M)...; assignment=rand_procs_M); fetch(ones_M_auto)

      # zeros_A_auto   =        zeros( AutoBlocks(), size(A)...; assignment=rand_procs_A); fetch(zeros_A_auto)
      # zeros_v_auto   =        zeros( AutoBlocks(), size(v)...; assignment=rand_procs_v); fetch(zeros_v_auto)
      # zeros_M_auto   =        zeros( AutoBlocks(), size(M)...; assignment=rand_procs_M); fetch(zeros_M_auto)

 
      @test chunk_processors(rand_A_auto)   == chunk_processors(randn_A_auto) ==                                    chunk_processors(ones_A_auto) == chunk_processors(zeros_A_auto) == tile_processors(rand_procs_A, get_default_blockgrid(A, numprocs))
    # @test chunk_processors(sprand_A_auto) == tile_processors(get_assignment_procgrid(A, numprocs, get_default_blockgrid(A, numprocs)), get_default_blockgrid(A, numprocs))
      # @test chunk_processors(rand_v_auto)   == chunk_processors(randn_v_auto) == chunk_processors(sprand_v_auto) == chunk_processors(ones_v_auto) == chunk_processors(zeros_v_auto) == tile_processors(rand_procs_v, get_default_blockgrid(v, numprocs))
      # @test chunk_processors(rand_M_auto)   == chunk_processors(randn_M_auto) == chunk_processors(sprand_M_auto) ==  chunk_processors(ones_M_auto) == chunk_processors(zeros_M_auto) == tile_processors(rand_procs_M, get_default_blockgrid(M, numprocs))

    end

    @testset "Explicit Blocks" begin

      dist_A_exp_rand_procs     = distribute(A, d_blocks_a, rand_procs_A); fetch(dist_A_exp_rand_procs)
      dist_A_blocks_rand_procs  = distribute(A, blocks_a,   rand_procs_A); fetch(dist_A_blocks_rand_procs)
      dist_v_exp_rand_procs     = distribute(v, d_blocks_v, rand_procs_v); fetch(dist_v_exp_rand_procs)
      dist_v_blocks_rand_procs  = distribute(v, blocks_v,   rand_procs_v); fetch(dist_v_blocks_rand_procs)
      dist_v_nblocks_rand_procs = distribute(v, blocks_nv, rand_procs_v); fetch(dist_v_nblocks_rand_procs)
      dist_v_vblocks_rand_procs = distribute(v, blocks_vv, rand_procs_v); fetch(dist_v_vblocks_rand_procs)
      dist_M_exp_rand_procs     = distribute(M, d_blocks_m, rand_procs_M); fetch(dist_M_exp_rand_procs)
      dist_M_blocks_rand_procs  = distribute(M, blocks_m,   rand_procs_M); fetch(dist_M_blocks_rand_procs)

      darr_A_exp_rand_procs     = DArray(    A, d_blocks_a, rand_procs_A); fetch(darr_A_exp_rand_procs)
      dvec_v_exp_rand_procs     = DVector(   v, d_blocks_v, rand_procs_v); fetch(dvec_v_exp_rand_procs)
      dmat_M_exp_rand_procs     = DMatrix(   M, d_blocks_m, rand_procs_M); fetch(dmat_M_exp_rand_procs)

      @test chunk_processors(dist_A_exp_rand_procs)     == chunk_processors(dist_A_blocks_rand_procs)  == chunk_processors(darr_A_exp_rand_procs) == tile_processors(rand_procs_A, blocks_a)
      @test chunk_processors(dist_v_exp_rand_procs)     == chunk_processors(dist_v_blocks_rand_procs)  == chunk_processors(dvec_v_exp_rand_procs) == tile_processors(rand_procs_v, blocks_v)
      @test chunk_processors(dist_v_nblocks_rand_procs) == chunk_processors(dist_v_vblocks_rand_procs)                                            == tile_processors(rand_procs_v, blocks_v)
      @test chunk_processors(dist_M_exp_rand_procs)     == chunk_processors(dist_M_blocks_rand_procs)  == chunk_processors(dmat_M_exp_rand_procs) == tile_processors(rand_procs_M, blocks_m)

    end

    @testset "Functions with Explicit Blocks" begin

      # rand_A_exp    =        rand(  d_blocks_a, size(A)...; assignment=rand_procs_A); fetch(rand_A_exp)
      # rand_v_exp    =        rand(  d_blocks_v, size(v)...; assignment=rand_procs_v); fetch(rand_v_exp)
      # rand_M_exp    =        rand(  d_blocks_m, size(M)...; assignment=rand_procs_M); fetch(rand_M_exp)

      # randn_A_exp   =        randn( d_blocks_a, size(A)...; assignment=rand_procs_A); fetch(randn_A_exp)
      # randn_v_exp   =        randn( d_blocks_v, size(v)...; assignment=rand_procs_v); fetch(randn_v_exp)
      # randn_M_exp   =        randn( d_blocks_m, size(M)...; assignment=rand_procs_M); fetch(randn_M_exp)

      # # sprand_A_exp  = Dagger.sprand(d_blocks_a, size(A)..., 0.5; assignment=rand_procs_A); fetch(sprand_A_exp)
      # sprand_v_exp  = Dagger.sprand(d_blocks_v, size(v)..., 0.5; assignment=rand_procs_v); fetch(sprand_v_exp)
      # sprand_M_exp  = Dagger.sprand(d_blocks_m, size(M)..., 0.5; assignment=rand_procs_M); fetch(sprand_M_exp)

      # ones_A_exp    =        ones(  d_blocks_a, size(A)...; assignment=rand_procs_A); fetch(ones_A_exp)
      # ones_v_exp    =        ones(  d_blocks_v, size(v)...; assignment=rand_procs_v); fetch(ones_v_exp)
      # ones_M_exp    =        ones(  d_blocks_m, size(M)...; assignment=rand_procs_M); fetch(ones_M_exp)

      # zeros_A_exp   =        zeros( d_blocks_a, size(A)...; assignment=rand_procs_A); fetch(zeros_A_exp)
      # zeros_v_exp   =        zeros( d_blocks_v, size(v)...; assignment=rand_procs_v); fetch(zeros_v_exp)
      # zeros_M_exp   =        zeros( d_blocks_m, size(M)...; assignment=rand_procs_M); fetch(zeros_M_exp)
 
      # @test chunk_processors(rand_A_exp)   == chunk_processors(randn_A_exp) ==                                    chunk_processors(ones_A_exp) == chunk_processors(zeros_A_exp) == tile_processors(rand_procs_A, blocks_a)
    # @test chunk_processors(sprand_A_exp) == tile_processors(get_assignment_procgrid(A, numprocs, get_default_blockgrid(A, numprocs)), blocks_a)
      # @test chunk_processors(rand_v_exp)   == chunk_processors(randn_v_exp) == chunk_processors(sprand_v_exp) == chunk_processors(ones_v_exp) == chunk_processors(zeros_v_exp) == tile_processors(rand_procs_A, blocks_a)
      # @test chunk_processors(rand_M_exp)   == chunk_processors(randn_M_exp) == chunk_processors(sprand_M_exp) ==  chunk_processors(ones_M_exp) == chunk_processors(zeros_M_exp) == tile_processors(rand_procs_A, blocks_a)

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
