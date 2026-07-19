using Test
using Dagger
using Dagger: CyclicProcGrid, BlockedProcGrid, DenseProcGrid,
              build_procgrid, procgrid_processor, materialize_procgrid,
              DistributedAcceleration

function legacy_dense_procgrid(assignment, sizeA, blocksize, availprocs)
    N = length(sizeA)
    np = length(availprocs)
    nblocks = ntuple(i -> cld(sizeA[i], blocksize[i]), N)
    if assignment == :arbitrary
        return nothing
    elseif assignment == :blockrow
        shape = ntuple(i -> i == 1 ? Int(ceil(sizeA[1] / blocksize[1])) : 1, N)
        rows_per_proc, extra = divrem(Int(ceil(sizeA[1] / blocksize[1])), np)
        counts = [rows_per_proc + (i <= extra ? 1 : 0) for i in 1:np]
        return reshape(vcat(fill.(availprocs, counts)...), shape)
    elseif assignment == :blockcol
        shape = ntuple(i -> i == N ? Int(ceil(sizeA[N] / blocksize[N])) : 1, N)
        cols_per_proc, extra = divrem(Int(ceil(sizeA[N] / blocksize[N])), np)
        counts = [cols_per_proc + (i <= extra ? 1 : 0) for i in 1:np]
        return reshape(vcat(fill.(availprocs, counts)...), shape)
    elseif assignment == :cyclicrow
        shape = ntuple(i -> i == 1 ? np : 1, N)
        return reshape(availprocs, shape)
    elseif assignment == :cycliccol
        shape = ntuple(i -> i == N ? np : 1, N)
        return reshape(availprocs, shape)
    end
    error("unknown assignment")
end

function legacy_lookup(grid, I::CartesianIndex)
    grid[CartesianIndex(mod1.(Tuple(I), size(grid))...)]
end

availprocs = collect(Dagger.compatible_processors())
filter!(p -> p isa Dagger.ThreadProc, availprocs)
sort!(availprocs, by=x -> (x.owner, x.tid))
np = length(availprocs)
accel = DistributedAcceleration()

@testset "build_procgrid :arbitrary Distributed" begin
    @test build_procgrid(:arbitrary, (16, 16), (4, 4), accel) === nothing
end

for assignment in (:blockrow, :blockcol, :cyclicrow, :cycliccol)
    @testset "parity $assignment" begin
        sizeA = (41, 35)
        blocksize = (4, 3)
        nblocks = ntuple(i -> cld(sizeA[i], blocksize[i]), 2)
        pg = build_procgrid(assignment, sizeA, blocksize, accel)
        legacy = legacy_dense_procgrid(assignment, sizeA, blocksize, availprocs)
        dense = materialize_procgrid(pg, nblocks)
        for I in CartesianIndices(nblocks)
            @test procgrid_processor(pg, I) == legacy_lookup(legacy, I)
            @test dense[I] == legacy_lookup(legacy, I)
        end
        # tiling: larger block grid than procgrid shape
        big = (nblocks[1] + 2, nblocks[2] + 1)
        for I in CartesianIndices(big)
            @test procgrid_processor(pg, I) == legacy_lookup(legacy, I)
        end
    end
end

@testset "CyclicProcGrid MPI round-robin parity" begin
    nblocks = (4, 4)
    procs = availprocs[1:min(4, np)]
    pg = CyclicProcGrid(procs, nblocks)
    legacy = Array{Dagger.Processor, 2}(undef, nblocks)
    for (i, I) in enumerate(CartesianIndices(legacy))
        legacy[I] = procs[mod1(i, length(procs))]
    end
    for I in CartesianIndices(nblocks)
        @test procgrid_processor(pg, I) == legacy[I]
    end
end

@testset "BlockedProcGrid" begin
    np_test = min(np, 3)
    if np_test >= 2
        shape = (6, 1)
        counts = fill(2, np_test)
        procs = availprocs[1:np_test]
        pg = BlockedProcGrid(procs, counts, shape, 1)
        legacy = reshape(vcat(fill.(procs, counts)...), shape)
        for I in CartesianIndices((8, 1))
            @test procgrid_processor(pg, I) == legacy_lookup(legacy, I)
        end
    end
end

@testset "DenseProcGrid" begin
    np_test = min(np, 4)
    if np_test >= 4
        grid = reshape(availprocs[1:4], (2, 2))
        pg = DenseProcGrid(grid)
        for I in CartesianIndices((5, 5))
            @test procgrid_processor(pg, I) == legacy_lookup(grid, I)
        end
    end
end
