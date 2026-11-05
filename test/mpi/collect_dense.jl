struct FailingCollectProcessor <: Dagger.Processor end
Dagger.gpu_synchronize(::FailingCollectProcessor) = error("owner readback failed")
collect_shift_cat(xs...; dims) = cat(xs...; dims) .+ 1
mpi_collect_fill!(x,v) = (fill!(x,v); nothing)

function test_mpi_dense_collect(scope)
    Dagger.with_options(; scope) do
        @testset "Replicated dense host collection" begin
            for T in (Int64, Float32, Float64, ComplexF64),
                (dims, blocks) in (((19,17),(6,5)), ((13,),(5,)),
                                  ((7,5,3),(3,2,2)), ((2,2),(8,8)))
                input = reshape(T.(1:prod(dims)), dims)
                a = fetch(Dagger.DArray(input, Dagger.Blocks(blocks...)))
                result = collect(a)
                @test result isa Array{T,length(dims)}
                @test result == input
                result[1] = zero(T)
                @test collect(a) == input
            end
            for dims in ((0,3), (3,0), (0,))
                a = Dagger.DArray(zeros(dims), Dagger.Blocks(ntuple(_->2, length(dims))...))
                @test size(collect(a)) == dims
            end
            input = reshape(Float64.(1:323),19,17)
            a = fetch(Dagger.DArray(input,Dagger.Blocks(6,5)))
            shifted = Dagger.DArray(Float64,Dagger.ArrayDomain((11:29,-6:10)),
                Dagger.DomainBlocks((11,-6),a.subdomains.cumlength),a.chunks,a.partitioning)
            @test collect(shifted) == input
            ps = MPIExt.uniform_mpi_processors(Dagger.current_acceleration())
            # Cover every eligible rank even when there are more processors
            # than the small ragged fixture has tiles.
            ownership = fetch(Dagger.DArray(zeros(Float32,2,2length(ps)),Dagger.Blocks(2,2)))
            @test Set(c.handle.rank for c in ownership.chunks) == Set(p.rank for p in ps)

            # All peers must see a failed owner readback before waiting for data.
            chunk = first(a.chunks)
            bad = Dagger.Chunk(chunk.chunktype,chunk.domain,chunk.handle,
                               FailingCollectProcessor(),chunk.scope,chunk.space)
            @test_throws ErrorException MPIExt.mpi_collect_host_tile(Dagger.current_acceleration(),bad)
            @test MPIExt.mpi_collect_host_tile(Dagger.current_acceleration(),chunk) == input[1:6,1:5]

            # Completing a task is not publishing its dirty remote replica.
            # collect must preserve the old gather graph's read dependency.
            a = fetch(Dagger.DArray(zeros(4,4),Dagger.Blocks(4,4)))
            target = last(MPIExt.uniform_mpi_processors(Dagger.current_acceleration()))
            writer = Dagger.with_options(;scope=Dagger.ExactScope(target)) do
                Dagger.spawn_datadeps(;sync=false,hierarchical=false) do
                    Dagger.@spawn mpi_collect_fill!(Dagger.InOut(only(a.chunks)),7.0)
                end
            end
            fetch(writer)
            @test collect(a) == fill(7.0,4,4)
            Dagger.synchronize()
        end
    end
end

@testset "Custom and structured MPI collection fallback" begin
    input = Float64.(1:4)
    a = fetch(Dagger.DArray(input,Dagger.Blocks(2)))
    custom = Dagger.DArray(Float64,a.domain,a.subdomains,a.chunks,a.partitioning,collect_shift_cat)
    @test collect(custom) == input .+ 1
    a = fetch(Dagger.DArray(reshape(Float64.(1:16),4,4),Dagger.Blocks(4,4)))
    c = Dagger.tochunk(LinearAlgebra.Diagonal(Float64.(1:4)))
    structured = Dagger.DArray(Float64,a.domain,a.subdomains,reshape([c],1,1),a.partitioning)
    @test collect(structured) == Matrix(LinearAlgebra.Diagonal(Float64.(1:4)))
end
