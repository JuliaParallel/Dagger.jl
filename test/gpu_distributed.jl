# Included by gpu.jl in every GPU CI job. Both workers select device 1, so
# this exercises inter-process transfers even on agents with just one GPU.
using Distributed, Dagger, LinearAlgebra, Test

@everywhere begin
    function distributed_gpu_add!(A)
        A .+= 1f0
        return nothing
    end
    function distributed_gpu_scale!(A)
        A .*= 2f0
        return nothing
    end
end

function test_distributed_gpu(kind, gpu_key)
    ws = filter(!=(myid()), workers())
    @test length(ws) >= 2
    length(ws) >= 2 || return
    w1, w2 = ws[1:2]
    @test Dagger.system_uuid(w1) == Dagger.system_uuid(w2)
    gpu_kw = NamedTuple{(gpu_key,)}((1,))
    s1 = Dagger.scope(; worker=w1, gpu_kw...)
    s2 = Dagger.scope(; worker=w2, gpu_kw...)
    p1 = only(Dagger.compatible_processors(s1))
    p2 = only(Dagger.compatible_processors(s2))
    @test p1 isa Dagger.gpu_processor(kind)
    @test p2 isa Dagger.gpu_processor(kind)
    scope = Dagger.UnionScope(s1, s2)

    @testset "GPU chunk transfers" begin
        # Include a size above the IPC threshold as well as small arrays.
        # Use nonuniform data to detect lost, stale, or incorrectly offset copies.
        for dims in ((8, 8), (256, 256))
            A = reshape(Float32.(1:prod(dims)), dims)
            source_task = Dagger.@spawn scope=s1 identity(A)
            source = fetch(source_task; raw=true)
            @test Dagger.processor(source) == p1
            @test Dagger.chunktype(source) <: Dagger.GPUArraysCore.AbstractGPUArray

            # Repeat the import of the same source, and exercise both directions.
            # Return a Chunk so the device array never crosses serialization.
            for _ in 1:2
                received = remotecall_fetch(w2, p1, p2, source) do from, to, chunk
                    Dagger.with_context(to) do
                        value = Dagger.move(from, to, chunk)
                        Dagger.tochunk(value, to)
                    end
                end
                @test Dagger.processor(received) == p2
                @test Dagger.chunktype(received) <: Dagger.GPUArraysCore.AbstractGPUArray
                @test collect(received) == A
                remotecall_fetch(w2, p2, received) do proc, chunk
                    Dagger.with_context(proc) do
                        distributed_gpu_add!(Dagger.unwrap(chunk))
                        Dagger.gpu_synchronize(proc)
                    end
                    nothing
                end
                @test collect(source) == A
                @test collect(received) == A .+ 1f0
                returned = Dagger.@spawn scope=s1 identity(received)
                @test fetch(returned) == A .+ 1f0
                @test Dagger.processor(fetch(returned; raw=true)) == p1
            end
        end
    end

    @testset "Cross-worker datadeps" begin
        A = reshape(Float32.(1:64), 8, 8)
        ref = copy(A)
        Dagger.with_options(; scope) do
            Dagger.spawn_datadeps() do
                Dagger.@spawn scope=s1 distributed_gpu_add!(Dagger.InOut(A))
                Dagger.@spawn scope=s2 distributed_gpu_scale!(Dagger.InOut(A))
                Dagger.@spawn scope=s1 distributed_gpu_add!(Dagger.InOut(A))
            end
        end
        @test A == (ref .+ 1f0) .* 2f0 .+ 1f0

    end

    @testset "Distributed GPU arrays" begin
        A = reshape(Float32.(1:256) ./ 256f0, 16, 16)
        B = reverse(A; dims=1)
        Dagger.with_options(; scope) do
            # Opposite placements guarantee that corresponding tiles must move.
            DA = Dagger.distribute(A, Dagger.Blocks(8, 16),
                                  reshape(Dagger.Processor[p1, p2], 2, 1))
            DB = Dagger.distribute(B, Dagger.Blocks(8, 16),
                                  reshape(Dagger.Processor[p2, p1], 2, 1))
            wait(DA)
            wait(DB)
            @test collect(DA .+ DB) ≈ A .+ B

            # The original reproducer uses row tiles times column tiles.
            # Metal/OpenCL currently lack the required dense matmul methods;
            # their transfer, broadcast, and datadeps coverage above is shared.
            if kind in (:CUDA, :ROC, :oneAPI)
                DB_cols = Dagger.distribute(B, Dagger.Blocks(16, 8),
                                           reshape(Dagger.Processor[p2, p1], 1, 2))
                wait(DB_cols)
                @test collect(DA * DB_cols) ≈ A * B
            end
        end
    end
end

@testset "Distributed workers sharing one GPU ($kind)" for (kind, key) in
    ((:CUDA, :cuda_gpu), (:ROC, :rocm_gpu), (:oneAPI, :intel_gpu),
     (:Metal, :metal_gpu), (:OpenCL, :cl_device))
    if Dagger.gpu_can_compute(kind)
        test_distributed_gpu(kind, key)
    end
end
