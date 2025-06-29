using Random
using LinearAlgebra

if USE_CUDA
    using Pkg
    Pkg.add("CUDA")
end
if USE_ROCM
    using Pkg
    Pkg.add("AMDGPU")
end
if USE_ONEAPI
    using Pkg
    Pkg.add("oneAPI")
end
if USE_METAL
    using Pkg
    Pkg.add("Metal")
end

@everywhere begin
    if !$IN_CI || $USE_CUDA
        try using CUDA
        catch end
    end

    if !$IN_CI || $USE_ROCM
        try using AMDGPU
        catch end
    end

    if !$IN_CI || $USE_ONEAPI
        try using oneAPI
        catch end
    end

    if !$IN_CI || $USE_METAL
        try using Metal
        catch end
    end

    using Distributed, Dagger
    import Dagger: Kernel
    using KernelAbstractions
end
@everywhere begin
    function isongpu(X)
        return !(X isa Array)
    end

    @kernel function fill_kernel(A, x)
        idx = @index(Global, Linear)
        A[idx] = x
    end
    function fill_thunk(A, x)
        backend = Dagger.gpu_kernel_backend()
        k = fill_kernel(backend, 8)
        k(A, x; ndrange=8)
        KernelAbstractions.synchronize(backend)
        return A, typeof(A)
    end

    @kernel function copy_kernel(B, A)
        idx = @index(Global, Linear)
        B[idx] = A[idx]
    end

    # Create a function to perform an in-place operation.
    function addarray!(x)
        x .= x .+ 1.0f0
    end
end

function generate_thunks()
    as = [Dagger.spawn(x->x+1, 1) for i in 1:10]
    Dagger.spawn((xs...)->[sum(xs)], as...)
end

@test (!USE_CUDA || Dagger.gpu_can_compute(:CUDA)) &&
      (!USE_ROCM || Dagger.gpu_can_compute(:ROC)) &&
      (!USE_ONEAPI || Dagger.gpu_can_compute(:oneAPI)) &&
      (!USE_METAL || Dagger.gpu_can_compute(:Metal))

@testset "CPU" begin
    @testset "KernelAbstractions" begin
        A = rand(Float32, 8)
        DA, T = fetch(Dagger.@spawn fill_thunk(A, 2.3f0))
        @test all(DA .== 2.3f0)
        @test T <: Array

        A = rand(Float64, 128)
        B = zeros(Float64, 128)
        Dagger.with_options(scope=Dagger.scope(worker=1,thread=1)) do
            fetch(Dagger.@spawn Kernel(copy_kernel)(B, A; ndrange=length(A)))
            Dagger.gpu_synchronize(:CPU)
        end
        @test all(B .== A)
    end
end

@testset "CUDA" begin
    if !Dagger.gpu_can_compute(:CUDA)
        @warn "No CUDA devices available, skipping tests"
    else
        cuproc = Base.get_extension(Dagger, :CUDAExt).CuArrayDeviceProc
        @test Dagger.gpu_processor(:CUDA) === cuproc
        ndevices = length(collect(CUDA.devices()))
        gpu_configs = Any[1]
        if ndevices > 1
            push!(gpu_configs, 2)
        end
        single_gpu_configs = copy(gpu_configs)
        push!(gpu_configs, :all)

        @testset "Arrays (GPU $gpu)" for gpu in gpu_configs
            scope = Dagger.scope(cuda_gpus=(gpu == :all ? Colon() : [gpu]))

            b = generate_thunks()
            c = Dagger.with_options(;scope) do
                @test fetch(Dagger.@spawn isongpu(b))
                Dagger.@spawn sum(b)
            end
            @test !fetch(Dagger.@spawn isongpu(b))
            @test fetch(Dagger.@spawn identity(c)) == 20
        end

        @testset "KernelAbstractions (GPU $gpu)" for gpu in gpu_configs
            scope = Dagger.scope(cuda_gpus=(gpu == :all ? Colon() : [gpu]))
            local_scope = Dagger.scope(worker=1, cuda_gpus=(gpu == :all ? Colon() : [gpu]))

            A = rand(Float32, 8)
            DA, T = Dagger.with_options(;scope) do
                fetch(Dagger.@spawn fill_thunk(A, 2.3f0))
            end
            @test all(DA .== 2.3f0)
            @test T <: CuArray

            if gpu != :all
                local A, B
                CUDA.device!(gpu-1) do
                    A = CUDA.rand(128)
                    B = CUDA.zeros(128)
                end
                Dagger.with_options(;scope=local_scope) do
                    fetch(Dagger.@spawn Kernel(copy_kernel)(B, A; ndrange=length(A)))
                    Dagger.gpu_synchronize(:CUDA)
                end
                CUDA.device!(gpu-1) do
                    @test all(B .== A)
                end
            end
        end

        @testset "DArray Allocation (GPU $gpu)" for gpu in single_gpu_configs
            scope = Dagger.scope(cuda_gpu=gpu)

            DA_cpu = rand(Blocks(4, 4), 8, 8)
            Dagger.with_options(;scope) do
                DA_gpu = similar(DA_cpu)
                for chunk in DA_gpu.chunks
                    chunk = fetch(chunk; raw=true)
                    @test chunk isa Dagger.Chunk{<:CuArray}
                    @test remotecall_fetch(chunk.handle.owner, chunk) do chunk
                        devs = collect(CUDA.devices())
                        arr = Dagger.MemPool.poolget(chunk.handle)
                        return CUDA.device(arr) == devs[gpu]
                    end
                end

                for fn in (rand, randn, ones, zeros)
                    DA = rand(Blocks(4, 4), 8, 8)
                    for chunk in DA.chunks
                        chunk = fetch(chunk; raw=true)
                        @test chunk isa Dagger.Chunk{<:CuArray}
                        @test remotecall_fetch(chunk.handle.owner, chunk) do chunk
                            devs = collect(CUDA.devices())
                            arr = Dagger.MemPool.poolget(chunk.handle)
                            return CUDA.device(arr) == devs[gpu]
                        end
                    end
                end
            end
        end

        @testset "Datadeps (GPU $gpu)" for gpu in gpu_configs
            local_scope = Dagger.scope(worker=1, cuda_gpus=(gpu == :all ? Colon() : [gpu]))

            DA = rand(Blocks(4, 4), 8, 8)
            DB = rand(Blocks(4, 4), 8, 8)

            # In-place Matmul
            DC = zeros(Blocks(4, 4), 8, 8)
            Dagger.with_options(;scope=local_scope) do
                mul!(DC, DA, DB)
            end
            @test collect(DC) ≈ collect(DA) * collect(DB)

            # Out-of-place Matmul
            Dagger.with_options(;scope=local_scope) do
                @test collect(DA * DB) ≈ collect(DA) * collect(DB)
            end

            # Out-of-place Cholesky
            A = rand(8, 8)
            A = A * A'
            A[diagind(A)] .+= size(A, 1)
            DA = DArray(A, Blocks(4, 4))
            Dagger.with_options(;scope=local_scope) do
                @test collect(cholesky(DA).U) ≈ cholesky(collect(DA)).U
            end
        end
    end
end

@testset "ROCm" begin
    if !Dagger.gpu_can_compute(:ROC)
        @warn "No ROCm devices available, skipping tests"
    else
        rocproc = Base.get_extension(Dagger, :ROCExt).ROCArrayDeviceProc
        @test Dagger.gpu_processor(:ROC) === rocproc
        ndevices = length(AMDGPU.devices())
        gpu_configs = Any[1]
        if ndevices > 1
            push!(gpu_configs, 2)
        end
        single_gpu_configs = copy(gpu_configs)
        push!(gpu_configs, :all)

        @testset "Arrays (GPU $gpu)" for gpu in gpu_configs
            scope = Dagger.scope(rocm_gpus=(gpu == :all ? Colon() : [gpu]))

            b = generate_thunks()
            c = Dagger.with_options(;scope) do
                @test fetch(Dagger.@spawn isongpu(b))
                Dagger.@spawn sum(b)
            end
            @test !fetch(Dagger.@spawn isongpu(b))
            @test fetch(Dagger.@spawn identity(c)) == 20
        end

        @testset "KernelAbstractions (GPU $gpu)" for gpu in gpu_configs
            scope = Dagger.scope(rocm_gpus=(gpu == :all ? Colon() : [gpu]))
            local_scope = Dagger.scope(worker=1, rocm_gpus=(gpu == :all ? Colon() : [gpu]))

            A = rand(Float32, 8)
            DA, T = Dagger.with_options(;scope) do
                fetch(Dagger.@spawn fill_thunk(A, 2.3f0))
            end
            @test all(DA .== 2.3f0)
            @test T <: ROCArray

            if gpu != :all
                local A, B
                AMDGPU.device!(AMDGPU.devices()[gpu]) do
                    A = AMDGPU.rand(128)
                    B = AMDGPU.zeros(128)
                end
                Dagger.with_options(;scope=local_scope) do
                    fetch(Dagger.@spawn Kernel(copy_kernel)(B, A; ndrange=length(A)))
                    Dagger.gpu_synchronize(:ROC)
                end
                AMDGPU.device!(AMDGPU.devices()[gpu]) do
                    @test all(B .== A)
                end
            end
        end

        @testset "DArray Allocation (GPU $gpu)" for gpu in single_gpu_configs
            scope = Dagger.scope(rocm_gpu=gpu)

            DA_cpu = rand(Blocks(4, 4), 8, 8)
            Dagger.with_options(;scope) do
                DA_gpu = similar(DA_cpu)
                for chunk in DA_gpu.chunks
                    chunk = fetch(chunk; raw=true)
                    @test chunk isa Dagger.Chunk{<:ROCArray}
                    @test remotecall_fetch(chunk.handle.owner, chunk) do chunk
                        devs = collect(AMDGPU.devices())
                        arr = Dagger.MemPool.poolget(chunk.handle)
                        return AMDGPU.device(arr) == devs[gpu]
                    end
                end

                for fn in (rand, randn, ones, zeros)
                    DA = rand(Blocks(4, 4), 8, 8)
                    for chunk in DA.chunks
                        chunk = fetch(chunk; raw=true)
                        @test chunk isa Dagger.Chunk{<:ROCArray}
                        @test remotecall_fetch(chunk.handle.owner, chunk) do chunk
                            devs = collect(AMDGPU.devices())
                            arr = Dagger.MemPool.poolget(chunk.handle)
                            return AMDGPU.device(arr) == devs[gpu]
                        end
                    end
                end
            end
        end

        @testset "Datadeps (GPU $gpu)" for gpu in gpu_configs
            local_scope = Dagger.scope(worker=1, rocm_gpus=(gpu == :all ? Colon() : [gpu]))

            DA = rand(Blocks(4, 4), 8, 8)
            DB = rand(Blocks(4, 4), 8, 8)

            # In-place Matmul
            DC = zeros(Blocks(4, 4), 8, 8)
            Dagger.with_options(;scope=local_scope) do
                mul!(DC, DA, DB)
            end
            @test collect(DC) ≈ collect(DA) * collect(DB)

            # Out-of-place Matmul
            Dagger.with_options(;scope=local_scope) do
                @test collect(DA * DB) ≈ collect(DA) * collect(DB)
            end

            # Out-of-place Cholesky
            A = rand(8, 8)
            A = A * A'
            A[diagind(A)] .+= size(A, 1)
            DA = DArray(A, Blocks(4, 4))
            Dagger.with_options(;scope=local_scope) do
                @test collect(cholesky(DA).U) ≈ cholesky(collect(DA)).U
            end
        end
    end
end

@testset "oneAPI" begin
    if !Dagger.gpu_can_compute(:oneAPI)
        @warn "No oneAPI devices available, skipping tests"
    else
        oneproc = Base.get_extension(Dagger, :IntelExt).oneArrayDeviceProc
        @test Dagger.gpu_processor(:oneAPI) === oneproc
        ndevices = length(oneAPI.devices())
        gpu_configs = Any[1]
        if ndevices > 1
            push!(gpu_configs, 2)
        end
        single_gpu_configs = copy(gpu_configs)
        push!(gpu_configs, :all)

        @testset "Arrays (GPU $gpu)" for gpu in gpu_configs
            scope = Dagger.scope(intel_gpus=(gpu == :all ? Colon() : [gpu]))

            b = generate_thunks()
            c = Dagger.with_options(;scope) do
                @test fetch(Dagger.@spawn isongpu(b))
                Dagger.@spawn sum(b)
            end
            @test !fetch(Dagger.@spawn isongpu(b))
            @test fetch(Dagger.@spawn identity(c)) == 20
        end

        @testset "KernelAbstractions (GPU $gpu)" for gpu in gpu_configs
            scope = Dagger.scope(intel_gpus=(gpu == :all ? Colon() : [gpu]))
            local_scope = Dagger.scope(worker=1, intel_gpus=(gpu == :all ? Colon() : [gpu]))

            A = rand(Float32, 8)
            DA, T = Dagger.with_options(;scope) do
                fetch(Dagger.@spawn fill_thunk(A, 2.3f0))
            end
            @test all(DA .== 2.3f0)
            @test T <: oneArray

            if gpu != :all
                local A, B
                old_dev = oneAPI.device()
                oneAPI.device!(oneAPI.devices()[gpu])
                A = oneAPI.rand(Float32, 128)
                B = oneAPI.zeros(Float32, 128)
                oneAPI.device!(old_dev)
                Dagger.with_options(;scope=local_scope) do
                    fetch(Dagger.@spawn Kernel(copy_kernel)(B, A; ndrange=length(A)))
                    Dagger.gpu_synchronize(:oneAPI)
                end
                old_dev = oneAPI.device()
                oneAPI.device!(oneAPI.devices()[gpu])
                @test all(B .== A)
                oneAPI.device!(old_dev)
            end
        end

        @testset "DArray Allocation (GPU $gpu)" for gpu in single_gpu_configs
            scope = Dagger.scope(intel_gpu=gpu)

            DA_cpu = rand(Blocks(4, 4), Float32, 8, 8)
            Dagger.with_options(;scope) do
                DA_gpu = similar(DA_cpu)
                for chunk in DA_gpu.chunks
                    chunk = fetch(chunk; raw=true)
                    @test chunk isa Dagger.Chunk{<:oneArray}
                    @test remotecall_fetch(chunk.handle.owner, chunk) do chunk
                        devs = collect(oneAPI.devices())
                        arr = Dagger.MemPool.poolget(chunk.handle)
                        return oneAPI.device(arr) == devs[gpu]
                    end
                end

                for fn in (rand, randn, ones, zeros)
                    DA = rand(Blocks(4, 4), Float32, 8, 8)
                    for chunk in DA.chunks
                        chunk = fetch(chunk; raw=true)
                        @test chunk isa Dagger.Chunk{<:oneArray}
                        @test remotecall_fetch(chunk.handle.owner, chunk) do chunk
                            devs = collect(oneAPI.devices())
                            arr = Dagger.MemPool.poolget(chunk.handle)
                            return oneAPI.device(arr) == devs[gpu]
                        end
                    end
                end
            end
        end

        #= FIXME: Requires more generic matmul, missing Cholesky methods
        @testset "Datadeps (GPU $gpu)" for gpu in gpu_configs
            local_scope = Dagger.scope(worker=1, intel_gpus=(gpu == :all ? Colon() : [gpu]))

            DA = rand(Blocks(4, 4), Float32, 8, 8)
            DB = rand(Blocks(4, 4), Float32, 8, 8)

            # In-place Matmul
            DC = zeros(Blocks(4, 4), Float32, 8, 8)
            Dagger.with_options(;scope=local_scope) do
                mul!(DC, DA, DB)
            end
            @test collect(DC) ≈ collect(DA) * collect(DB)

            # Out-of-place Matmul
            Dagger.with_options(;scope=local_scope) do
                @test collect(DA * DB) ≈ collect(DA) * collect(DB)
            end

            # Out-of-place Cholesky
            A = rand(Float32, 8, 8)
            A = A * A'
            A[diagind(A)] .+= size(A, 1)
            DA = DArray(A, Blocks(4, 4))
            Dagger.with_options(;scope=local_scope) do
                @test collect(cholesky(DA).U) ≈ cholesky(collect(DA)).U
            end
        end
        =#
    end
end

@testset "Metal" begin
    if !Dagger.gpu_can_compute(:Metal)
        @warn "No Metal devices available, skipping tests"
    else
        mtlproc = Base.get_extension(Dagger, :MetalExt).MtlArrayDeviceProc
        @test Dagger.gpu_processor(:Metal) === mtlproc
        b = generate_thunks()
        c = Dagger.with_options(;scope=Dagger.scope(metal_gpu=1)) do
            @test fetch(Dagger.@spawn isongpu(b))
            Dagger.@spawn sum(b)
        end
        @test !fetch(Dagger.@spawn isongpu(b))
        @test fetch(Dagger.@spawn identity(c)) == 20

        @testset "KernelAbstractions" begin
            scope = Dagger.scope(metal_gpu=1)
            local_scope = Dagger.scope(worker=1, metal_gpu=1)

            A = rand(Float32, 8)
            DA, T = Dagger.with_options(;scope) do
                fetch(Dagger.@spawn fill_thunk(A, 2.3f0))
            end
            @test all(DA .== 2.3f0)
            @test T <: MtlArray

            A = Metal.rand(128)
            B = Metal.zeros(128)
            Dagger.with_options(;scope=local_scope) do
                fetch(Dagger.@spawn Kernel(copy_kernel)(B, A; ndrange=length(A)))
                Dagger.gpu_synchronize(:Metal)
            end
            @test all(B .== A)
        end

        @testset "DArray Allocation" begin
            gpu = 1
            # FIXME: Multi-worker serialization is broken
            scope = Dagger.scope(worker=1, metal_gpu=gpu)

            DA_cpu = rand(Blocks(4, 4), Float32, 8, 8)
            Dagger.with_options(;scope) do
                DA_gpu = similar(DA_cpu)
                for chunk in DA_gpu.chunks
                    chunk = fetch(chunk; raw=true)
                    @test chunk isa Dagger.Chunk{<:MtlArray}
                    @test remotecall_fetch(chunk.handle.owner, chunk) do chunk
                        devs = collect(Metal.devices())
                        arr = Dagger.MemPool.poolget(chunk.handle)
                        return Metal.device(arr) == devs[gpu]
                    end
                end

                for fn in (rand, randn, ones, zeros)
                    DA = rand(Blocks(4, 4), Float32, 8, 8)
                    for chunk in DA.chunks
                        chunk = fetch(chunk; raw=true)
                        @test chunk isa Dagger.Chunk{<:MtlArray}
                        @test remotecall_fetch(chunk.handle.owner, chunk) do chunk
                            devs = collect(Metal.devices())
                            arr = Dagger.MemPool.poolget(chunk.handle)
                            return Metal.device(arr) == devs[gpu]
                        end
                    end
                end
            end
        end

        #= FIXME: Requires more generic matmul, missing Cholesky methods
        @testset "Datadeps" begin
            local_scope = Dagger.scope(worker=1, metal_gpu=1)

            DA = rand(Blocks(4, 4), Float32, 8, 8)
            DB = rand(Blocks(4, 4), Float32, 8, 8)

            # In-place Matmul
            DC = zeros(Blocks(4, 4), Float32, 8, 8)
            Dagger.with_options(;scope=local_scope) do
                mul!(DC, DA, DB)
            end
            @test collect(DC) ≈ collect(DA) * collect(DB)

            # Out-of-place Matmul
            Dagger.with_options(;scope=local_scope) do
                @test collect(DA * DB) ≈ collect(DA) * collect(DB)
            end

            # Out-of-place Cholesky
            A = rand(Float32, 8, 8)
            A = A * A'
            A[diagind(A)] .+= size(A, 1)
            DA = DArray(A, Blocks(4, 4))
            Dagger.with_options(;scope=local_scope) do
                @test collect(cholesky(DA).U) ≈ cholesky(collect(DA)).U
            end
        end
        =#

        @testset "In-place operations" begin
            # Create a page-aligned array.
            dims = (2, 2)
            T = Float32
            pagesize = ccall(:getpagesize, Cint, ())
            addr = Ref(C_NULL)

            ccall(
                :posix_memalign,
                Cint,
                (Ptr{Ptr{Cvoid}}, Csize_t, Csize_t), addr,
                pagesize,
                prod(dims) * sizeof(T)
            )

            array = unsafe_wrap(
                Array{T, length(dims)},
                reinterpret(Ptr{T}, addr[]),
                dims,
                own = false
            )

            # Initialize the array.
            array[1, 1] = 1
            array[1, 2] = 2
            array[2, 1] = 3
            array[2, 2] = 4

            # Perform the computation only on a local `MtlArrayDeviceProc`
            t = Dagger.@spawn scope=Dagger.scope(worker=1, metal_gpu=1) addarray!(array)

            # Fetch and check the results.
            ret = fetch(t)

            @test ret[1, 1] == 2.0f0
            @test ret[1, 2] == 3.0f0
            @test ret[2, 1] == 4.0f0
            @test ret[2, 2] == 5.0f0

            # Check if the operation happened in-place.
            @test_broken array[1, 1] == 2.0f0
            @test_broken array[1, 2] == 3.0f0
            @test_broken array[2, 1] == 4.0f0
            @test_broken array[2, 2] == 5.0f0
        end
    end
end
