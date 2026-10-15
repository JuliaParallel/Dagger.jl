# MPI × GPU datadeps suite for ROCm. See test/mpi/gpu_suite.jl for the shared
# logic; this file only supplies the ROCm-specific config.
#
# Run (env must provide Dagger, MPI, AMDGPU; see test/rocmenv):
#   mpiexec -n 2 julia --project=test/rocmenv --threads=2 test/mpi/rocm.jl

using AMDGPU_LLVM_Backend_jll
using Dagger, MPI, AMDGPU, LinearAlgebra, Random, Test
using Dagger: In, Out, InOut, Deps

using Distributed

include(joinpath(@__DIR__, "gpu_suite.jl"))

const ROCExt = Base.get_extension(Dagger, :ROCExt)
@assert ROCExt !== nothing "ROCExt failed to load"

function rocm_gpu_direct_check()
    # Default is library-detected; force-off must stick for this process
    old = get(ENV, "DAGGER_MPI_GPU_DIRECT", nothing)
    try
        ENV["DAGGER_MPI_GPU_DIRECT"] = "0"
        ROCExt.MPI_GPU_DIRECT[] = nothing
        A = AMDGPU.ones(Float32, 4)
        @test Dagger.mpi_device_direct(A) == false
    finally
        if old === nothing
            delete!(ENV, "DAGGER_MPI_GPU_DIRECT")
        else
            ENV["DAGGER_MPI_GPU_DIRECT"] = old
        end
        ROCExt.MPI_GPU_DIRECT[] = nothing
    end

    @testset "Uninitialized ROCm replica slots" begin
        # A slot is allocated on its named device, with the exact source type
        # and shape, but owns independent storage. Datadeps fills it later.
        for T in (Float32, Float64, ComplexF32, ComplexF64), dims in ((3, 4), (0, 4))
            source = ROCArray{T}(undef, dims)
            space = Dagger.value_memory_space(source)
            @test Dagger.can_alloc_uninit(space, typeof(source))
            slot = Dagger.alloc_uninit(space, typeof(source), Dagger.alloc_header(source))
            @test typeof(slot) === typeof(source)
            @test size(slot) == dims
            @test Dagger.value_memory_space(slot) == space
            source .= one(T)
            slot .= T(3)
            AMDGPU.synchronize()
            @test Array(source) == fill(one(T), dims)
            @test Array(slot) == fill(T(3), dims)
        end
        @test !Dagger.can_alloc_uninit(ROCExt.ROCVRAMMemorySpace(1, 1), ROCArray{Any, 2})
    end
end

run_mpi_gpu_suite((;
    name = "ROCm",
    DeviceProc = Dagger.ROCArrayDeviceProc,
    gpu_key = :rocm_gpu,
    elt = Float64,
    matmul = true,
    cholesky = true,
    stencil = true,
    # The 3D/4D stencil tests break the ROCm backend and corrupt subsequent
    # tests (mirrors the single-process skip in test/array/stencil.jl).
    stencil_skip_highdim = true,
    sparse = true,
    remap = (;
        make_space = () -> ROCExt.ROCVRAMMemorySpace(1, 1),
        device_field = :device_id,
        kind = :ROC,
        extra = rocm_gpu_direct_check,
    ),
))
