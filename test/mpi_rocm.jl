# MPI × GPU datadeps suite for ROCm. See test/mpi_gpu_suite.jl for the shared
# logic; this file only supplies the ROCm-specific config.
#
# Run (env must provide Dagger, MPI, AMDGPU; see test/rocmenv):
#   mpiexec -n 2 julia --project=test/rocmenv --threads=2 test/mpi_rocm.jl

using Dagger, MPI, AMDGPU, LinearAlgebra, Random, Test
using Dagger: In, Out, InOut, Deps

include(joinpath(@__DIR__, "mpi_gpu_suite.jl"))

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
end

run_mpi_gpu_suite((;
    name = "ROCm",
    DeviceProc = ROCExt.ROCArrayDeviceProc,
    elt = Float64,
    matmul = true,
    cholesky = true,
    remap = (;
        make_space = () -> ROCExt.ROCVRAMMemorySpace(1, 1),
        device_field = :device_id,
        kind = :ROC,
        extra = rocm_gpu_direct_check,
    ),
))
