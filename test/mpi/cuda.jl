# MPI × GPU datadeps suite for CUDA. See test/mpi/gpu_suite.jl for the shared
# logic; this file only supplies the CUDA-specific config.
#
# Run (env must provide Dagger, MPI, CUDA; see test/cudaenv):
#   mpiexec -n 2 julia --project=test/cudaenv --threads=2 test/mpi/cuda.jl

using Dagger, MPI, CUDA, LinearAlgebra, Random, Test
using Dagger: In, Out, InOut, Deps

using Distributed

include(joinpath(@__DIR__, "gpu_suite.jl"))

const CUDAExt = Base.get_extension(Dagger, :CUDAExt)
@assert CUDAExt !== nothing "CUDAExt failed to load"

run_mpi_gpu_suite((;
    name = "CUDA",
    DeviceProc = Dagger.CuArrayDeviceProc,
    gpu_key = :cuda_gpu,
    elt = Float64,
    subarray_depmod = true,
    matmul = true,
    cholesky = true,
    stencil = true,
    sparse = true,
))
