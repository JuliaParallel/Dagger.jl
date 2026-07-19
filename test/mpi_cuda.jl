# MPI × GPU datadeps suite for CUDA. See test/mpi_gpu_suite.jl for the shared
# logic; this file only supplies the CUDA-specific config.
#
# Run (env must provide Dagger, MPI, CUDA; see test/cudaenv):
#   mpiexec -n 2 julia --project=test/cudaenv --threads=2 test/mpi_cuda.jl

using Dagger, MPI, CUDA, LinearAlgebra, Random, Test
using Dagger: In, Out, InOut, Deps

include(joinpath(@__DIR__, "mpi_gpu_suite.jl"))

const CUDAExt = Base.get_extension(Dagger, :CUDAExt)
@assert CUDAExt !== nothing "CUDAExt failed to load"

run_mpi_gpu_suite((;
    name = "CUDA",
    DeviceProc = CUDAExt.CuArrayDeviceProc,
    elt = Float64,
    subarray_depmod = true,
    matmul = true,
    cholesky = true,
))
