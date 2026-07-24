# MPI × GPU datadeps suite for oneAPI. See test/mpi_gpu_suite.jl for the shared
# logic; this file only supplies the oneAPI-specific config.
#
# GPU-direct requires Intel MPI + I_MPI_OFFLOAD or DAGGER_MPI_GPU_DIRECT=1;
# otherwise the data plane is host-staged through P2P.
#
# Run (env must provide Dagger, MPI, oneAPI; see test/oneapienv):
#   mpiexec -n 2 julia --project=test/oneapienv --threads=2 test/mpi_oneapi.jl

using Dagger, MPI, oneAPI, LinearAlgebra, Random, Test
using Dagger: In, Out, InOut, Deps

include(joinpath(@__DIR__, "mpi_gpu_suite.jl"))

const IntelExt = Base.get_extension(Dagger, :IntelExt)
@assert IntelExt !== nothing "IntelExt failed to load"

run_mpi_gpu_suite((;
    name = "oneAPI",
    DeviceProc = IntelExt.oneArrayDeviceProc,
    elt = Float32,
    matmul = true,
    remap = (;
        make_space = () -> IntelExt.IntelVRAMMemorySpace(1, 1),
        device_field = :device_id,
        kind = :oneAPI,
    ),
))
