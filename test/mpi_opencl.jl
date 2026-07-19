# MPI × GPU datadeps suite for OpenCL. See test/mpi_gpu_suite.jl for the shared
# logic; this file only supplies the OpenCL-specific config.
#
# Run (env must provide Dagger, MPI, OpenCL; see test/openclenv):
#   mpiexec -n 2 julia --project=test/openclenv --threads=2 test/mpi_opencl.jl

using Dagger, MPI, OpenCL, LinearAlgebra, Random, Test
using Dagger: In, Out, InOut, Deps

include(joinpath(@__DIR__, "mpi_gpu_suite.jl"))

const OpenCLExt = Base.get_extension(Dagger, :OpenCLExt)
@assert OpenCLExt !== nothing "OpenCLExt failed to load"

run_mpi_gpu_suite((;
    name = "OpenCL",
    DeviceProc = OpenCLExt.CLArrayDeviceProc,
    elt = Float32,
    remap = (;
        make_space = () -> OpenCLExt.CLMemorySpace(1, 1),
        device_field = :device,
        kind = :OpenCL,
        test_ipc = true,
    ),
))
