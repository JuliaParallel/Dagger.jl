# MPI × GPU datadeps suite for Metal. See test/mpi_gpu_suite.jl for the shared
# logic; this file only supplies the Metal-specific config.
#
# Run (macOS; env must provide Dagger, MPI, Metal; see test/metalenv):
#   mpiexec -n 2 julia --project=test/metalenv --threads=2 test/mpi_metal.jl

using Dagger, MPI, Metal, LinearAlgebra, Random, Test
using Dagger: In, Out, InOut, Deps

using Distributed

include(joinpath(@__DIR__, "mpi_gpu_suite.jl"))

const MetalExt = Base.get_extension(Dagger, :MetalExt)
@assert MetalExt !== nothing "MetalExt failed to load"

run_mpi_gpu_suite((;
    name = "Metal",
    DeviceProc = Dagger.MtlArrayDeviceProc,
    gpu_key = :metal_gpu,
    elt = Float32,
    stencil = true,
    # The 3D/4D stencil tests break the Metal backend and corrupt subsequent
    # tests (mirrors the single-process skip in test/array/stencil.jl).
    stencil_skip_highdim = true,
    sparse = true,
    remap = (;
        make_space = () -> MetalExt.MetalVRAMMemorySpace(1, 1),
        device_field = :device_id,
        kind = :Metal,
        test_ipc = true,
    ),
))
