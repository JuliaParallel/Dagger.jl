# Focused CUDA smoke for GPU AMG / block PCs. Not part of runtests.jl.
using Test
using Random
using LinearAlgebra
using SparseArrays
using Distributed
using Krylov
using AlgebraicMultigrid
using CUDA
using Dagger

include(joinpath(@__DIR__, "array", "gpu_pc_defs.jl"))
include(joinpath(@__DIR__, "array", "gpu_amg_defs.jl"))

@assert pathof(Dagger) !== nothing
@info "Dagger" pathof(Dagger) functional=Dagger.gpu_can_compute(:CUDA)

Dagger.gpu_can_compute(:CUDA) || error("No CUDA device; cannot run GPU AMG smoke")

scope = Dagger.scope(worker=1, cuda_gpu=1)
CUDAExt = Base.get_extension(Dagger, :CUDAExt)
check_vec = chunk -> begin
    v = Dagger.MemPool.poolget(chunk.handle)
    return v isa CUDA.CuArray && chunk.space isa CUDAExt.CUDAVRAMMemorySpace
end
check_device_lu = F -> F isa LinearAlgebra.LU && F.factors isa CUDA.CuArray

@testset "CUDA GPU AMG / PC smoke" begin
    test_gpu_pc_apply(; scope, check_vec, check_device_lu, T=Float32)
    test_gpu_global_amg(; scope, check_vec, T=Float32)
end
