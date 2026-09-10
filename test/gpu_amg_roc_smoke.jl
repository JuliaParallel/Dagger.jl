# Focused ROC smoke for GPU AMG / block PCs. Not part of runtests.jl.
#   JULIA_NUM_THREADS=2 julia --project=test/rocmenv test/gpu_amg_roc_smoke.jl

using Test
using Random
using LinearAlgebra
using SparseArrays
using Distributed
using Krylov
using AlgebraicMultigrid
using AMDGPU
using Dagger

include(joinpath(@__DIR__, "array", "gpu_pc_defs.jl"))
include(joinpath(@__DIR__, "array", "gpu_amg_defs.jl"))

@assert pathof(Dagger) !== nothing
@info "Dagger" pathof(Dagger) functional=Dagger.gpu_can_compute(:ROC)

Dagger.gpu_can_compute(:ROC) || error("No ROCm device; cannot run GPU AMG smoke")

scope = Dagger.scope(worker=1, rocm_gpu=1)
ROCExt = Base.get_extension(Dagger, :ROCExt)
check_vec = chunk -> begin
    v = Dagger.MemPool.poolget(chunk.handle)
    return v isa AMDGPU.ROCArray && chunk.space isa ROCExt.ROCVRAMMemorySpace
end
check_device_lu = F -> F isa LinearAlgebra.LU && F.factors isa AMDGPU.ROCArray

@testset "ROC GPU AMG / PC smoke" begin
    test_gpu_pc_apply(; scope, check_vec, check_device_lu, T=Float32)
    test_gpu_global_amg(; scope, check_vec, T=Float32)
end
