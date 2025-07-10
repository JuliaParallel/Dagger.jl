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
if USE_OPENCL
    using Pkg
    Pkg.add("OpenCL")
    Pkg.add("pocl_jll")
end

@everywhere begin
    if $USE_CUDA
        using CUDA
    elseif !$IN_CI
        try using CUDA
        catch end
    end

    if $USE_ROCM
        using AMDGPU
    elseif !$IN_CI
        try using AMDGPU
        catch end
    end

    if $USE_ONEAPI
        using oneAPI
    elseif !$IN_CI
        try using oneAPI
        catch end
    end

    if $USE_METAL
        using Metal
    elseif !$IN_CI
        try using Metal
        catch end
    end

    if $USE_OPENCL
        using pocl_jll, OpenCL
    elseif !$IN_CI
        try using pocl_jll, OpenCL
        catch end
    end
end

if USE_CUDA
    push!(GPU_SCOPES, (:CUDA, CuArray, Dagger.scope(;worker=1, cuda_gpu=1)))
    if length(CUDA.devices()) > 1
        push!(GPU_SCOPES, (:CUDA, CuArray, Dagger.scope(;worker=1, cuda_gpu=2)))
    end
end
if USE_ROCM
    push!(GPU_SCOPES, (:ROCm, ROCArray, Dagger.scope(;worker=1, rocm_gpu=1)))
    if length(AMDGPU.devices()) > 1
        push!(GPU_SCOPES, (:ROCm, ROCmrray, Dagger.scope(;worker=1, rocm_gpu=2)))
    end
end
if USE_ONEAPI
    push!(GPU_SCOPES, (:oneAPI, oneArray, Dagger.scope(;worker=1, intel_gpu=1)))
    if length(oneAPI.devices()) > 1
        push!(GPU_SCOPES, (:oneAPI, oneArray, Dagger.scope(;worker=1, intel_gpu=2)))
    end
end
if USE_METAL
    push!(GPU_SCOPES, (:Metal, MtlArray, Dagger.scope(;worker=1, metal_gpu=1)))
    if length(Metal.devices()) > 1
        push!(GPU_SCOPES, (:Metal, MtlArray, Dagger.scope(;worker=1, metal_gpu=2)))
    end
end
if USE_OPENCL
    push!(GPU_SCOPES, (:OpenCL, CLArray, Dagger.scope(;worker=1, cl_device=1)))
    if length(cl.devices(cl.default_platform())) > 1
        push!(GPU_SCOPES, (:OpenCL, CLArray, Dagger.scope(;worker=1, cl_device=2)))
    end
end
