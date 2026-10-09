# Resolve GPU dependencies before loading packages or launching workers.
# Installing a backend can change shared packages such as LLVM and Atomix.
if USE_CUDA
    using Pkg
    Pkg.add("CUDA")
end
if USE_ROCM
    using Pkg
    Pkg.add(["AMDGPU", "AMDGPU_LLVM_Backend_jll"])
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

