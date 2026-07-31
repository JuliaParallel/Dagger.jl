# GPU Support

Dagger supports GPU acceleration for CUDA, ROCm (AMD), Intel oneAPI, Metal (Apple), and OpenCL devices. GPU support enables automatic data movement between CPU and GPU memory, distributed GPU computing across multiple devices, and seamless integration with Julia's GPU ecosystem.

Dagger's GPU support is built on top of the [KernelAbstractions.jl](https://github.com/JuliaGPU/KernelAbstractions.jl) package, as well as the specific GPU-specific packages for each backend (e.g. [CUDA.jl](https://github.com/JuliaGPU/CUDA.jl), [AMDGPU.jl](https://github.com/JuliaGPU/AMDGPU.jl), [oneAPI.jl](https://github.com/JuliaGPU/oneAPI.jl), [Metal.jl](https://github.com/JuliaGPU/Metal.jl), and [OpenCL.jl](https://github.com/JuliaGPU/OpenCL.jl)). Dagger's GPU support is designed to be fully interoperable with the Julia GPU ecosystem, allowing you to use Dagger to distribute your GPU computations across multiple devices.

There are a few ways to use Dagger's GPU support:

1. **KernelAbstractions**: Use the `KernelAbstractions.jl` interface to write GPU kernels, and then use `Dagger.Kernel` and `Dagger.@spawn` to execute them.
2. **DArray**: Use the `DArray` interface to create distributed GPU arrays, and then call regular array operations on them, which will be automatically executed on the GPU.
3. **Datadeps**: Use the `Datadeps.jl` interface to create GPU-compatible algorithms, within which you can call kernels or array operations.
4. **Manual**: Use `Dagger.gpu_kernel_backend()` to get the appropriate backend for the current processor, and use that to execute kernels.

In all cases, you need to ensure that right GPU-specific package is loaded.

## Package Loading

Dagger's GPU support requires loading one of the following packages:

- [CUDA.jl](https://github.com/JuliaGPU/CUDA.jl) for NVIDIA GPUs
- [AMDGPU.jl](https://github.com/JuliaGPU/AMDGPU.jl) for AMD GPUs
- [oneAPI.jl](https://github.com/JuliaGPU/oneAPI.jl) for Intel GPUs
- [Metal.jl](https://github.com/JuliaGPU/Metal.jl) for Apple GPUs
- [OpenCL.jl](https://github.com/JuliaGPU/OpenCL.jl) for OpenCL devices

### Backend Detection

You can check if a given kind of GPU is supported by calling:

- CUDA: `Dagger.gpu_can_compute(:CUDA)`
- AMDGPU: `Dagger.gpu_can_compute(:ROC)`
- oneAPI: `Dagger.gpu_can_compute(:oneAPI)`
- Metal: `Dagger.gpu_can_compute(:Metal)`
- OpenCL: `Dagger.gpu_can_compute(:OpenCL)`

### Backend-Specific Scopes

Once you've loaded the appropriate package, you can create a scope for that
backend by calling:

```julia
# First GPU of different GPU types
cuda_scope = Dagger.scope(cuda_gpu=1)
rocm_scope = Dagger.scope(rocm_gpu=1)  
intel_scope = Dagger.scope(intel_gpu=1)
metal_scope = Dagger.scope(metal_gpu=1)
opencl_scope = Dagger.scope(cl_device=1)
```

These kinds of scopes can be passed to `Dagger.@spawn` or `Dagger.with_options`
to enable GPU acceleration on the given backend. Note that by default, Dagger
will not use any GPU if a compatible scope isn't provided through one of these
mechanisms.

## KernelAbstractions

The most direct way to use GPU acceleration in Dagger is through the KernelAbstractions.jl interface. Dagger provides seamless integration with KernelAbstractions, automatically selecting the appropriate backend for the current processor.

### Basic Kernel Usage

Write your kernels using the standard KernelAbstractions syntax:

```julia
using KernelAbstractions

@kernel function vector_add!(c, a, b)
    i = @index(Global, Linear)
    c[i] = a[i] + b[i]
end

@kernel function fill_kernel!(arr, value)
    i = @index(Global, Linear)
    arr[i] = value
end
```

### Using `Dagger.Kernel` for Automatic Backend Selection

`Dagger.Kernel` wraps your kernel functions and automatically selects the correct backend based on the current processor:

```julia
# Use in tasks - backend is selected automatically
cpu_array = Dagger.@mutable zeros(1000)
gpu_array = Dagger.@mutable CUDA.zeros(1000)

# Runs on CPU
fetch(Dagger.@spawn Dagger.Kernel(fill_kernel!)(cpu_array, 42.0; ndrange=length(cpu_array)))

# Runs on GPU when scoped appropriately
Dagger.with_options(;scope=Dagger.scope(cuda_gpu=1)) do
    fetch(Dagger.@spawn Dagger.Kernel(fill_kernel!)(gpu_array, 42.0; ndrange=length(gpu_array)))

    # Synchronize the GPU
    Dagger.gpu_synchronize(:CUDA)
end
```

Notice the usage of `Dagger.@mutable` to create mutable arrays on the GPU. This
is required when mutating arrays in-place with Dagger-launched kernels.

### Manual Backend Selection with `gpu_kernel_backend`

For more control, use `Dagger.gpu_kernel_backend()` to get the backend for the current processor:

```julia
function manual_kernel_execution(arr, value)
    # Get the backend for the current processor
    backend = Dagger.gpu_kernel_backend()

    # Create kernel with specific backend
    kernel = fill_kernel!(backend)

    # Execute kernel
    kernel(arr, value; ndrange=length(arr))

    return arr
end

# Use within a Dagger task
arr = Dagger.@mutable CUDA.zeros(1000)
Dagger.with_options(;scope=Dagger.scope(cuda_gpu=1)) do
    fetch(Dagger.@spawn manual_kernel_execution(arr, 42.0))

    Dagger.gpu_synchronize(:CUDA)
end
```

### Kernel Synchronization

Dagger handles synchronization automatically within Dagger tasks, but if you
mixed Dagger-launched and non-Dagger-launched kernels, you can synchronize the
GPU manually:

```julia
# Launch kernel as a task - Dagger.Kernel handles backend selection automatically
arr = Dagger.@mutable CUDA.zeros(1000)
Dagger.with_options(;scope=Dagger.scope(cuda_gpu=1)) do
    result = fetch(Dagger.@spawn Dagger.Kernel(fill_kernel!)(arr, 42.0; ndrange=length(arr)))

    # Synchronize kernels launched by Dagger tasks
    Dagger.gpu_synchronize()

    # Launch kernel as a task - Dagger.Kernel handles backend selection automatically
    fill_kernel(CUDABackend())(arr, 42.0; ndrange=length(arr))

    return result
end
```

## DArray: Distributed GPU Arrays

Dagger's `DArray` type seamlessly supports GPU acceleration, allowing you to
create distributed arrays that are automatically allocated in GPU memory when
using appropriate scopes.

### GPU Array Allocation

Allocate `DArray`s directly on GPU devices:

```julia
using CUDA  # or AMDGPU, oneAPI, Metal

# Single GPU allocation
gpu_scope = Dagger.scope(cuda_gpu=1)
Dagger.with_options(;scope=gpu_scope) do
    # All standard allocation functions work
    DA_rand = rand(Blocks(32, 32), Float32, 128, 128)
    DA_ones = ones(Blocks(32, 32), Float32, 128, 128)
    DA_zeros = zeros(Blocks(32, 32), Float32, 128, 128)
    DA_randn = randn(Blocks(32, 32), Float32, 128, 128)
end
```

### Multi-GPU Distribution

Distribute arrays across multiple GPUs:

```julia
# Use all available CUDA GPUs
all_gpu_scope = Dagger.scope(cuda_gpus=:)
Dagger.with_options(;scope=all_gpu_scope) do
    DA = rand(Blocks(64, 64), Float32, 256, 256)
    # Each chunk may be allocated on a different GPU
end

# Use specific GPUs
multi_gpu_scope = Dagger.scope(cuda_gpus=[1, 2, 3])
Dagger.with_options(;scope=multi_gpu_scope) do
    DA = ones(Blocks(32, 32), Float32, 192, 192)
end
```

### Converting Between CPU and GPU Arrays

Move existing arrays to GPU:

```julia
# Create CPU DArray
cpu_array = rand(Blocks(32, 32), 128, 128)

# Convert to GPU
gpu_scope = Dagger.scope(cuda_gpu=1)
Dagger.with_options(;scope=gpu_scope) do
    gpu_array = similar(cpu_array)
    # gpu_array now has the same structure but is allocated on GPU
end

# Convert back to CPU
cpu_result = collect(gpu_array)  # Brings all data back to CPU
```

### (Advanced) Verifying GPU Allocation

If necessary for testing or debugging, you can check that your `DArray` chunks
are actually living on the GPU:

```julia
gpu_scope = Dagger.scope(cuda_gpu=1)
Dagger.with_options(;scope=gpu_scope) do
    DA = rand(Blocks(4, 4), Float32, 8, 8)

    # Check each chunk
    for chunk in DA.chunks
        raw_chunk = fetch(chunk; raw=true)
        @assert raw_chunk isa Dagger.Chunk{<:CuArray}

        # Verify it's on the correct GPU device
        @assert remotecall_fetch(raw_chunk.handle.owner, raw_chunk) do chunk
            arr = Dagger.MemPool.poolget(chunk.handle)
            return CUDA.device(arr) == CUDA.devices()[1]  # GPU 1
        end
    end
end
```

## Datadeps: GPU-Compatible Algorithms

Datadeps regions work seamlessly with GPU arrays, enabling complex GPU
algorithms with automatic dependency management. Unlike without Datadeps, you
don't need to use `Dagger.@mutable`, as Datadeps ensures that array mutation is
performed correctly.

### In-Place GPU Operations

```julia
using LinearAlgebra

# Create GPU arrays
gpu_scope = Dagger.scope(cuda_gpu=1)
Dagger.with_options(;scope=gpu_scope) do
    DA = rand(Blocks(4, 4), Float32, 8, 8)
    DB = rand(Blocks(4, 4), Float32, 8, 8)
    DC = zeros(Blocks(4, 4), Float32, 8, 8)

    # In-place matrix multiplication on GPU
    Dagger.spawn_datadeps() do
        Dagger.@spawn mul!(Out(DC), In(DA), In(DB))
    end

    # Verify result
    @assert collect(DC) ≈ collect(DA) * collect(DB)
end
```

Notice that we didn't need to call `Dagger.gpu_synchronize()` here, because
the `DArray` is automatically synchronized when the `DArray` is collected.

### Out-of-Place GPU Operations

Because Dagger options propagate into function calls, you can call algorithms
that use Datadeps (such as `DArray` matrix multiplication) on GPUs, without
having to do any extra work:

```julia
gpu_scope = Dagger.scope(cuda_gpu=1)
Dagger.with_options(;scope=gpu_scope) do
    DA = rand(Blocks(4, 4), Float32, 8, 8)
    DB = rand(Blocks(4, 4), Float32, 8, 8)

    # Out-of-place operations
    DC = DA * DB  # Automatically runs on GPU

    @assert collect(DC) ≈ collect(DA) * collect(DB)
end
```

### Complex GPU Algorithms

```julia
using LinearAlgebra

gpu_scope = Dagger.scope(cuda_gpu=1)
Dagger.with_options(;scope=gpu_scope) do
    # Create a positive definite matrix for Cholesky decomposition
    A = rand(Float32, 8, 8)
    A = A * A'
    A[diagind(A)] .+= size(A, 1)
    DA = DArray(A, Blocks(4, 4))
    
    # Cholesky decomposition on GPU
    chol_result = cholesky(DA)
    @assert collect(chol_result.U) ≈ cholesky(collect(DA)).U
end
```

### Cross-Backend Operations

```julia
# You can even mix different GPU types in a single computation
# (though data movement between different GPU types goes through CPU)

cuda_data = Dagger.with_options(;scope=Dagger.scope(cuda_gpu=1)) do
    rand(Blocks(32, 32), Float32, 64, 64)
end

rocm_result = Dagger.with_options(;scope=Dagger.scope(rocm_gpu=1)) do
    # Data automatically moved: CUDA GPU -> CPU -> ROCm GPU
    fetch(Dagger.@spawn sum(cuda_data))
end
```

## Distributed GPU Computing

You can easily combine GPU acceleration with distributed computing across
multiple workers.

### Multi-Worker GPU Setup

```julia
using Distributed
addprocs(4)  # Add 4 workers

@everywhere using Dagger, CUDA

# Use GPU 1 on worker 2
distributed_gpu_scope = Dagger.scope(worker=2, cuda_gpu=1)

Dagger.with_options(;scope=distributed_gpu_scope) do
    # Create a GPU array and sum it on worker 2, GPU 1
    DA = rand(Blocks(32, 32), Float32, 128, 128)
    result = fetch(Dagger.@spawn sum(DA))
end
```

### Load Balancing Across GPUs and Workers

```julia
# Distribute work across multiple workers and their GPUs
workers_with_gpus = [
    Dagger.scope(worker=2, cuda_gpu=1),
    Dagger.scope(worker=3, cuda_gpu=1),
    Dagger.scope(worker=4, rocm_gpu=1)  # Mix of GPU types
]

# Dagger will automatically balance work across available resources
results = map(workers_with_gpus) do scope
    Dagger.with_options(;scope) do
        DA = rand(Blocks(16, 16), Float32, 64, 64)
        fetch(Dagger.@spawn sum(DA))
    end
end
```