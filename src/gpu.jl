const CPUProc = Union{OSProc, ThreadProc}

"""
    Kernel{F}

A type that wraps a KernelAbstractions kernel function. Can be passed to
`Dagger.@spawn` to launch a kernel on a GPU. Synchronization is handled
automatically, but you can also call `Dagger.gpu_synchronize()` to synchronize
kernels manually.
"""
struct Kernel{F} end

Kernel(f) = Kernel{f}()

function (::Kernel{F})(args...; ndrange) where F
    @nospecialize args
    dev = gpu_kernel_backend()
    kern = F(dev)
    kern(args...; ndrange)
end

macro gpuproc(PROC, T)
    PROC = esc(PROC)
    T = esc(T)
    quote
        # Assume that we can run anything
        Dagger.iscompatible_func(proc::$PROC, opts, f) = true
        Dagger.iscompatible_arg(proc::$PROC, opts, x) = true

        # CPUs shouldn't process our array type
        Dagger.iscompatible_arg(proc::ThreadProc, opts, x::$T) = false
    end
end

"""
    gpu_processor(kind::Symbol)

Get the processor type for the given kind of GPU. Supported kinds are
`:CPU`, `:CUDA`, `:ROC`, `:oneAPI`, `:Metal`, and `:OpenCL`.
"""
gpu_processor(kind::Symbol) = gpu_processor(Val(kind))
gpu_processor(::Val{:CPU}) = ThreadProc

"""
    gpu_can_compute(kind::Symbol)

Check if the given kind of GPU is ready and usable. Supported kinds are
`:CPU`, `:CUDA`, `:ROC`, `:oneAPI`, `:Metal`, and `:OpenCL`.
"""
gpu_can_compute(kind::Symbol) = gpu_can_compute(Val(kind))
gpu_can_compute(::Val{:CPU}) = true
gpu_can_compute(::Val) = false

function gpu_with_device end

move_optimized(from_proc::Processor,
               to_proc::Processor,
               x) = nothing

"""
    gpu_kernel_backend()
    gpu_kernel_backend(proc::Processor)

Get the KernelAbstractions backend for the current processor.
"""
gpu_kernel_backend() = gpu_kernel_backend(task_processor())
gpu_kernel_backend(::ThreadProc) = KernelAbstractions.CPU()

"""
    gpu_synchronize(proc::Processor)

Synchronize all kernels launched by Dagger tasks on the given processor.
"""
gpu_synchronize(proc::Processor) = nothing

"""
    gpu_synchronize()

Synchronize all kernels launched by Dagger tasks in the current scope.
"""
function gpu_synchronize()
    for proc in Dagger.compatible_processors()
        gpu_synchronize(proc)
    end
end
"""
    gpu_synchronize(kind::Symbol)

Synchronize all kernels launched by Dagger tasks in the current scope for the
given processor kind. Alternatively, if `kind == :all`, synchronize all
kernels on all processors. Supported kinds are `:CPU`, `:CUDA`, `:ROC`,
`:oneAPI`, `:Metal`, and `:OpenCL`.
"""
function gpu_synchronize(kind::Symbol)
    if kind == :all
        for proc in Dagger.all_processors()
            gpu_synchronize(proc)
        end
    else
        gpu_synchronize(Val(kind))
    end
end
gpu_synchronize(::Val{:CPU}) = nothing

with_context!(proc::Processor) = nothing
with_context!(space::MemorySpace) = nothing