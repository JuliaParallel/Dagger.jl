using KernelAbstractions, Adapt

const CPUProc = Union{OSProc, ThreadProc}

struct Kernel{F} end
Kernel(f) = Kernel{f}()

function (::Kernel{F})(args...; ndrange) where F
    @nospecialize args
    dev = kernel_backend()
    kern = F(dev)
    kern(args...; ndrange)
    KernelAbstractions.synchronize(dev)
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

gpu_processor(kind::Symbol) = gpu_processor(Val(kind))
gpu_processor(::Val) = ThreadProc
gpu_can_compute(kind::Symbol) = gpu_can_compute(Val(kind))
gpu_can_compute(::Val) = false
function gpu_with_device end

move_optimized(from_proc::Processor,
               to_proc::Processor,
               x) = nothing

kernel_backend() = kernel_backend(task_processor())
kernel_backend(::ThreadProc) = CPU()

gpu_synchronize(proc::Processor) = nothing
gpu_synchronize(kind::Symbol) = gpu_synchronize(Val(kind))
gpu_synchronize(::Val{:CPU}) = nothing