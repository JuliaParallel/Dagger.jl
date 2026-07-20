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

# Adapt RefValue
mutable struct GPURef{T,S<:MemorySpace} <: Ref{T}
    value::T
    space::S # This is ignored for aliasing
end
Base.getindex(x::GPURef) = x.value
Base.setindex!(x::GPURef, value) = x.value = value
# FIXME: Wire up with adapt
function aliasing(x::GPURef)
    addr = UInt(Base.pointer_from_objref(x) + fieldoffset(typeof(x), 1))
    ptr = RemotePtr{Cvoid}(addr, x.space)
    ainfo = ObjectAliasing(ptr, sizeof(x.value))
    return CombinedAliasing([ainfo])
end
memory_space(x::GPURef) = x.space
function read_remainder!(copies::Vector{UInt8}, copies_offset::UInt64, from::GPURef, from_ptr::UInt64, n::UInt64)
    if from_ptr == UInt64(Base.pointer_from_objref(from) + fieldoffset(typeof(from), 1))
        unsafe_copyto!(pointer(copies, copies_offset), Ptr{UInt8}(from_ptr), n)
    else
        read_remainder!(copies, copies_offset, from[], from_ptr, n)
    end
end
function write_remainder!(copies::Vector{UInt8}, copies_offset::UInt64, to::GPURef, to_ptr::UInt64, n::UInt64)
    if to_ptr == UInt64(Base.pointer_from_objref(to) + fieldoffset(typeof(to), 1))
        unsafe_copyto!(Ptr{UInt8}(to_ptr), pointer(copies, copies_offset), n)
    else
        write_remainder!(copies, copies_offset, to[], to_ptr, n)
    end
end
"""
    proc_concurrency(proc::Processor) -> Int

Number of tasks a processor can execute concurrently.

Defaults to `1`: one Dagger processor maps to one execution unit (e.g. a
`ThreadProc` is a single Julia thread running one task at a time). GPU
processor types whose backends multiplex several independent streams over one
device override this with their stream count, so cost-model schedulers account
for K-way in-processor parallelism when computing earliest-finish-time.

This matters because EFT with an implicit capacity of 1 charges each queued
task the full serial `proc_ready + task_time`, which on a multi-stream GPU
overstates completion time by up to a factor of K and distorts placement
decisions relative to single-slot CPU processors. Capacity-aware processor
allocation is the model assumed by the task-scheduling literature these
schedulers implement (Sinnen & Sousa 2005, §4.3).

See `_slots_for` in `src/datadeps/scheduling.jl` for the K-slot readiness
tracking that consumes this.
"""
proc_concurrency(::Processor) = 1

"""
    _translate_fn_for(::Type{F}, proc::Processor) -> Type or nothing

Map a CPU-side function type to the accelerator-side function type that will
actually execute on `proc`. Returns `nothing` when no mapping exists.

Generated in lockstep with the `Dagger.move(::CPUProc, ::GPUProc, ::typeof(fn))`
overloads in the vendor extensions -- the two are inherently paired, since that
`move` is precisely what performs the substitution at dispatch time.
"""
_translate_fn_for(::Type, ::Processor) = nothing

"""
    _translate_sig_for(sig::Vector, proc::Processor) -> Vector or nothing

Rewrite a CPU-side metrics signature into the form that `proc` would record
after dispatch, or `nothing` if any element has no mapping.

Needed because `MetricsTracker` keys runtime samples on the *post-dispatch*
signature: a task submitted as `BLAS.gemm!(::Matrix{Float64}, ...)` is recorded
on a GPU as `CUBLAS.gemm!(::CuArray{Float64,2,DeviceMemory}, ...)`. The
scheduler only ever sees the pre-dispatch form, so without translation no GPU
sample is reachable and `_runtime_lookup_chain` falls through to its
processor-agnostic tier, handing back a CPU runtime for a GPU.

This is a stopgap. The general fix is a processor-independent task identity
recorded alongside the physical signature, which would make translation
unnecessary for every backend at once.
"""
_translate_sig_for(::Vector, ::Processor) = nothing
