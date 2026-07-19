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

# Backend kind for an array or memory space (`:CPU`, `:CUDA`, `:ROC`, …).
# GPU extensions override for their array / VRAM space types.
gpu_memory_kind(x) = :CPU
gpu_memory_kind(::MemorySpace) = :CPU

# Page-lock a host buffer for faster DtoH/HtoD (GPU extensions override per Val).
# Unpinning happens via the buffer's GC finalizer. Used by MPI and Distributed.
pin_buffer!(kind::Symbol, buf) = pin_buffer!(Val(kind), buf)
pin_buffer!(::Val, buf) = nothing

### Host staging buffer pools (per GPU backend kind)
# Staging device payloads through fresh pageable allocations is slow and
# GC-heavy; transfers reuse pooled host buffers that GPU extensions
# page-lock (pinned memory roughly doubles DtoH/HtoD bandwidth).
# Pools are keyed by `gpu_memory_kind` (`:CPU`/`:CUDA`/`:ROC`, …) so
# differently pinned buffers never share a free-list.
const STAGE_POOLS = LockedObject(Dict{Symbol, Dict{Int,Vector{Vector{UInt8}}}}())
const STAGE_POOL_MAX_BYTES = Ref(256 * 1024 * 1024)
const STAGE_POOL_BYTES = Threads.Atomic{Int}(0)

function stage_acquire!(kind::Symbol, nbytes::Integer)
    cls = nextpow(2, max(Int(nbytes), 4096))
    buf = lock(STAGE_POOLS) do pools
        pool = get!(Dict{Int,Vector{Vector{UInt8}}}, pools, kind)
        bufs = get(pool, cls, nothing)
        (bufs === nothing || isempty(bufs)) ? nothing : pop!(bufs)
    end
    if buf === nothing
        buf = Vector{UInt8}(undef, cls)
        pin_buffer!(kind, buf)
    else
        Threads.atomic_sub!(STAGE_POOL_BYTES, cls)
    end
    return buf
end
function stage_release!(kind::Symbol, buf::Vector{UInt8})
    cls = length(buf)
    if STAGE_POOL_BYTES[] + cls > STAGE_POOL_MAX_BYTES[]
        return # drop it; GC unpins via the registration finalizer
    end
    Threads.atomic_add!(STAGE_POOL_BYTES, cls)
    lock(STAGE_POOLS) do pools
        pool = get!(Dict{Int,Vector{Vector{UInt8}}}, pools, kind)
        push!(get!(Vector{Vector{UInt8}}, pool, cls), buf)
    end
    return
end

# Copy a dense device payload into a pooled host buffer; returns
# (host_view, buf, kind) — keep `buf` rooted while using the view, then release
function stage_to_host!(value::DenseArray{T}) where T
    kind = gpu_memory_kind(value)
    buf = stage_acquire!(kind, sizeof(value))
    host = unsafe_wrap(Array, Ptr{T}(pointer(buf)), size(value))
    with_context!(memory_space(value))
    copyto!(host, value)
    return host, buf, kind
end

# Owned host Array for DtoH (Distributed remotecall / return paths): pin then copy
function pinned_host_array(value::DenseArray{T}) where T
    kind = gpu_memory_kind(value)
    host = Array{T}(undef, size(value))
    pin_buffer!(kind, host)
    with_context!(memory_space(value))
    copyto!(host, value)
    return host
end

### Same-node device IPC
# When both endpoints are device memory on the same node, the payload can stay
# on-device: the sender stages into an IPC-exportable allocation and ships only
# a small handle; the receiver maps it and copies device-to-device.
# GPU extensions override eligibility and export/import. Transport of the
# handle (MPI P2P vs Distributed remotecall) is acceleration-specific.
# DAGGER_IPC=0 disables the path.
ipc_eligible(from_inner::MemorySpace, to_inner::MemorySpace) = false
# Transfers smaller than this stay on the staged path (handle exchange and
# ack latency dominate below the crossover, ~128KiB on PCIe-attached GPUs)
const IPC_MIN_BYTES = Ref{Int}(128 * 1024)
# Export `value`: returns `(info, token)` where `info` is a small serializable
# description and `token` keeps the staging allocation alive until release
ipc_export(value) = error("No IPC export implementation for $(typeof(value))")
ipc_release!(token) = nothing
# Copy the exported data into `dest` (receiver side)
ipc_copyto!(dest, info) = error("No IPC import implementation for $(typeof(dest))")
# Materialize a fresh device array from the exported data (receiver side)
ipc_materialize(info) = error("No IPC materialize implementation for $(typeof(info))")

# MPI/GPU interop hooks. GPU package extensions (which cannot depend on MPI)
# specialize these per array/space type, and MPIExt refines `mpi_library_gpu_aware`.
# Declared in core so both extension families extend the same Dagger generics
# (and so GPU extensions load even when MPIExt isn't present).
#
# Can `value` be handed to MPI directly as a device buffer? (needs GPU-aware MPI)
mpi_device_direct(value) = false
# Make pending device work on `value`/`space` visible to the host/MPI
mpi_device_sync(value) = nothing
mpi_device_sync(space::MemorySpace) = nothing
# Remap rank-local memory space stamps to the owning rank so spans from
# different ranks never falsely alias. GPU extensions stamp their VRAM spaces.
mpi_remap_space(space::CPURAMMemorySpace, owner::Int) = CPURAMMemorySpace(owner)
mpi_remap_space(space::MemorySpace, owner::Int) =
    throw(ArgumentError("mpi_remap_space not defined for $(typeof(space)); backends must stamp the owning MPI rank"))
# Whether the loaded MPI library supports device buffers of `kind`. Only invoked
# under MPI acceleration; MPIExt provides the method (queries the MPI library).
function mpi_library_gpu_aware end

# Distributed: same node iff workers share a system_uuid (CUDAExt IPC gate)
same_node(::DistributedAcceleration, w1::Integer, w2::Integer) =
    system_uuid(Int(w1)) == system_uuid(Int(w2))

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