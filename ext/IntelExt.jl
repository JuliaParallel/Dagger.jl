module IntelExt

export oneArrayDeviceProc

import Dagger, MemPool
import Dagger: CPURAMMemorySpace, Chunk, unwrap
import MemPool: DRef, poolget
import Distributed: myid, remotecall_fetch
import LinearAlgebra
using KernelAbstractions, Adapt

const CPUProc = Union{Dagger.OSProc,Dagger.ThreadProc}

if isdefined(Base, :get_extension)
    import oneAPI
else
    import ..oneAPI
end
import oneAPI: ZeDevice, ZeDriver, ZeContext, oneArray, oneAPIBackend
import oneAPI: driver, driver!, device, device!, context, context!
#import oneAPI: CUBLAS, CUSOLVER

using UUIDs

"Represents a single Intel GPU device."
struct oneArrayDeviceProc <: Dagger.Processor
    owner::Int
    device_id::Int
end
Dagger.get_parent(proc::oneArrayDeviceProc) = Dagger.OSProc(proc.owner)
Dagger.root_worker_id(proc::oneArrayDeviceProc) = proc.owner
Base.show(io::IO, proc::oneArrayDeviceProc) =
    print(io, "oneArrayDeviceProc(worker $(proc.owner), device $(proc.device_id))")
Dagger.short_name(proc::oneArrayDeviceProc) = "W: $(proc.owner), oneAPI: $(proc.device_id)"
Dagger.@gpuproc(oneArrayDeviceProc, oneArray)

"Represents the memory space of a single Intel GPU's VRAM."
struct IntelVRAMMemorySpace <: Dagger.MemorySpace
    owner::Int
    device_id::Int
end
Dagger.root_worker_id(space::IntelVRAMMemorySpace) = space.owner
function Dagger.memory_space(x::oneArray)
    dev = oneAPI.device(x)
    device_id = _device_id(dev)
    return IntelVRAMMemorySpace(myid(), device_id)
end
_device_id(dev::ZeDevice) = findfirst(other_dev->other_dev === dev, collect(oneAPI.devices()))
_device_id(x::oneArray) = _device_id(oneAPI.device(x))
function Dagger.aliasing(x::oneArray{T}) where T
    space = Dagger.memory_space(x)
    S = typeof(space)
    gpu_ptr = pointer(x)
    rptr = Dagger.RemotePtr{Cvoid}(UInt64(gpu_ptr), space)
    return Dagger.ContiguousAliasing(Dagger.MemorySpan{S}(rptr, sizeof(T)*length(x)))
end

# MPI (SPMD) integration: stamp owning rank on VRAM spaces
Dagger.mpi_remap_space(space::IntelVRAMMemorySpace, owner::Int) =
    IntelVRAMMemorySpace(owner, space.device_id)
Dagger.value_memory_space(x::oneArray) = Dagger.memory_space(x)

# Staging-pool key; Level Zero has no CUDA-style pin of arbitrary host buffers
Dagger.gpu_memory_kind(::oneArray) = :oneAPI
Dagger.gpu_memory_kind(::IntelVRAMMemorySpace) = :oneAPI
Dagger.pin_buffer!(::Val{:oneAPI}, buf::DenseArray) = nothing

function Dagger.unsafe_free!(x::oneArray)
    oneAPI.unsafe_free!(x)
    return
end

Dagger.memory_spaces(proc::oneArrayDeviceProc) = Set([IntelVRAMMemorySpace(proc.owner, proc.device_id)])
Dagger.processors(space::IntelVRAMMemorySpace) = Set([oneArrayDeviceProc(space.owner, space.device_id)])

function to_device(proc::oneArrayDeviceProc)
    @assert Dagger.root_worker_id(proc) == myid()
    return DEVICES[proc.device_id]
end

function with_context!(device_id::Integer)
    driver!(DRIVERS[device_id])
    device!(DEVICES[device_id])
    context!(_get_context(DRIVERS[device_id]))
end
function with_context!(proc::oneArrayDeviceProc)
    @assert Dagger.root_worker_id(proc) == myid()
    with_context!(proc.device_id)
end
function with_context!(space::IntelVRAMMemorySpace)
    @assert Dagger.root_worker_id(space) == myid()
    with_context!(space.device_id)
end
Dagger.with_context!(proc::oneArrayDeviceProc) = with_context!(proc)
Dagger.with_context!(space::IntelVRAMMemorySpace) = with_context!(space)
function with_context(f, x)
    old_drv = driver()
    old_dev = device()
    old_ctx = context()

    with_context!(x)
    try
        f()
    finally
        driver!(old_drv)
        device!(old_dev)
        context!(old_ctx)
    end
end

function _ensure_device(x::oneArray, device_id::Int)
    dev = DEVICES[device_id]
    if oneAPI.device(x) === dev
        return x
    end
    src_dev_id = _device_id(x)
    @assert src_dev_id !== nothing "Unknown source device for oneArray"
    # Ensure any pending work on the source device is complete before DtoD copy
    # N.B. oneAPI does not synchronize the source in `copyto!`, making this necessary
    with_context(src_dev_id) do
        oneAPI.synchronize()
    end
    return with_context(device_id) do
        arr = similar(x)
        copyto!(arr, x)  # direct DtoD within shared context
        oneAPI.synchronize()
        return arr
    end
end

function _sync_with_context(x::Union{Dagger.Processor,Dagger.MemorySpace})
    with_context(x) do
        oneAPI.synchronize()
    end
end
function sync_with_context(x::Union{Dagger.Processor,Dagger.MemorySpace})
    if Dagger.root_worker_id(x) == myid()
        _sync_with_context(x)
    else
        # Do nothing, as we have received our value over a serialization
        # boundary, which should synchronize for us
    end
end

# Allocations
Dagger.allocate_array_func(::oneArrayDeviceProc, ::typeof(rand)) = oneAPI.rand
Dagger.allocate_array_func(::oneArrayDeviceProc, ::typeof(randn)) = oneAPI.randn
Dagger.allocate_array_func(::oneArrayDeviceProc, ::typeof(ones)) = oneAPI.ones
Dagger.allocate_array_func(::oneArrayDeviceProc, ::typeof(zeros)) = oneAPI.zeros
struct AllocateUndef{S} end
(::AllocateUndef{S})(T, dims::Dims{N}) where {S,N} = oneArray{S,N}(undef, dims)
Dagger.allocate_array_func(::oneArrayDeviceProc, ::Dagger.AllocateUndef{S}) where S = AllocateUndef{S}()

# In-place
# N.B. These methods assume that later operations will implicitly or
# explicitly synchronize with their associated stream
function Dagger.move!(to_space::Dagger.CPURAMMemorySpace, from_space::IntelVRAMMemorySpace, to::AbstractArray{T,N}, from::AbstractArray{T,N}) where {T,N}
    if Dagger.root_worker_id(from_space) == myid()
        sync_with_context(from_space)
        with_context!(from_space)
    end
    copyto!(to, from)
    # N.B. DtoH will synchronize
    return
end
function Dagger.move!(to_space::IntelVRAMMemorySpace, from_space::Dagger.CPURAMMemorySpace, to::AbstractArray{T,N}, from::AbstractArray{T,N}) where {T,N}
    with_context!(to_space)
    copyto!(to, from)
    return
end
function Dagger.move!(to_space::IntelVRAMMemorySpace, from_space::IntelVRAMMemorySpace, to::AbstractArray{T,N}, from::AbstractArray{T,N}) where {T,N}
    sync_with_context(from_space)
    with_context!(to_space)
    if Dagger.needs_multi_span_copy(to_space, from_space, to, from)
        Dagger.multi_span_move!(to, from)
    else
        copyto!(to, from)
    end
    return
end

# Out-of-place HtoD
function Dagger.move(from_proc::CPUProc, to_proc::oneArrayDeviceProc, x)
    with_context(to_proc) do
        if x isa DenseArray && isbitstype(eltype(x))
            Dagger.pin_buffer!(:oneAPI, x)
        end
        arr = adapt(oneArray, x)
        oneAPI.synchronize()
        return arr
    end
end
function Dagger.move(from_proc::CPUProc, to_proc::oneArrayDeviceProc, x::Chunk)
    from_w = Dagger.root_worker_id(from_proc)
    to_w = Dagger.root_worker_id(to_proc)
    @assert myid() == to_w
    cpu_data = remotecall_fetch(unwrap, from_w, x)
    with_context(to_proc) do
        if cpu_data isa DenseArray && isbitstype(eltype(cpu_data))
            Dagger.pin_buffer!(:oneAPI, cpu_data)
        end
        arr = adapt(oneArray, cpu_data)
        oneAPI.synchronize()
        return arr
    end
end
function Dagger.move(from_proc::CPUProc, to_proc::oneArrayDeviceProc, x::oneArray)
    return _ensure_device(x, to_proc.device_id)
end

# Out-of-place DtoH
function Dagger.move(from_proc::oneArrayDeviceProc, to_proc::CPUProc, x)
    with_context(from_proc) do
        oneAPI.synchronize()
        _x = x isa DenseArray && isbitstype(eltype(x)) ?
             Dagger.pinned_host_array(x) : adapt(Array, x)
        oneAPI.synchronize()
        return _x
    end
end
function Dagger.move(from_proc::oneArrayDeviceProc, to_proc::CPUProc, x::Chunk)
    from_w = Dagger.root_worker_id(from_proc)
    to_w = Dagger.root_worker_id(to_proc)
    @assert myid() == to_w
    remotecall_fetch(from_w, x) do x
        arr = unwrap(x)
        return Dagger.move(from_proc, to_proc, arr)
    end
end
function Dagger.move(from_proc::oneArrayDeviceProc, to_proc::CPUProc, x::oneArray{T,N}) where {T,N}
    with_context(from_proc) do
        oneAPI.synchronize()
        _x = Dagger.pinned_host_array(x)
        oneAPI.synchronize()
        return _x
    end
end

# Out-of-place DtoD
function Dagger.move(from_proc::oneArrayDeviceProc, to_proc::oneArrayDeviceProc, x::Dagger.Chunk{T}) where T<:oneArray
    if from_proc == to_proc
        # Same process and GPU, no change
        arr = unwrap(x)
        with_context(oneAPI.synchronize, from_proc)
        return arr
    elseif Dagger.root_worker_id(from_proc) == Dagger.root_worker_id(to_proc)
        # Same process but different GPUs, rehome to target device
        from_arr = unwrap(x)
        with_context(oneAPI.synchronize, from_proc)
        return _ensure_device(from_arr, to_proc.device_id)
    else
        # Different node, use DtoH, serialization, HtoD (pinned host staging)
        host_copy = remotecall_fetch(from_proc.owner, from_proc, x) do from_proc, x
            return with_context(from_proc) do
                Dagger.pinned_host_array(unwrap(x))
            end
        end
        return with_context(to_proc) do
            Dagger.pin_buffer!(:oneAPI, host_copy)
            return oneArray(host_copy)
        end
    end
end

function Dagger.move(from_proc::oneArrayDeviceProc, to_proc::oneArrayDeviceProc, x::oneArray)
    if from_proc == to_proc
        with_context(oneAPI.synchronize, from_proc)
        return x
    elseif Dagger.root_worker_id(from_proc) == Dagger.root_worker_id(to_proc)
        return _ensure_device(x, to_proc.device_id)
    else
        host_copy = with_context(from_proc) do
            return Dagger.pinned_host_array(x)
        end
        return with_context(to_proc) do
            Dagger.pin_buffer!(:oneAPI, host_copy)
            return oneArray(host_copy)
        end
    end
end

# Adapt generic functions
Dagger.move(from_proc::CPUProc, to_proc::oneArrayDeviceProc, x::Function) = x
Dagger.move(from_proc::CPUProc, to_proc::oneArrayDeviceProc, x::Chunk{T}) where {T<:Function} =
    Dagger.move(from_proc, to_proc, fetch(x))

# Adapt BLAS/LAPACK functions (same pattern as CUDAExt/ROCExt)
import LinearAlgebra: BLAS, LAPACK
_keep_blas_functions = Set(["iamax"])
for lib in [BLAS, LAPACK]
    for name in names(lib; all=true)
        name == nameof(lib) && continue
        startswith(string(name), '#') && continue
        if !endswith(string(name), '!') && !(string(name) in _keep_blas_functions)
            continue
        end
        if name in names(oneAPI.oneMKL; all=true)
            fn = getproperty(lib, name)
            mklfn = getproperty(oneAPI.oneMKL, name)
            @eval Dagger.move(from_proc::CPUProc, to_proc::oneArrayDeviceProc, ::$(typeof(fn))) = $mklfn
        end
    end
end

# Task execution
function Dagger.execute!(proc::oneArrayDeviceProc, f, args...; kwargs...)
    @nospecialize f args kwargs
    tls = Dagger.get_tls()
    task = Threads.@spawn begin
        Dagger.set_tls!(tls)
        with_context!(proc)
        result = Base.@invokelatest f(args...; kwargs...)
        # N.B. Synchronization must be done when accessing result or args
        return result
    end

    try
        fetch(task)
    catch err
        stk = current_exceptions(task)
        err, frames = stk[1]
        rethrow(CapturedException(err, frames))
    end
end

# Adapt RefValue
Dagger.move(from_proc::CPUProc, to_proc::oneArrayDeviceProc, x::Base.RefValue) =
    Dagger.GPURef(Dagger.move(from_proc, to_proc, x[]), only(Dagger.memory_spaces(to_proc)))
Dagger.move(from_proc::oneArrayDeviceProc, to_proc::CPUProc, x::Dagger.GPURef{T,IntelVRAMMemorySpace} where T) =
    Ref(Dagger.move(from_proc, to_proc, x[]))
function Dagger.move!(dep_mod, to_space::CPURAMMemorySpace, from_space::IntelVRAMMemorySpace, to::Base.RefValue, from::Dagger.GPURef)
    if Dagger.type_may_alias(typeof(from[]))
        Dagger.move!(dep_mod, to_space, from_space, to[], from[])
    else
        to[] = dep_mod(from[])
    end
    return
end
function Dagger.move!(dep_mod, to_space::IntelVRAMMemorySpace, from_space::CPURAMMemorySpace, to::Dagger.GPURef, from::Base.RefValue)
    if Dagger.type_may_alias(typeof(from[]))
        Dagger.move!(dep_mod, to_space, from_space, to[], from[])
    else
        to[] = dep_mod(from[])
    end
    return
end
function Dagger.move!(dep_mod, to_space::IntelVRAMMemorySpace, from_space::IntelVRAMMemorySpace, to::Dagger.GPURef, from::Dagger.GPURef)
    if Dagger.type_may_alias(typeof(from[]))
        Dagger.move!(dep_mod, to_space, from_space, to[], from[])
    else
        to[] = dep_mod(from[])
    end
    return
end

# Adapt HaloArray
oneArray(H::Dagger.HaloArray) = convert(oneArray, H)
Base.convert(::Type{C}, H::Dagger.HaloArray) where {C<:oneArray} =
    Dagger.HaloArray(C(H.center),
                     C.(H.halos),
                     H.halo_width;
                     own_center=H.own_center)
Adapt.adapt_structure(to::oneAPI.KernelAdaptor, H::Dagger.HaloArray) =
    Dagger.HaloArray(adapt(to, H.center),
                     adapt.(Ref(to), H.halos),
                     H.halo_width;
                     own_center=H.own_center)
function Dagger.inner_stencil_proc!(::oneArrayDeviceProc, f, output, read_vars)
    Dagger.Kernel(_inner_stencil!)(f, output, read_vars; ndrange=size(output))
    return
end
@kernel function _inner_stencil!(f, output, read_vars)
    idx = @index(Global, Cartesian)
    f(idx, output, read_vars)
end

Dagger.gpu_processor(::Val{:oneAPI}) = oneArrayDeviceProc
Dagger.gpu_can_compute(::Val{:oneAPI}) = oneAPI.functional()
Dagger.gpu_kernel_backend(::oneArrayDeviceProc) = oneAPIBackend()
Dagger.gpu_with_device(f, proc::oneArrayDeviceProc) =
    device!(f, proc.device_id)
function Dagger.gpu_synchronize(proc::oneArrayDeviceProc)
    with_context(proc) do
        oneAPI.synchronize()
    end
end
function Dagger.gpu_synchronize(::Val{:oneAPI})
    for dev in oneAPI.devices()
        idx = findfirst(other_dev->other_dev == dev, collect(oneAPI.devices()))
        _sync_with_context(oneArrayDeviceProc(myid(), idx))
    end
end

Dagger.to_scope(::Val{:intel_gpu}, sc::NamedTuple) =
    Dagger.to_scope(Val{:intel_gpus}(), merge(sc, (;intel_gpus=[sc.intel_gpu])))
Dagger.scope_key_precedence(::Val{:intel_gpu}) = 1
function Dagger.to_scope(::Val{:intel_gpus}, sc::NamedTuple)
    if haskey(sc, :worker)
        workers = Int[sc.worker]
    elseif haskey(sc, :workers) && sc.workers != Colon()
        workers = sc.workers
    else
        workers = map(gproc->gproc.pid, Dagger.procs(Dagger.Sch.eager_context()))
    end
    scopes = Dagger.ExactScope[]
    dev_ids = sc.intel_gpus
    for worker in workers
        procs = Dagger.get_processors(Dagger.OSProc(worker))
        for proc in procs
            proc isa oneArrayDeviceProc || continue
            if dev_ids == Colon() || proc.device_id in dev_ids
                scope = Dagger.ExactScope(proc)
                push!(scopes, scope)
            end
        end
    end
    return Dagger.UnionScope(scopes)
end
Dagger.scope_key_precedence(::Val{:intel_gpus}) = 1

# MPI data plane: pass oneArrays to MPI directly when the library is Level Zero /
# Intel-MPI aware (DAGGER_MPI_GPU_DIRECT=0/1 forces; else mpi_library_gpu_aware)
const MPI_GPU_DIRECT = Ref{Union{Nothing,Bool}}(nothing)
function mpi_gpu_direct_enabled()
    v = MPI_GPU_DIRECT[]
    v !== nothing && return v
    env = get(ENV, "DAGGER_MPI_GPU_DIRECT", "")
    v = if !isempty(env)
        something(tryparse(Bool, env), false)
    else
        Dagger.mpi_library_gpu_aware(:oneAPI)
    end
    MPI_GPU_DIRECT[] = v
    return v
end
Dagger.mpi_device_direct(x::oneAPI.oneStridedArray) = mpi_gpu_direct_enabled()
Dagger.mpi_device_sync(x::oneArray) = oneAPI.synchronize()
Dagger.mpi_device_sync(::IntelVRAMMemorySpace) = oneAPI.synchronize()

# Same-node device IPC via Level Zero zeMem*IpcHandle. Stage into a dedicated
# device_alloc (not the array pool) so the handle stays valid across processes.
# DAGGER_IPC=0 disables the path.
const GPU_IPC = Ref{Union{Nothing,Bool}}(nothing)
function ipc_enabled()
    v = GPU_IPC[]
    v !== nothing && return v
    v = something(tryparse(Bool, get(ENV, "DAGGER_IPC", "true")), true)
    GPU_IPC[] = v
    return v
end
Dagger.ipc_eligible(::IntelVRAMMemorySpace, ::IntelVRAMMemorySpace) = ipc_enabled()

const oneL0 = oneAPI.oneL0

struct ZeIpcInfo{T,N}
    handle::oneL0.ze_ipc_mem_handle_t
    shape::Dims{N}
    bytesize::Int
end
function Dagger.ipc_export(value::oneAPI.oneStridedArray{T,N}) where {T,N}
    # Stage into a fresh device allocation so the IPC handle outlives the source
    with_context!(Dagger.memory_space(value))
    staged = oneArray{T,N}(undef, size(value))
    copyto!(staged, value)
    oneAPI.synchronize()
    handle_ref = Ref{oneL0.ze_ipc_mem_handle_t}()
    oneL0.zeMemGetIpcHandle(oneAPI.context(), pointer(staged), handle_ref)
    return ZeIpcInfo{T,N}(handle_ref[], size(value), sizeof(value)), staged
end
Dagger.ipc_release!(staged::oneArray) = oneAPI.unsafe_free!(staged)
function Dagger.ipc_copyto!(dest::oneAPI.oneStridedArray{T,N}, info::ZeIpcInfo{T,N}) where {T,N}
    @assert size(dest) == info.shape "IPC shape mismatch: $(size(dest)) != $(info.shape)"
    with_context!(Dagger.memory_space(dest))
    ctx = oneAPI.context()
    dev = oneAPI.device()
    ptr_ref = Ref{Ptr{Cvoid}}()
    oneL0.zeMemOpenIpcHandle(ctx, dev, info.handle, UInt32(0), ptr_ref)
    try
        src_ptr = reinterpret(oneL0.ZePtr{T}, ptr_ref[])
        Base.unsafe_copyto!(ctx, dev, pointer(dest), src_ptr, length(dest))
        oneAPI.synchronize()
    finally
        oneL0.zeMemCloseIpcHandle(ctx, ptr_ref[])
    end
    return dest
end
function Dagger.ipc_materialize(info::ZeIpcInfo{T,N}) where {T,N}
    dest = oneArray{T,N}(undef, info.shape)
    return Dagger.ipc_copyto!(dest, info)
end

const DEVICES = Dict{Int, ZeDevice}()
const DRIVERS = Dict{Int, ZeDriver}()
const CONTEXTS = IdDict{ZeDriver, ZeContext}()

function _get_context(driver::ZeDriver)
    return get!(CONTEXTS, driver) do
        ZeContext(driver)
    end
end

function __init__()
    if oneAPI.functional()
        for (device_id, dev) in enumerate(oneAPI.devices())
            @debug "Registering Intel GPU processor with Dagger: $dev"
            Dagger.add_processor_callback!("zearray_device_$(device_id)") do
                proc = oneArrayDeviceProc(myid(), device_id)
                DEVICES[device_id] = dev
                driver!(dev.driver)
                DRIVERS[device_id] = dev.driver
                device!(dev)
                _get_context(dev.driver)
                return proc
            end
        end
    end
end

end # module IntelExt
