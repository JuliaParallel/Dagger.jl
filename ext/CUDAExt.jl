module CUDAExt

export CuArrayDeviceProc

import Dagger, MemPool
import Dagger: CPURAMMemorySpace, Chunk, unwrap
import MemPool: DRef, poolget
import Distributed: myid, remotecall_fetch
import LinearAlgebra
using KernelAbstractions, Adapt

const CPUProc = Union{Dagger.OSProc,Dagger.ThreadProc}

if isdefined(Base, :get_extension)
    import CUDA
else
    import ..CUDA
end
import CUDA: CuDevice, CuContext, CuStream, CuArray, CUDABackend
import CUDA: devices, attribute, context, context!, stream, stream!
import CUDA: CUBLAS, CUSOLVER

using UUIDs

"Represents a single CUDA GPU device."
struct CuArrayDeviceProc <: Dagger.Processor
    owner::Int
    device::Int
    device_uuid::UUID
end
Dagger.get_parent(proc::CuArrayDeviceProc) = Dagger.OSProc(proc.owner)
Dagger.root_worker_id(proc::CuArrayDeviceProc) = proc.owner
Base.show(io::IO, proc::CuArrayDeviceProc) =
    print(io, "CuArrayDeviceProc(worker $(proc.owner), device $(proc.device), uuid $(proc.device_uuid))")
Dagger.short_name(proc::CuArrayDeviceProc) = "W: $(proc.owner), CUDA: $(proc.device)"
Dagger.@gpuproc(CuArrayDeviceProc, CuArray)

"Represents the memory space of a single CUDA GPU's VRAM."
struct CUDAVRAMMemorySpace <: Dagger.MemorySpace
    owner::Int
    device::Int
    device_uuid::UUID
end
Dagger.root_worker_id(space::CUDAVRAMMemorySpace) = space.owner
function Dagger.memory_space(x::CuArray)
    dev = CUDA.device(x)
    device_id = dev.handle
    device_uuid = CUDA.uuid(dev)
    return CUDAVRAMMemorySpace(myid(), device_id, device_uuid)
end
function Dagger.aliasing(x::CuArray{T}) where T
    space = Dagger.memory_space(x)
    S = typeof(space)
    cuptr = pointer(x)
    rptr = Dagger.RemotePtr{Cvoid}(UInt64(cuptr), space)
    return Dagger.ContiguousAliasing(Dagger.MemorySpan{S}(rptr, sizeof(T)*length(x)))
end

function Dagger.unsafe_free!(x::CuArray)
    CUDA.unsafe_free!(x)
    return
end

Dagger.memory_spaces(proc::CuArrayDeviceProc) = Set([CUDAVRAMMemorySpace(proc.owner, proc.device, proc.device_uuid)])
Dagger.processors(space::CUDAVRAMMemorySpace) = Set([CuArrayDeviceProc(space.owner, space.device, space.device_uuid)])

function to_device(proc::CuArrayDeviceProc)
    @assert Dagger.root_worker_id(proc) == myid()
    return DEVICES[proc.device]
end
function to_context(proc::CuArrayDeviceProc)
    @assert Dagger.root_worker_id(proc) == myid()
    return CONTEXTS[proc.device]
end
to_context(handle::Integer) = CONTEXTS[handle]
to_context(dev::CuDevice) = to_context(dev.handle)

function with_context!(handle::Integer)
    context!(CONTEXTS[handle])
    stream!(STREAMS[handle])
end
function with_context!(proc::CuArrayDeviceProc)
    @assert Dagger.root_worker_id(proc) == myid()
    with_context!(proc.device)
end
function with_context!(space::CUDAVRAMMemorySpace)
    @assert Dagger.root_worker_id(space) == myid()
    with_context!(space.device)
end
Dagger.with_context!(proc::CuArrayDeviceProc) = with_context!(proc)
Dagger.with_context!(space::CUDAVRAMMemorySpace) = with_context!(space)
function with_context(f, x)
    old_ctx = context()
    old_stream = stream()

    with_context!(x)
    try
        f()
    finally
        context!(old_ctx)
        stream!(old_stream)
    end
end

function _sync_with_context(x::Union{Dagger.Processor,Dagger.MemorySpace})
    with_context(x) do
        CUDA.synchronize()
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
Dagger.allocate_array_func(::CuArrayDeviceProc, ::typeof(rand)) = CUDA.rand
Dagger.allocate_array_func(::CuArrayDeviceProc, ::typeof(randn)) = CUDA.randn
Dagger.allocate_array_func(::CuArrayDeviceProc, ::typeof(ones)) = CUDA.ones
Dagger.allocate_array_func(::CuArrayDeviceProc, ::typeof(zeros)) = CUDA.zeros
struct AllocateUndef{S} end
(::AllocateUndef{S})(T, dims::Dims{N}) where {S,N} = CuArray{S,N}(undef, dims)
Dagger.allocate_array_func(::CuArrayDeviceProc, ::Dagger.AllocateUndef{S}) where S = AllocateUndef{S}()

# In-place
# N.B. These methods assume that later operations will implicitly or
# explicitly synchronize with their associated stream
function Dagger.move!(to_space::Dagger.CPURAMMemorySpace, from_space::CUDAVRAMMemorySpace, to::AbstractArray{T,N}, from::AbstractArray{T,N}) where {T,N}
    if Dagger.root_worker_id(from_space) == myid()
        sync_with_context(from_space)
        with_context!(from_space)
    end
    copyto!(to, from)
    # N.B. DtoH will synchronize
    return
end
function Dagger.move!(to_space::CUDAVRAMMemorySpace, from_space::Dagger.CPURAMMemorySpace, to::AbstractArray{T,N}, from::AbstractArray{T,N}) where {T,N}
    with_context!(to_space)
    copyto!(to, from)
    return
end
function Dagger.move!(to_space::CUDAVRAMMemorySpace, from_space::CUDAVRAMMemorySpace, to::AbstractArray{T,N}, from::AbstractArray{T,N}) where {T,N}
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
function Dagger.move(from_proc::CPUProc, to_proc::CuArrayDeviceProc, x)
    with_context(to_proc) do
        if x isa DenseArray && isbitstype(eltype(x))
            Dagger.pin_buffer!(:CUDA, x)
        end
        arr = adapt(CuArray, x)
        CUDA.synchronize()
        return arr
    end
end
function Dagger.move(from_proc::CPUProc, to_proc::CuArrayDeviceProc, x::Chunk)
    from_w = Dagger.root_worker_id(from_proc)
    to_w = Dagger.root_worker_id(to_proc)
    @assert myid() == to_w
    cpu_data = remotecall_fetch(unwrap, from_w, x)
    with_context(to_proc) do
        if cpu_data isa DenseArray && isbitstype(eltype(cpu_data))
            Dagger.pin_buffer!(:CUDA, cpu_data)
        end
        arr = adapt(CuArray, cpu_data)
        CUDA.synchronize()
        return arr
    end
end
function Dagger.move(from_proc::CPUProc, to_proc::CuArrayDeviceProc, x::CuArray)
    if CUDA.device(x) == to_device(to_proc)
        return x
    end
    with_context(to_proc) do
        _x = similar(x)
        copyto!(_x, x)
        CUDA.synchronize()
        return _x
    end
end

# Out-of-place DtoH
function Dagger.move(from_proc::CuArrayDeviceProc, to_proc::CPUProc, x)
    with_context(from_proc) do
        CUDA.synchronize()
        _x = x isa DenseArray && isbitstype(eltype(x)) ?
             Dagger.pinned_host_array(x) : adapt(Array, x)
        CUDA.synchronize()
        return _x
    end
end
function Dagger.move(from_proc::CuArrayDeviceProc, to_proc::CPUProc, x::Chunk)
    from_w = Dagger.root_worker_id(from_proc)
    to_w = Dagger.root_worker_id(to_proc)
    @assert myid() == to_w
    remotecall_fetch(from_w, x) do x
        arr = unwrap(x)
        return Dagger.move(from_proc, to_proc, arr)
    end
end
function Dagger.move(from_proc::CuArrayDeviceProc, to_proc::CPUProc, x::CuArray{T,N}) where {T,N}
    with_context(from_proc) do
        CUDA.synchronize()
        _x = Dagger.pinned_host_array(x)
        CUDA.synchronize()
        return _x
    end
end

# Out-of-place DtoD
function Dagger.move(from_proc::CuArrayDeviceProc, to_proc::CuArrayDeviceProc, x::Dagger.Chunk{T}) where T<:CuArray
    if from_proc == to_proc
        # Same process and GPU, no change
        arr = unwrap(x)
        with_context(CUDA.synchronize, from_proc)
        return arr
    elseif Dagger.root_worker_id(from_proc) == Dagger.root_worker_id(to_proc)
        # Same process but different GPUs, use DtoD copy
        from_arr = unwrap(x)
        with_context(CUDA.synchronize, from_proc)
        return with_context(to_proc) do
            to_arr = similar(from_arr)
            copyto!(to_arr, from_arr)
            CUDA.synchronize()
            return to_arr
        end
    elseif Dagger.same_node(Dagger.current_acceleration(), from_proc.owner, to_proc.owner) && from_proc.device_uuid == to_proc.device_uuid
        # Same node, we can use IPC
        ipc_handle, eT, shape = remotecall_fetch(from_proc.owner, x) do x
            arr = unwrap(x)
            ipc_handle_ref = Ref{CUDA.CUipcMemHandle}()
            GC.@preserve arr begin
                CUDA.cuIpcGetMemHandle(ipc_handle_ref, pointer(arr))
            end
            (ipc_handle_ref[], eltype(arr), size(arr))
        end
        r_ptr = Ref{CUDA.CUdeviceptr}()
        CUDA.device!(from_proc.device) do
            CUDA.cuIpcOpenMemHandle(r_ptr, ipc_handle, CUDA.CU_IPC_MEM_LAZY_ENABLE_PEER_ACCESS)
        end
        ptr = Base.unsafe_convert(CUDA.CuPtr{eT}, r_ptr[])
        arr = unsafe_wrap(CuArray, ptr, shape; own=false)
        finalizer(arr) do arr
            CUDA.cuIpcCloseMemHandle(pointer(arr))
        end
        if from_proc.device_uuid != to_proc.device_uuid
            return CUDA.device!(to_proc.device) do
                to_arr = similar(arr)
                copyto!(to_arr, arr)
                to_arr
            end
        else
            return arr
        end
    else
        # Different node, use DtoH, serialization, HtoD (pinned host staging)
        host_copy = remotecall_fetch(from_proc.owner, from_proc, x) do from_proc, x
            return with_context(from_proc) do
                Dagger.pinned_host_array(unwrap(x))
            end
        end
        return with_context(to_proc) do
            Dagger.pin_buffer!(:CUDA, host_copy)
            return CuArray(host_copy)
        end
    end
end

function Dagger.move(from_proc::CuArrayDeviceProc, to_proc::CuArrayDeviceProc, x::CuArray)
    if from_proc == to_proc
        with_context(CUDA.synchronize, from_proc)
        return x
    elseif Dagger.root_worker_id(from_proc) == Dagger.root_worker_id(to_proc)
        with_context(CUDA.synchronize, from_proc)
        return with_context(to_proc) do
            to_arr = similar(x)
            copyto!(to_arr, x)
            CUDA.synchronize()
            return to_arr
        end
    else
        host_copy = with_context(from_proc) do
            return Dagger.pinned_host_array(x)
        end
        return with_context(to_proc) do
            Dagger.pin_buffer!(:CUDA, host_copy)
            return CuArray(host_copy)
        end
    end
end

# Adapt generic functions
Dagger.move(from_proc::CPUProc, to_proc::CuArrayDeviceProc, x::Function) = x
Dagger.move(from_proc::CPUProc, to_proc::CuArrayDeviceProc, x::Chunk{T}) where {T<:Function} =
    Dagger.move(from_proc, to_proc, fetch(x))

# Task execution
function Dagger.execute!(proc::CuArrayDeviceProc, f, args...; kwargs...)
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

# Adapt BLAS/LAPACK functions
import LinearAlgebra: BLAS, LAPACK
_keep_blas_functions = Set(["iamax"])
for lib in [BLAS, LAPACK]
    for name in names(lib; all=true)
        name == nameof(lib) && continue
        startswith(string(name), '#') && continue
        if !endswith(string(name), '!') && !any(endswith(string(name), func) for func in _keep_blas_functions)
            continue
        end

        for culib in [CUBLAS, CUSOLVER]
            if name in names(culib; all=true)
                fn = getproperty(lib, name)
                cufn = getproperty(culib, name)
                @eval Dagger.move(from_proc::CPUProc, to_proc::CuArrayDeviceProc, ::$(typeof(fn))) = $cufn
            end
        end
    end
end

# Adapt RefValue
Dagger.move(from_proc::CPUProc, to_proc::CuArrayDeviceProc, x::Base.RefValue) =
    Dagger.GPURef(Dagger.move(from_proc, to_proc, x[]), only(Dagger.memory_spaces(to_proc)))
Dagger.move(from_proc::CuArrayDeviceProc, to_proc::CPUProc, x::Dagger.GPURef{T,CUDAVRAMMemorySpace} where T) =
    Ref(Dagger.move(from_proc, to_proc, x[]))
function Dagger.move!(dep_mod, to_space::CPURAMMemorySpace, from_space::CUDAVRAMMemorySpace, to::Base.RefValue, from::Dagger.GPURef)
    if Dagger.type_may_alias(typeof(from[]))
        Dagger.move!(dep_mod, to_space, from_space, to[], from[])
    else
        to[] = dep_mod(from[])
    end
    return
end
function Dagger.move!(dep_mod, to_space::CUDAVRAMMemorySpace, from_space::CPURAMMemorySpace, to::Dagger.GPURef, from::Base.RefValue)
    if Dagger.type_may_alias(typeof(from[]))
        Dagger.move!(dep_mod, to_space, from_space, to[], from[])
    else
        to[] = dep_mod(from[])
    end
    return
end
function Dagger.move!(dep_mod, to_space::CUDAVRAMMemorySpace, from_space::CUDAVRAMMemorySpace, to::Dagger.GPURef, from::Dagger.GPURef)
    if Dagger.type_may_alias(typeof(from[]))
        Dagger.move!(dep_mod, to_space, from_space, to[], from[])
    else
        to[] = dep_mod(from[])
    end
    return
end

# Adapt HaloArray
CuArray(H::Dagger.HaloArray) = convert(CuArray, H)
Base.convert(::Type{C}, H::Dagger.HaloArray) where {C<:CuArray} =
    Dagger.HaloArray(C(H.center),
                     C.(H.halos),
                     H.halo_width;
                     own_center=H.own_center)
Adapt.adapt_structure(to::CUDA.KernelAdaptor, H::Dagger.HaloArray) =
    Dagger.HaloArray(adapt(to, H.center),
                     adapt.(Ref(to), H.halos),
                     H.halo_width;
                     own_center=H.own_center)
function Dagger.inner_stencil_proc!(::CuArrayDeviceProc, f, output, read_vars)
    Dagger.Kernel(_inner_stencil!)(f, output, read_vars; ndrange=size(output))
    return
end
@kernel function _inner_stencil!(f, output, read_vars)
    idx = @index(Global, Cartesian)
    f(idx, output, read_vars)
end

Dagger.gpu_processor(::Val{:CUDA}) = CuArrayDeviceProc
Dagger.gpu_can_compute(::Val{:CUDA}) = CUDA.has_cuda()
Dagger.gpu_kernel_backend(::CuArrayDeviceProc) = CUDABackend()
Dagger.gpu_with_device(f, proc::CuArrayDeviceProc) =
    CUDA.device!(f, proc.device)
function Dagger.gpu_synchronize(proc::CuArrayDeviceProc)
    with_context(proc) do
        CUDA.synchronize()
    end
end
function Dagger.gpu_synchronize(::Val{:CUDA})
    for dev in CUDA.devices()
        _sync_with_context(CuArrayDeviceProc(myid(), dev.handle, CUDA.uuid(dev)))
    end
end

Dagger.to_scope(::Val{:cuda_gpu}, sc::NamedTuple) =
    Dagger.to_scope(Val{:cuda_gpus}(), merge(sc, (;cuda_gpus=[sc.cuda_gpu])))
Dagger.scope_key_precedence(::Val{:cuda_gpu}) = 1
function Dagger.to_scope(::Val{:cuda_gpus}, sc::NamedTuple)
    if haskey(sc, :worker)
        workers = Int[sc.worker]
    elseif haskey(sc, :workers) && sc.workers != Colon()
        workers = sc.workers
    else
        workers = map(gproc->gproc.pid, Dagger.procs(Dagger.Sch.eager_context()))
    end
    scopes = Dagger.ExactScope[]
    dev_ids = sc.cuda_gpus
    for worker in workers
        procs = Dagger.get_processors(Dagger.OSProc(worker))
        for proc in procs
            proc isa CuArrayDeviceProc || continue
            if dev_ids == Colon() || proc.device+1 in dev_ids
                scope = Dagger.ExactScope(proc)
                push!(scopes, scope)
            end
        end
    end
    return Dagger.UnionScope(scopes)
end
Dagger.scope_key_precedence(::Val{:cuda_gpus}) = 1

# MPI (SPMD) integration: aliasing spans broadcast from an owner rank must be
# stamped with that rank so same-device addresses on different ranks never
# falsely alias (every rank has myid() == 1 under SPMD)
Dagger.mpi_remap_space(space::CUDAVRAMMemorySpace, owner::Int) =
    CUDAVRAMMemorySpace(owner, space.device, space.device_uuid)
# Acceleration-free memory space of a raw value, for chunk record labeling
Dagger.value_memory_space(x::CuArray) = Dagger.memory_space(x)

# MPI data plane: pass CuArrays to MPI directly when the library is CUDA-aware
# (DAGGER_MPI_GPU_DIRECT=0/1 forces the decision; detection is best-effort)
const MPI_GPU_DIRECT = Ref{Union{Nothing,Bool}}(nothing)
function mpi_gpu_direct_enabled()
    v = MPI_GPU_DIRECT[]
    v !== nothing && return v
    env = get(ENV, "DAGGER_MPI_GPU_DIRECT", "")
    v = if !isempty(env)
        something(tryparse(Bool, env), false)
    else
        Dagger.mpi_library_gpu_aware(:CUDA)
    end
    MPI_GPU_DIRECT[] = v
    return v
end
Dagger.mpi_device_direct(x::CUDA.StridedCuArray) = mpi_gpu_direct_enabled()
# Device-wide sync: producers/consumers run on other tasks' streams, so a
# current-stream synchronize is not enough at the communication boundary
# (device_synchronize yields to the Julia scheduler while waiting)
Dagger.mpi_device_sync(x::CuArray) = CUDA.device_synchronize()
Dagger.mpi_device_sync(::CUDAVRAMMemorySpace) = CUDA.device_synchronize()

# Page-lock host staging buffers (~2x DtoH/HtoD bandwidth); CUDA.pin
# registers a GC finalizer that unregisters the memory
Dagger.gpu_memory_kind(::CuArray) = :CUDA
Dagger.gpu_memory_kind(::CUDAVRAMMemorySpace) = :CUDA
Dagger.pin_buffer!(::Val{:CUDA}, buf::DenseArray) = (CUDA.pin(buf); nothing)

# Same-node device IPC: pool-backed CuArrays are not cuIpc-exportable, so the
# sender stages into a direct (cuMemAlloc) allocation and ships its 64-byte
# handle; the receiver maps it and copies device-to-device.
# DAGGER_IPC=0 disables the path.
const GPU_IPC = Ref{Union{Nothing,Bool}}(nothing)
function ipc_enabled()
    v = GPU_IPC[]
    v !== nothing && return v
    v = something(tryparse(Bool, get(ENV, "DAGGER_IPC", "true")), true)
    GPU_IPC[] = v
    return v
end
Dagger.ipc_eligible(::CUDAVRAMMemorySpace, ::CUDAVRAMMemorySpace) = ipc_enabled()

# The driver API wrappers live in CUDA itself on 5.x and in CUDACore
# (DeviceMemory's home module) on 6.x
const CUDADRV = isdefined(CUDA, :cuIpcGetMemHandle) ? CUDA :
                parentmodule(CUDA.DeviceMemory)

struct CuIpcInfo{T,N}
    handle::CUDADRV.CUipcMemHandle
    shape::Dims{N}
end
function Dagger.ipc_export(value::CUDA.StridedCuArray{T,N}) where {T,N}
    # Activate the source device — MPI execute! may have bound the dest context
    with_context!(Dagger.memory_space(value))
    buf = CUDADRV.alloc(CUDA.DeviceMemory, sizeof(value))
    staged = unsafe_wrap(CuArray, convert(CUDA.CuPtr{T}, buf.ptr), size(value))
    copyto!(staged, value)
    CUDA.device_synchronize()
    handle_ref = Ref{CUDADRV.CUipcMemHandle}()
    CUDADRV.cuIpcGetMemHandle(handle_ref, convert(CUDA.CuPtr{Nothing}, buf.ptr))
    return CuIpcInfo{T,N}(handle_ref[], size(value)), buf
end
Dagger.ipc_release!(buf::CUDA.DeviceMemory) = CUDADRV.free(buf)
function _ipc_open(info::CuIpcInfo{T,N}) where {T,N}
    ptr_ref = Ref{CUDA.CuPtr{Cvoid}}()
    if isdefined(CUDADRV, :cuIpcOpenMemHandle_v2)
        CUDADRV.cuIpcOpenMemHandle_v2(ptr_ref, info.handle, CUDADRV.CU_IPC_MEM_LAZY_ENABLE_PEER_ACCESS)
    else
        CUDADRV.cuIpcOpenMemHandle(ptr_ref, info.handle, CUDADRV.CU_IPC_MEM_LAZY_ENABLE_PEER_ACCESS)
    end
    src = unsafe_wrap(CuArray, convert(CUDA.CuPtr{T}, ptr_ref[]), info.shape)
    return src, ptr_ref[]
end
function Dagger.ipc_copyto!(dest::CUDA.StridedCuArray{T,N}, info::CuIpcInfo{T,N}) where {T,N}
    @assert size(dest) == info.shape "IPC shape mismatch: $(size(dest)) != $(info.shape)"
    with_context!(Dagger.memory_space(dest))
    src, raw = _ipc_open(info)
    try
        copyto!(dest, src)
        CUDA.device_synchronize()
    finally
        CUDA.cuIpcCloseMemHandle(raw)
    end
    return dest
end
function Dagger.ipc_materialize(info::CuIpcInfo{T,N}) where {T,N}
    # Caller must have set the destination device context (ipc_copyto! does)
    dest = CuArray{T,N}(undef, info.shape)
    return Dagger.ipc_copyto!(dest, info)
end

const DEVICES = Dict{Int, CuDevice}()
const CONTEXTS = Dict{Int, CuContext}()
const STREAMS = Dict{Int, CuStream}()

function __init__()
    if CUDA.has_cuda()
        for dev in CUDA.devices()
            @debug "Registering CUDA GPU processor with Dagger: $dev"
            Dagger.add_processor_callback!("cuarray_device_$(dev.handle)") do
                proc = CuArrayDeviceProc(myid(), dev.handle, CUDA.uuid(dev))
                DEVICES[dev.handle] = dev
                ctx = context(dev)
                CONTEXTS[dev.handle] = ctx
                context!(ctx) do
                    STREAMS[dev.handle] = stream()
                end
                return proc
            end
        end
    end
end

end # module CUDAExt
